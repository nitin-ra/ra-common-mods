# PostgreSQL Client & Migration Toolkit (`ra-common-mods/postgres`)

A production-grade, hardened, lightweight PostgreSQL infrastructure module for Go microservices. It provides robust connection pool lifecycle management, safe transactional forward and rollback schema migrations, multi-database transaction isolation, and context-propagated atomic transaction management without unnecessary frameworks or savepoint complexity.

---

## Features

- **Connection Pool Tuning (`pgxpool`)**: Configurable min/max connections, connection lifetimes, idle timeouts, and health-check ping timeouts via environment variables or typed configuration.
- **Strict Configuration Validation**: Zero unsafe production defaults. Validates host, port (1–65535), username, database, pool bounds (`MinConns <= MaxConns`), and valid SSL modes. Automatic migrations are disabled by default (`AutoMigrate: false`).
- **Safe Credential Handling**: Passwords and sensitive parameters are never printed in structured logs or connection errors. Provides `RedactedDSN(cfg)` for safe diagnostic logs.
- **Transactional Schema Migrations**:
  - Multi-statement SQL script support (parameterless protocol execution).
  - Cryptographic integrity protection: SHA-256 checksum verification detects tampering of already-applied migrations.
  - Strict forward (`.up.sql`) and rollback (`.down.sql`) pairing.
  - Namespace isolation: Configurable migration tracking tables (`MigrationOptions.TableName`) to support multiple services/modules sharing a database.
  - Mandatory session-level advisory locking (`pg_advisory_lock`) with non-zero 64-bit key validation.
  - Supports standard filesystem paths (`RunMigrations`) and embedded filesystems (`RunMigrationsFS` via `//go:embed` or `io/fs.FS`).
- **Hardened Transaction Toolkit**:
  - **Minimal `DBTX` Interface**: Minimal `Exec`, `Query`, `QueryRow` interface for repository implementations, plus optional `ExtendedDBTX` for advanced operations (`CopyFrom`, `SendBatch`).
  - **`WithTx` / `WithTxOptions`**: Atomic transaction block guaranteeing commit on success, detached-context rollback on error or context cancellation, and rollback before re-throwing panics.
  - **`WithinTx` / `WithinTxOptions`**: Context-propagated transactions. When called within an existing transaction for the same database, it participates directly in the single atomic transaction without SAVEPOINT overhead.
  - **Multi-Database Isolation**: Transactions are strictly bound to their owning `*Database` instance. `db.DB(ctx)` returns `(DBTX, error)` and explicitly rejects cross-database transaction contamination.
  - **`WithRetryTx`**: Automatic backoff and retry on PostgreSQL transient concurrency conflicts (`40001: serialization_failure` and `40P01: deadlock_detected`) respecting context cancellation.
  - **Side-Effect Isolation**: Use `db.Pool` directly when writing out-of-band audit events or alerts that should not be rolled back with the main transaction.

---

## Installation

```bash
go get github.com/routerarchitects/ra-common-mods/postgres
```

---

## Configuration

### 1. Standard (Single Database)

```go
package main

import (
	"context"
	"log/slog"

	"github.com/routerarchitects/ra-common-mods/postgres"
)

func main() {
	ctx := context.Background()

	// Loads from STORAGE_TYPE_POSTGRESQL_* environment variables
	cfg, err := postgres.DefaultConfig()
	if err != nil {
		panic(err)
	}

	db, err := postgres.Connect(ctx, cfg, slog.Default())
	if err != nil {
		panic(err)
	}
	defer db.Close()
}
```

### 2. Multi-Database (Primary + Read Replica)

```go
package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/routerarchitects/ra-common-mods/postgres"
)

type Config struct {
	// Primary DB: STORAGE_TYPE_POSTGRESQL_HOST, STORAGE_TYPE_POSTGRESQL_PORT, etc.
	PrimaryDB postgres.Config `envPrefix:"STORAGE_TYPE_POSTGRESQL_"`

	// Read Replica: REPLICA_POSTGRESQL_HOST, REPLICA_POSTGRESQL_PORT, etc.
	ReplicaDB postgres.Config `envPrefix:"REPLICA_POSTGRESQL_"`
}
```

---

## Schema Migrations

### Migration File Convention
Migration files must follow lexicographical numbering with paired `.up.sql` and `.down.sql` scripts:
```text
db/migrations/
├── 000001_initial_schema.up.sql
├── 000001_initial_schema.down.sql
├── 000002_add_indexes.up.sql
└── 000002_add_indexes.down.sql
```

### Running Migrations from Filesystem Path
```go
const ServiceMigrationLockKey int64 = 0x4d414e474f535643 // Unique 64-bit advisory lock key

if cfg.AutoMigrate {
    if err := db.RunMigrations(ctx, cfg.MigrationsDir, ServiceMigrationLockKey); err != nil {
        return fmt.Errorf("migration failure: %w", err)
    }
}
```

### Running Migrations with Custom Table Names & Embedded FS (`//go:embed`)
```go
//go:embed db/migrations/*.sql
var migrationFiles embed.FS

opts := postgres.MigrationOptions{
    TableName:       "service_schema_migrations",
    AdvisoryLockKey: ServiceMigrationLockKey,
}

if err := db.RunMigrationsWithOptions(ctx, migrationFiles, "db/migrations", opts); err != nil {
    return fmt.Errorf("embedded migration failure: %w", err)
}
```

### Reversing Migrations (Rollback)
```go
// Roll back the latest 2 applied migrations
if err := db.Rollback(ctx, cfg.MigrationsDir, 2, ServiceMigrationLockKey); err != nil {
    return fmt.Errorf("rollback failure: %w", err)
}
```

---

## Transaction Toolkit

### 1. Minimal Repository Seam (`DBTX`)

Repositories accept `DBTX` to execute queries transparently against either a connection pool or an active transaction:

```go
type AccountRepository struct{}

func (r *AccountRepository) Deduct(ctx context.Context, db postgres.DBTX, id string, amount int) error {
    _, err := db.Exec(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, id)
    return err
}
```

### 2. Explicit Transaction Closure (`WithTx`)
```go
err := db.WithTx(ctx, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES ($1, $2)", "acc-1", 100); err != nil {
        return err // Automatically rolled back!
    }
    if _, err := tx.Exec(ctx, "INSERT INTO ledgers (account_id, delta) VALUES ($1, $2)", "acc-1", 100); err != nil {
        return err // Automatically rolled back!
    }
    return nil // Automatically committed!
})
```

### 3. Context-Propagated Transactions (`WithinTx`)
`WithinTx` propagates the active transaction through `context.Context`. Repositories retrieve the transaction or connection pool safely using `db.DB(ctx)`:

```go
func (s *Service) TransferFunds(ctx context.Context, from, to string, amount int) error {
    return s.db.WithinTx(ctx, func(txCtx context.Context) error {
        txDB, err := s.db.DB(txCtx)
        if err != nil {
            return err
        }

        if err := s.accountRepo.Deduct(txCtx, txDB, from, amount); err != nil {
            return err
        }
        if err := s.accountRepo.Credit(txCtx, txDB, to, amount); err != nil {
            return err
        }
        return nil
    })
}
```

- **Nested Calls**: If another function invokes `WithinTx` inside an existing transaction on the same database, it participates directly in the outer transaction without savepoints.
- **Multi-Database Safety**: If `dbB.DB(txCtx)` or `dbB.WithinTx(txCtx)` is called with a context containing an active transaction from `dbA`, it immediately returns a conflict error, preventing unintended cross-database transaction contamination.

### 4. Retry on Serialization & Deadlock (`WithRetryTx`)
```go
err := db.WithRetryTx(ctx, 3, func(tx pgx.Tx) error {
    // Operations susceptible to concurrent row lock conflicts (40001 or 40P01)
    return updateInventory(ctx, tx)
})
```

### 5. Writing Side-Effects Outside the Transaction
When recording an audit log or failure notification that must persist even if the business transaction fails, pass `db.Pool` directly:

```go
func (s *Service) HandleOperation(ctx context.Context) error {
    return s.db.WithinTx(ctx, func(txCtx context.Context) error {
        txDB, err := s.db.DB(txCtx)
        if err != nil {
            return err
        }

        if err := s.doWork(txCtx, txDB); err != nil {
            // Write directly to pool so the audit record is preserved upon transaction rollback
            _ = s.auditRepo.RecordFailure(context.Background(), s.db.Pool, "work_failed", err.Error())
            return err // txCtx operations roll back
        }
        return nil
    })
}
```

---

## License

This project is licensed under the Apache 2.0 License.
