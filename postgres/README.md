# PostgreSQL Client & Migration Toolkit (`ra-common-mods/postgres`)

A production-grade, highly resilient PostgreSQL module providing connection pool lifecycle management, transactional forward and rollback schema migrations, and a flexible transaction toolkit with PostgreSQL savepoint support.

---

## Features

- **Connection Pool Tuning (`pgxpool`)**: Configurable min/max connections, connection lifetime, idle timeouts, and health-check ping timeouts via environment variables.
- **Prefix-Driven Multi-Database Configuration**: Easily bind multiple databases (e.g., Primary Read/Write, Read Replica, or Analytics) using `envPrefix` without struct duplication.
- **Transactional Schema Migrations**:
  - Enforced pairing between forward (`.up.sql`) and rollback (`.down.sql`) migration files.
  - Session-level advisory locking (`pg_advisory_lock`) to serialize concurrent container migrations.
  - Atomic execution of each migration file within isolated transactions (`pgx.BeginFunc`).
  - Supports standard directory paths (`RunMigrations`) as well as Go 1.16+ embedded filesystems (`RunMigrationsFS` via `//go:embed` or `io/fs.FS`).
- **Comprehensive Transaction Toolkit**:
  - **Unified `DBTX` Interface**: Common interface implemented by `*pgxpool.Pool`, `*pgxpool.Conn`, and `pgx.Tx`, allowing repository methods to run either standalone or inside transactions without duplicate methods.
  - **`WithTx`**: Functional closure guaranteeing automatic commit on success, safe rollback on error, and rollback before re-throwing panics.
  - **`WithinTx`**: Context-propagated transaction chaining with **automatic `SAVEPOINT` support** for nested transactions.
  - **`WithRetryTx`**: Automatic backoff and retry on PostgreSQL transient concurrency conflicts (`40001: serialization_failure` and `40P01: deadlock_detected`).
  - **`NonTx()`**: Dedicated escape hatch to execute un-rollbackable side effects (such as audit logs or security alerts) even from within a failing transaction block.

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
const ServiceMigrationLockKey int64 = 0x4d414e474f535643 // Unique 64-bit ASCII key

if cfg.AutoMigrate {
    if err := db.RunMigrations(ctx, cfg.MigrationsDir, ServiceMigrationLockKey); err != nil {
        return fmt.Errorf("migration failure: %w", err)
    }
}
```

### Running Migrations from Embedded Binary (`//go:embed`)
```go
//go:embed db/migrations/*.sql
var migrationFiles embed.FS

if err := db.RunMigrationsFS(ctx, migrationFiles, "db/migrations", ServiceMigrationLockKey); err != nil {
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

### 1. Explicit Transaction Closure (`WithTx`)
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

### 2. Context-Propagated & Nested Transactions (`WithinTx`)
`WithinTx` propagates the active transaction through `context.Context`. Repositories use `db.DB(ctx)` to seamlessly participate:

```go
func (s *Service) TransferFunds(ctx context.Context, from, to string, amount int) error {
    return s.db.WithinTx(ctx, func(txCtx context.Context) error {
        if err := s.accountRepo.Deduct(txCtx, from, amount); err != nil {
            return err
        }
        if err := s.accountRepo.Credit(txCtx, to, amount); err != nil {
            return err
        }
        return nil
    })
}
```

If another function calls `TransferFunds` inside an existing `WithinTx`, an atomic PostgreSQL `SAVEPOINT` is created automatically, preventing outer transaction abortion if an inner block is handled!

### 3. Writing Side-Effects Outside the Transaction (`NonTx`)
When writing an audit log or failure event that must be preserved even if the business transaction rolls back:

```go
func (s *Service) HandleOperation(ctx context.Context) error {
    return s.db.WithinTx(ctx, func(txCtx context.Context) error {
        if err := s.doWork(txCtx); err != nil {
            // Write to pool directly so audit log is NOT rolled back!
            s.auditRepo.RecordFailure(s.db.NonTx(), "work_failed", err.Error())
            return err // txCtx operations roll back
        }
        return nil
    })
}
```

---

## License

This project is licensed under the Apache 2.0 License.
