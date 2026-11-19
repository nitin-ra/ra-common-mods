package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/routerarchitects/ra-common-mods/postgres"
)

func getIntegrationDB(t *testing.T) *postgres.Database {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("skipping PostgreSQL integration test: set TEST_POSTGRES_DSN or DATABASE_URL to enable")
	}

	cfg, err := postgres.DefaultConfig()
	if err != nil {
		t.Fatalf("failed to parse default config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := postgres.Connect(ctx, cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping PostgreSQL integration test (could not connect to %s): %v", cfg.Host, err)
	}

	return db
}

func TestIntegration_MultiStatementMigrationAndRollback(t *testing.T) {
	db := getIntegrationDB(t)
	defer db.Close()

	ctx := context.Background()
	lockKey := int64(0x494e544547303031) // INTEG001
	tableName := "integ_test_schema_migrations"

	// Cleanup any previous run
	_, _ = db.Pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s, integ_orders, integ_users CASCADE", tableName))
	defer func() {
		_, _ = db.Pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s, integ_orders, integ_users CASCADE", tableName))
	}()

	migrations := fstest.MapFS{
		"migrations/000001_initial_schema.up.sql": &fstest.MapFile{
			Data: []byte(`
				-- Multi-statement DDL script
				CREATE TABLE integ_users (
					id VARCHAR(64) PRIMARY KEY,
					name VARCHAR(255) NOT NULL,
					created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				);
				CREATE TABLE integ_orders (
					id VARCHAR(64) PRIMARY KEY,
					user_id VARCHAR(64) REFERENCES integ_users(id),
					amount INT NOT NULL
				);
				CREATE INDEX idx_integ_orders_user ON integ_orders(user_id);
			`),
		},
		"migrations/000001_initial_schema.down.sql": &fstest.MapFile{
			Data: []byte(`
				DROP TABLE IF EXISTS integ_orders CASCADE;
				DROP TABLE IF EXISTS integ_users CASCADE;
			`),
		},
	}

	opts := postgres.MigrationOptions{
		TableName:       tableName,
		AdvisoryLockKey: lockKey,
	}

	// 1. Run migrations with multi-statement SQL and custom table
	if err := db.RunMigrationsWithOptions(ctx, migrations, "migrations", opts); err != nil {
		t.Fatalf("failed to run integration migrations: %v", err)
	}

	// Verify tables were created
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'integ_users'").Scan(&count); err != nil || count == 0 {
		t.Fatalf("expected integ_users table to exist, count=%d, err=%v", count, err)
	}

	// 2. Checksum validation (running again when unchanged is a no-op)
	if err := db.RunMigrationsWithOptions(ctx, migrations, "migrations", opts); err != nil {
		t.Fatalf("subsequent migration run failed: %v", err)
	}

	// 3. Rollback
	if err := db.RollbackWithOptions(ctx, migrations, "migrations", 1, opts); err != nil {
		t.Fatalf("failed to rollback migration: %v", err)
	}

	// Verify tables were dropped
	if err := db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'integ_users'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("expected integ_users table to be dropped, count=%d", count)
	}
}

func TestIntegration_AdvisoryLockConcurrency(t *testing.T) {
	db := getIntegrationDB(t)
	defer db.Close()

	ctx := context.Background()
	lockKey := int64(0x494e544547303032)
	tableName := "integ_concurrent_migrations"

	_, _ = db.Pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s, integ_items CASCADE", tableName))
	defer func() {
		_, _ = db.Pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s, integ_items CASCADE", tableName))
	}()

	migrations := fstest.MapFS{
		"migrations/000001_items.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE IF NOT EXISTS integ_items (id INT);"),
		},
		"migrations/000001_items.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE IF EXISTS integ_items CASCADE;"),
		},
	}

	opts := postgres.MigrationOptions{
		TableName:       tableName,
		AdvisoryLockKey: lockKey,
	}

	// Run concurrent migrations across 5 goroutines
	var wg sync.WaitGroup
	errCh := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.RunMigrationsWithOptions(ctx, migrations, "migrations", opts); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent migration returned unexpected error: %v", err)
	}
}

func TestIntegration_TransactionCommitRollbackAndPanic(t *testing.T) {
	db := getIntegrationDB(t)
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Pool.Exec(ctx, "CREATE TEMP TABLE tx_test_items (name VARCHAR(64))")

	// 1. Commit on success
	err := db.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test_items (name) VALUES ('item1')")
		return err
	})
	if err != nil {
		t.Fatalf("WithTx commit failed: %v", err)
	}

	// 2. Rollback on error
	err = db.WithTx(ctx, func(tx pgx.Tx) error {
		_, _ = tx.Exec(ctx, "INSERT INTO tx_test_items (name) VALUES ('item2')")
		return errors.New("simulated business error")
	})
	if err == nil {
		t.Fatal("expected error from WithTx callback")
	}

	// 3. Rollback on panic
	func() {
		defer func() {
			p := recover()
			if p == nil {
				t.Fatal("expected panic to propagate")
			}
		}()
		_ = db.WithTx(ctx, func(tx pgx.Tx) error {
			_, _ = tx.Exec(ctx, "INSERT INTO tx_test_items (name) VALUES ('item3')")
			panic("simulated panic")
		})
	}()

	// Verify only item1 exists
	rows, err := db.Pool.Query(ctx, "SELECT name FROM tx_test_items")
	if err != nil {
		t.Fatalf("failed to query tx_test_items: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		names = append(names, n)
	}

	if len(names) != 1 || names[0] != "item1" {
		t.Fatalf("expected only ['item1'] committed, got: %v", names)
	}
}

func TestIntegration_WithinTx_SameDBReuse_And_CrossDBRejection(t *testing.T) {
	dbA := getIntegrationDB(t)
	defer dbA.Close()

	dbB := &postgres.Database{Pool: nil} // Second db instance without pool

	ctx := context.Background()
	_, _ = dbA.Pool.Exec(ctx, "CREATE TEMP TABLE nested_test_items (val INT)")

	// Test same-DB nesting and cross-DB rejection
	err := dbA.WithinTx(ctx, func(txCtx context.Context) error {
		dbtx, err := dbA.DB(txCtx)
		if err != nil {
			return err
		}
		_, err = dbtx.Exec(txCtx, "INSERT INTO nested_test_items (val) VALUES (42)")
		if err != nil {
			return err
		}

		// Nested WithinTx on dbA participates in the same atomic transaction
		err = dbA.WithinTx(txCtx, func(nestedCtx context.Context) error {
			nestedDBTX, err := dbA.DB(nestedCtx)
			if err != nil {
				return err
			}
			_, err = nestedDBTX.Exec(nestedCtx, "INSERT INTO nested_test_items (val) VALUES (84)")
			return err
		})
		if err != nil {
			return err
		}

		// dbB must detect cross-database transaction contamination and reject without executing callback
		crossCallbackRan := false
		err = dbB.WithinTx(txCtx, func(bCtx context.Context) error {
			crossCallbackRan = true
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "cross-database transaction detected") {
			t.Fatalf("expected cross-database error on dbB, got: %v", err)
		}
		if crossCallbackRan {
			t.Fatal("expected dbB callback NOT to run")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("expected WithinTx to succeed, got: %v", err)
	}
}
