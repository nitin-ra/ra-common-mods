package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/routerarchitects/ra-common-mods/postgres"
)

type mockTx struct {
	pgx.Tx
}

func TestTx_NilPoolErrors(t *testing.T) {
	database := &postgres.Database{Pool: nil}

	err := database.WithTx(context.Background(), func(tx pgx.Tx) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected nil pool error on WithTx, got: %v", err)
	}

	err = database.WithinTx(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected nil pool error on WithinTx, got: %v", err)
	}

	err = database.WithRetryTx(context.Background(), 2, func(tx pgx.Tx) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected nil pool error on WithRetryTx, got: %v", err)
	}
}

func TestTx_ContextPropagation_SingleDB(t *testing.T) {
	db := &postgres.Database{Pool: nil}
	ctx := context.Background()

	// Initial context has no tx
	tx, ok := db.TxFromContext(ctx)
	if ok || tx != nil {
		t.Fatal("expected no tx in empty context")
	}

	// TxFromContext on nil context
	tx, ok = db.TxFromContext(nil)
	if ok || tx != nil {
		t.Fatal("expected no tx in nil context")
	}

	// Inject mock tx into context for this db
	mock := &mockTx{}
	txCtx := postgres.ContextWithTx(ctx, db, mock)

	retrievedTx, ok := db.TxFromContext(txCtx)
	if !ok || retrievedTx != mock {
		t.Fatal("expected retrieved tx to match injected mock tx")
	}

	// db.DB(txCtx) returns the active mock tx
	dbtx, err := db.DB(txCtx)
	if err != nil {
		t.Fatalf("unexpected error from db.DB: %v", err)
	}
	if dbtx != mock {
		t.Fatal("expected db.DB to return active mock tx")
	}
}

func TestTx_MultiDatabase_Isolation(t *testing.T) {
	dbA := &postgres.Database{Pool: nil}
	dbB := &postgres.Database{Pool: nil}

	ctx := context.Background()
	mockA := &mockTx{}

	// Start transaction on dbA and propagate in context
	txCtx := postgres.ContextWithTx(ctx, dbA, mockA)

	// 1. dbA can retrieve its own transaction
	txA, ok := dbA.TxFromContext(txCtx)
	if !ok || txA != mockA {
		t.Fatalf("expected dbA to retrieve its own tx, got ok=%v, tx=%v", ok, txA)
	}
	dbtxA, err := dbA.DB(txCtx)
	if err != nil || dbtxA != mockA {
		t.Fatalf("expected dbA.DB to return mockA, got err=%v, dbtx=%v", err, dbtxA)
	}

	// 2. dbB must NOT retrieve dbA's transaction
	txB, ok := dbB.TxFromContext(txCtx)
	if ok || txB != nil {
		t.Fatalf("expected dbB to NOT retrieve dbA's tx, got ok=%v, tx=%v", ok, txB)
	}

	// 3. dbB.DB(txCtx) must reject cross-database transaction contamination
	_, err = dbB.DB(txCtx)
	if err == nil {
		t.Fatal("expected dbB.DB to reject cross-database tx context, got nil")
	}
	if !strings.Contains(err.Error(), "different database instance") {
		t.Fatalf("expected cross-database error, got: %v", err)
	}

	// 4. dbB.WithinTx(txCtx) must reject cross-database transaction contamination
	err = dbB.WithinTx(txCtx, func(c context.Context) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected dbB.WithinTx to reject cross-database tx context, got nil")
	}
	if !strings.Contains(err.Error(), "different database") {
		t.Fatalf("expected cross-database error on WithinTx, got: %v", err)
	}
}

func TestTx_WithinTx_IncompatibleOptionsRejection(t *testing.T) {
	db := &postgres.Database{Pool: nil}
	mock := &mockTx{}
	txCtx := postgres.ContextWithTx(context.Background(), db, mock)

	// Attempting to apply new isolation level inside existing transaction must fail
	err := db.WithinTxOptions(txCtx, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(c context.Context) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error applying new options to existing tx, got nil")
	}
	if !strings.Contains(err.Error(), "cannot apply new transaction options to an existing transaction") {
		t.Fatalf("expected incompatible options error, got: %v", err)
	}
}

func TestTx_WithinTx_ReusesExistingTransaction(t *testing.T) {
	db := &postgres.Database{Pool: nil}
	mock := &mockTx{}
	txCtx := postgres.ContextWithTx(context.Background(), db, mock)

	// Calling WithinTx when already in tx should directly participate without error
	called := false
	err := db.WithinTx(txCtx, func(nestedCtx context.Context) error {
		called = true
		tx, ok := db.TxFromContext(nestedCtx)
		if !ok || tx != mock {
			t.Fatal("nested call failed to receive existing mock tx")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error from nested WithinTx: %v", err)
	}
	if !called {
		t.Fatal("expected nested WithinTx callback to be invoked")
	}
}

func TestTx_WithRetryTx_NonRetryableError(t *testing.T) {
	db := &postgres.Database{Pool: nil}

	// Nil pool error is non-retryable; should return immediately without retry delay
	start := time.Now()
	err := db.WithRetryTx(context.Background(), 5, func(tx pgx.Tx) error {
		return nil
	})
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected nil pool error, got nil")
	}
	if duration > 100*time.Millisecond {
		t.Errorf("WithRetryTx took %v; expected immediate return on non-retryable error", duration)
	}
}

func TestTx_WithRetryTx_ContextCancelled(t *testing.T) {
	db := &postgres.Database{Pool: nil}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := db.WithRetryTx(ctx, 3, func(tx pgx.Tx) error {
		return &pgconn.PgError{Code: "40001"} // Transient error
	})
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected context.Canceled or pool error, got: %v", err)
	}
}
