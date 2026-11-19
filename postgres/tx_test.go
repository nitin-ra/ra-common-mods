package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/routerarchitects/ra-common-mods/postgres"
)

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

func TestTx_ContextPropagation(t *testing.T) {
	ctx := context.Background()

	// Initial context has no tx
	tx, ok := postgres.TxFromContext(ctx)
	if ok || tx != nil {
		t.Fatal("expected no tx in empty context")
	}

	// TxFromContext on nil context
	tx, ok = postgres.TxFromContext(nil)
	if ok || tx != nil {
		t.Fatal("expected no tx in nil context")
	}
}

func TestTx_DB_FallbackToPool(t *testing.T) {
	var nilDB *postgres.Database
	if nilDB.DB(context.Background()) != nil {
		t.Error("expected nil DBTX from nil Database")
	}
	if nilDB.NonTx() != nil {
		t.Error("expected nil DBTX from nil Database NonTx")
	}

	db := &postgres.Database{Pool: nil}
	// When no transaction is in context, db.DB(ctx) returns db.Pool
	if db.DB(context.Background()) != nil {
		t.Error("expected nil pool since db.Pool is nil")
	}
	if db.NonTx() != nil {
		t.Error("expected nil pool from NonTx")
	}
}
