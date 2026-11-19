package postgres_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/routerarchitects/ra-common-mods/postgres"
)

func TestConnect_UnsupportedStorageType(t *testing.T) {
	cfg := postgres.Config{
		StorageType: "mysql",
		Host:        "localhost",
		Port:        3306,
	}

	logger := slog.Default()
	database, err := postgres.Connect(context.Background(), cfg, logger)
	if err == nil {
		if database != nil {
			database.Close()
		}
		t.Fatal("expected error for unsupported storage type, got nil")
	}

	if !strings.Contains(err.Error(), "unsupported storage type: mysql") {
		t.Fatalf("expected error message containing 'unsupported storage type: mysql', got: %v", err)
	}
}

func TestDatabase_Ping_NilPool(t *testing.T) {
	database := &postgres.Database{Pool: nil}
	err := database.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error when pinging nil pool, got nil")
	}

	if !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected error containing 'database pool is not initialized', got: %v", err)
	}
}

func TestDatabase_Close_NilPool(t *testing.T) {
	var nilDB *postgres.Database
	nilDB.Close()

	database := &postgres.Database{Pool: nil}
	database.Close()
}

func TestConnect_UnreachableDatabase_Timeout(t *testing.T) {
	cfg := postgres.Config{
		StorageType:        "postgresql",
		Host:               "127.0.0.1",
		Port:               54329,
		Username:           "postgres",
		Password:           "postgres",
		Database:           "unreachable_db",
		SSLMode:            "disable",
		HealthCheckTimeout: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := slog.Default()
	start := time.Now()
	database, err := postgres.Connect(ctx, cfg, logger)
	duration := time.Since(start)

	if err == nil {
		if database != nil {
			database.Close()
		}
		t.Fatal("expected error connecting to unreachable database, got nil")
	}

	t.Logf("Connect failed as expected in %v: %v", duration, err)
}

func TestDatabase_Logger(t *testing.T) {
	var nilDB *postgres.Database
	if nilDB.Logger() == nil {
		t.Error("expected non-nil default logger on nil Database")
	}

	customLogger := slog.New(slog.DiscardHandler)
	db := &postgres.Database{Pool: nil}
	// By default log is nil
	if db.Logger() == nil {
		t.Error("expected non-nil fallback logger when log field is nil")
	}
	_ = customLogger
}
