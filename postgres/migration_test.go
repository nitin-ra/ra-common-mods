package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/routerarchitects/ra-common-mods/postgres"
)

const testAdvisoryLockKey int64 = 0x4d414e474f474653

func TestMigration_MissingDirectory(t *testing.T) {
	database := &postgres.Database{Pool: nil}
	nonExistentDir := filepath.Join(t.TempDir(), "nonexistent_dir")

	err := database.RunMigrations(context.Background(), nonExistentDir, testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected does not exist error on RunMigrations, got: %v", err)
	}

	err = database.Rollback(context.Background(), nonExistentDir, 1, testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected does not exist error on Rollback, got: %v", err)
	}
}

func TestMigration_EmptyDirectory(t *testing.T) {
	database := &postgres.Database{Pool: nil}
	emptyDir := t.TempDir()

	err := database.RunMigrations(context.Background(), emptyDir, testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "no migration files (*.up.sql) found in directory") {
		t.Fatalf("expected no migration files error, got: %v", err)
	}
}

func TestMigration_MissingDownFile(t *testing.T) {
	database := &postgres.Database{Pool: nil}
	dir := t.TempDir()

	// Create an up migration without a matching down migration
	upPath := filepath.Join(dir, "000001_test.up.sql")
	if err := os.WriteFile(upPath, []byte("CREATE TABLE test_table (id INT);"), 0o600); err != nil {
		t.Fatalf("failed to create dummy up file: %v", err)
	}

	err := database.RunMigrations(context.Background(), dir, testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "missing paired rollback file") {
		t.Fatalf("expected missing paired rollback file error, got: %v", err)
	}
}

func TestMigration_NilPool(t *testing.T) {
	database := &postgres.Database{Pool: nil}
	dir := t.TempDir()

	// Create paired dummy migration files
	upPath := filepath.Join(dir, "000001_test.up.sql")
	downPath := filepath.Join(dir, "000001_test.down.sql")
	if err := os.WriteFile(upPath, []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatalf("failed to create dummy up file: %v", err)
	}
	if err := os.WriteFile(downPath, []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatalf("failed to create dummy down file: %v", err)
	}

	err := database.RunMigrations(context.Background(), dir, testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected nil pool error on RunMigrations, got: %v", err)
	}
}

func TestMigration_FS_Support(t *testing.T) {
	database := &postgres.Database{Pool: nil}

	// 1. Nil FS error check
	err := database.RunMigrationsFS(context.Background(), nil, ".", testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "migrations filesystem must not be nil") {
		t.Fatalf("expected nil filesystem error, got: %v", err)
	}

	err = database.RollbackFS(context.Background(), nil, ".", 1, testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "migrations filesystem must not be nil") {
		t.Fatalf("expected nil filesystem error on rollback, got: %v", err)
	}

	// 2. MapFS with missing down migration
	mapFS := fstest.MapFS{
		"migrations/000001_init.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE items (id INT);"),
		},
	}

	err = database.RunMigrationsFS(context.Background(), mapFS, "migrations", testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "missing paired rollback file") {
		t.Fatalf("expected missing paired rollback file error with MapFS, got: %v", err)
	}

	// 3. MapFS with paired migrations fails on nil pool
	mapFS["migrations/000001_init.down.sql"] = &fstest.MapFile{
		Data: []byte("DROP TABLE items;"),
	}

	err = database.RunMigrationsFS(context.Background(), mapFS, "migrations", testAdvisoryLockKey)
	if err == nil || !strings.Contains(err.Error(), "database pool is not initialized") {
		t.Fatalf("expected nil pool error on RunMigrationsFS, got: %v", err)
	}
}

func TestRollback_InvalidSteps(t *testing.T) {
	database := &postgres.Database{Pool: nil}

	// Steps <= 0 returns nil immediately without attempting DB connection
	if err := database.Rollback(context.Background(), "dummy_dir", 0, testAdvisoryLockKey); err != nil {
		t.Fatalf("expected nil error for steps=0, got: %v", err)
	}
	if err := database.Rollback(context.Background(), "dummy_dir", -1, testAdvisoryLockKey); err != nil {
		t.Fatalf("expected nil error for steps=-1, got: %v", err)
	}
}
