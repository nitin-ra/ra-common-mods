package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/routerarchitects/ra-common-mods/apperror"
)

// RunMigrations automatically scans and applies forward SQL migrations (*.up.sql) from migrationsDir.
// It acquires a PostgreSQL session-level advisory lock using advisoryLockKey to serialize migrations across concurrent instances.
func (db *Database) RunMigrations(ctx context.Context, migrationsDir string, advisoryLockKey int64) error {
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		return apperror.New(apperror.CodeInternal, fmt.Sprintf("database migrations directory '%s' does not exist", migrationsDir))
	}
	return db.runMigrationsInternal(ctx, os.DirFS(migrationsDir), ".", advisoryLockKey)
}

// RunMigrationsFS executes forward migrations from an arbitrary fs.FS filesystem (e.g. embed.FS or os.DirFS).
func (db *Database) RunMigrationsFS(ctx context.Context, fsys fs.FS, dir string, advisoryLockKey int64) error {
	if dir == "" {
		dir = "."
	}
	return db.runMigrationsInternal(ctx, fsys, dir, advisoryLockKey)
}

func (db *Database) runMigrationsInternal(ctx context.Context, fsys fs.FS, dir string, advisoryLockKey int64) error {
	if fsys == nil {
		return apperror.New(apperror.CodeInternal, "migrations filesystem must not be nil")
	}

	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("failed to read migrations directory %q", dir), err)
	}

	var upFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".up.sql") {
			upFiles = append(upFiles, entry.Name())
		}
	}

	if len(upFiles) == 0 {
		return apperror.New(apperror.CodeInternal, fmt.Sprintf("no migration files (*.up.sql) found in directory '%s'", dir))
	}

	// Verify that each .up.sql file has a matching .down.sql rollback script
	for _, upFile := range upFiles {
		version := strings.TrimSuffix(upFile, ".up.sql")
		downFile := version + ".down.sql"
		downPath := downFile
		if dir != "." {
			downPath = path.Join(dir, downFile)
		}
		if _, err := fs.Stat(fsys, downPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err) {
				return apperror.New(apperror.CodeInternal, fmt.Sprintf("migration '%s' is missing paired rollback file '%s'", upFile, downFile))
			}
			return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("failed to inspect rollback file '%s'", downFile), err)
		}
	}

	if db == nil || db.Pool == nil {
		return apperror.New(apperror.CodeInternal, "database pool is not initialized")
	}

	if db.log != nil {
		db.log.InfoContext(ctx, "checking database migrations", "directory", dir)
	}

	// Sort migrations lexicographically (e.g. 000001_core_schema.up.sql)
	sort.Strings(upFiles)

	// Acquire a dedicated connection from the pool for advisory locking
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to acquire connection for migrations", err)
	}
	defer conn.Release()

	// Acquire advisory lock using unique 64-bit key if provided
	if advisoryLockKey != 0 {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
			return apperror.Wrap(apperror.CodeInternal, "failed to acquire migration advisory lock", err)
		}
		defer func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var unlocked bool
			if err := conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil || !unlocked {
				if db.log != nil {
					db.log.ErrorContext(ctx, "failed to release migration advisory lock; terminating connection", "error", err, "unlocked", unlocked)
				}
				_ = conn.Conn().Close(context.Background())
			}
		}()
	}

	// Initialize tracking table
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := conn.Exec(ctx, createTableSQL); err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to create schema_migrations table", err)
	}

	for _, file := range upFiles {
		version := strings.TrimSuffix(file, ".up.sql")
		filePath := file
		if dir != "." {
			filePath = path.Join(dir, file)
		}

		// Check if this migration was already applied
		var exists bool
		checkSQL := `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`
		err := conn.QueryRow(ctx, checkSQL, version).Scan(&exists)
		if err != nil {
			return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("failed to check migration state for %s", version), err)
		}

		if exists {
			continue
		}

		if db.log != nil {
			db.log.InfoContext(ctx, "applying database migration", "version", version, "file", file)
		}

		content, err := fs.ReadFile(fsys, filePath)
		if err != nil {
			return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("failed to read migration file %s", file), err)
		}

		// Run the migration in an isolated transaction
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(content)); err != nil {
				return err
			}

			insertSQL := `INSERT INTO schema_migrations (version) VALUES ($1)`
			if _, err := tx.Exec(ctx, insertSQL, version); err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("migration %s failed to execute, transaction rolled back", version), err)
		}

		if db.log != nil {
			db.log.InfoContext(ctx, "successfully applied migration", "version", version)
		}
	}

	if db.log != nil {
		db.log.InfoContext(ctx, "all database migrations are up to date")
	}
	return nil
}

// Rollback reverses the latest applied migrations up to the specified number of steps.
// It acquires the same session-level advisory lock using advisoryLockKey to ensure concurrency safety.
func (db *Database) Rollback(ctx context.Context, migrationsDir string, steps int, advisoryLockKey int64) error {
	if steps <= 0 {
		return nil
	}
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		return apperror.New(apperror.CodeInternal, fmt.Sprintf("database migrations directory '%s' does not exist", migrationsDir))
	}
	return db.rollbackInternal(ctx, os.DirFS(migrationsDir), ".", steps, advisoryLockKey)
}

// RollbackFS reverses the latest applied migrations up to the specified number of steps from an fs.FS filesystem.
func (db *Database) RollbackFS(ctx context.Context, fsys fs.FS, dir string, steps int, advisoryLockKey int64) error {
	if steps <= 0 {
		return nil
	}
	if dir == "" {
		dir = "."
	}
	return db.rollbackInternal(ctx, fsys, dir, steps, advisoryLockKey)
}

func (db *Database) rollbackInternal(ctx context.Context, fsys fs.FS, dir string, steps int, advisoryLockKey int64) error {
	if steps <= 0 {
		return nil
	}

	if fsys == nil {
		return apperror.New(apperror.CodeInternal, "migrations filesystem must not be nil")
	}

	if db == nil || db.Pool == nil {
		return apperror.New(apperror.CodeInternal, "database pool is not initialized")
	}

	// Acquire a dedicated connection from the pool for advisory locking
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to acquire connection for rollback", err)
	}
	defer conn.Release()

	// Acquire advisory lock using unique 64-bit key if provided
	if advisoryLockKey != 0 {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
			return apperror.Wrap(apperror.CodeInternal, "failed to acquire migration advisory lock", err)
		}
		defer func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var unlocked bool
			if err := conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil || !unlocked {
				if db.log != nil {
					db.log.ErrorContext(ctx, "failed to release migration advisory lock; terminating connection", "error", err, "unlocked", unlocked)
				}
				_ = conn.Conn().Close(context.Background())
			}
		}()
	}

	// Query applied migrations in reverse chronological order
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY applied_at DESC, version DESC LIMIT $1`, steps)
	if err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to query applied schema migrations", err)
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return apperror.Wrap(apperror.CodeInternal, "failed to scan migration version", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to read applied migration versions", err)
	}

	for _, version := range versions {
		downFile := version + ".down.sql"
		downPath := downFile
		if dir != "." {
			downPath = path.Join(dir, downFile)
		}

		content, err := fs.ReadFile(fsys, downPath)
		if err != nil {
			return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("failed to read rollback file %s", downFile), err)
		}

		if db.log != nil {
			db.log.InfoContext(ctx, "rolling back database migration", "version", version, "file", downFile)
		}

		// Run rollback script in an isolated transaction
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(content)); err != nil {
				return err
			}

			deleteSQL := `DELETE FROM schema_migrations WHERE version = $1`
			if _, err := tx.Exec(ctx, deleteSQL, version); err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return apperror.Wrap(apperror.CodeInternal, fmt.Sprintf("failed to roll back migration %s, transaction aborted", version), err)
		}

		if db.log != nil {
			db.log.InfoContext(ctx, "successfully rolled back migration", "version", version)
		}
	}

	return nil
}
