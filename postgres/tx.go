package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/routerarchitects/ra-common-mods/apperror"
)

type txContextKey struct {
	db *Database
}

type activeTxOwnerKey struct{}

// ContextWithTx injects an active pgx.Tx transaction handle associated with db into the context.
func ContextWithTx(ctx context.Context, db *Database, tx pgx.Tx) context.Context {
	if ctx == nil || db == nil || tx == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, txContextKey{db: db}, tx)
	return context.WithValue(ctx, activeTxOwnerKey{}, db)
}

// TxFromContext retrieves an active pgx.Tx transaction handle from context associated with this database instance.
// Returns false if no transaction is active for this database.
func (db *Database) TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	if ctx == nil || db == nil {
		return nil, false
	}
	tx, ok := ctx.Value(txContextKey{db: db}).(pgx.Tx)
	return tx, ok && tx != nil
}

// DB returns the active pgx.Tx from ctx if one is present for this database instance;
// otherwise it returns db.Pool.
// If the context contains an active transaction belonging to a DIFFERENT database instance,
// it returns an explicit error to prevent accidental cross-database query execution.
func (db *Database) DB(ctx context.Context) (DBTX, error) {
	if db == nil {
		return nil, apperror.New(apperror.CodeInternal, "database instance is nil")
	}

	if tx, ok := db.TxFromContext(ctx); ok {
		return tx, nil
	}

	// Guard against cross-database transaction contamination
	if ctx != nil {
		if activeOwner, ok := ctx.Value(activeTxOwnerKey{}).(*Database); ok && activeOwner != nil && activeOwner != db {
			return nil, apperror.New(apperror.CodeConflict, "context contains a transaction belonging to a different database instance")
		}
	}

	if db.Pool == nil {
		return nil, apperror.New(apperror.CodeInternal, "database pool is not initialized")
	}

	return db.Pool, nil
}

// WithTx executes fn within an atomic PostgreSQL transaction.
// If fn returns nil, the transaction is committed.
// If fn returns an error or panics, the transaction is safely rolled back using a detached context
// to ensure connection cleanup even if ctx has already been cancelled.
func (db *Database) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return db.WithTxOptions(ctx, pgx.TxOptions{}, fn)
}

// WithTxOptions executes fn within a transaction using customized pgx.TxOptions
// (e.g., custom isolation levels or read-only access).
func (db *Database) WithTxOptions(ctx context.Context, opts pgx.TxOptions, fn func(tx pgx.Tx) error) (err error) {
	if db == nil || db.Pool == nil {
		return apperror.New(apperror.CodeInternal, "database pool is not initialized")
	}

	tx, err := db.Pool.BeginTx(ctx, opts)
	if err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to begin database transaction", err)
	}

	defer func() {
		if p := recover(); p != nil {
			// Detached context ensures rollback succeeds even if ctx was cancelled
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = tx.Rollback(rollbackCtx)
			cancel()
			panic(p) // Re-throw panic after rollback to preserve caller diagnostics
		} else if err != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rbErr := tx.Rollback(rollbackCtx)
			cancel()
			if rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				if db.log != nil {
					db.log.ErrorContext(ctx, "failed to rollback transaction", "error", rbErr)
				}
			}
		} else {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				err = apperror.Wrap(apperror.CodeInternal, "failed to commit database transaction", commitErr)
			}
		}
	}()

	err = fn(tx)
	return err
}

// WithinTx wraps fn in a context-propagated transaction.
// If an active transaction already exists for this database in ctx, fn directly participates
// in the existing transaction without creating savepoints or sub-transactions.
// If the context contains a transaction from a different database instance, an explicit conflict error is returned.
func (db *Database) WithinTx(ctx context.Context, fn func(txCtx context.Context) error) error {
	return db.WithinTxOptions(ctx, pgx.TxOptions{}, fn)
}

// WithinTxOptions executes WithinTx with customized pgx.TxOptions.
// If called within an existing transaction, specifying incompatible options returns an error.
func (db *Database) WithinTxOptions(ctx context.Context, opts pgx.TxOptions, fn func(txCtx context.Context) error) error {
	if db == nil {
		return apperror.New(apperror.CodeInternal, "database instance is nil")
	}

	// 1. Guard against cross-database transaction conflict
	if ctx != nil {
		if activeOwner, ok := ctx.Value(activeTxOwnerKey{}).(*Database); ok && activeOwner != nil && activeOwner != db {
			return apperror.New(apperror.CodeConflict, "cannot start transaction: context already has an active transaction for a different database")
		}
	}

	// 2. Check if a transaction for this database already exists in ctx
	if existingTx, ok := db.TxFromContext(ctx); ok && existingTx != nil {
		// Reject incompatible options attempted on an existing transaction
		if opts.IsoLevel != "" || opts.AccessMode != "" {
			return apperror.New(apperror.CodeConflict, "cannot apply new transaction options to an existing transaction")
		}
		// Directly participate in existing atomic transaction
		return fn(ctx)
	}

	// 3. Start top-level transaction and inject into context
	if db.Pool == nil {
		return apperror.New(apperror.CodeInternal, "database pool is not initialized")
	}

	return db.WithTxOptions(ctx, opts, func(tx pgx.Tx) error {
		txCtx := ContextWithTx(ctx, db, tx)
		return fn(txCtx)
	})
}

// WithRetryTx executes fn in a transaction, automatically retrying if a transient
// serialization failure (PostgreSQL SQLSTATE 40001) or deadlock (PostgreSQL SQLSTATE 40P01) occurs.
// Note: fn may be invoked multiple times and must not perform irreversible external side-effects.
func (db *Database) WithRetryTx(ctx context.Context, maxRetries int, fn func(tx pgx.Tx) error) error {
	if maxRetries < 0 {
		maxRetries = 0
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = db.WithTx(ctx, fn)
		if lastErr == nil {
			return nil
		}

		var pgErr *pgconn.PgError
		if errors.As(lastErr, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			// Do not sleep after final attempt
			if attempt == maxRetries {
				return lastErr
			}

			// Transient error; back off and retry if context is still valid
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration((attempt+1)*50) * time.Millisecond):
				continue
			}
		}

		// Non-retryable error
		return lastErr
	}

	return lastErr
}
