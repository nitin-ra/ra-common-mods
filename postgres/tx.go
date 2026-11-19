package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/routerarchitects/ra-common-mods/apperror"
)

type txContextKey struct{}

var savepointCounter uint64

// ContextWithTx injects an active pgx.Tx transaction handle into the context.
func ContextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// TxFromContext retrieves an active pgx.Tx transaction handle from context if present.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(txContextKey{}).(pgx.Tx)
	return tx, ok
}

// DB returns the active pgx.Tx from ctx if one is present (e.g. inside WithinTx);
// otherwise it returns db.Pool. This allows repository methods to automatically
// participate in an ongoing transaction without altering their public signatures.
func (db *Database) DB(ctx context.Context) DBTX {
	if db == nil {
		return nil
	}
	if tx, ok := TxFromContext(ctx); ok && tx != nil {
		return tx
	}
	if db.Pool == nil {
		return nil
	}
	return db.Pool
}

// NonTx ALWAYS returns the underlying db.Pool, bypassing any active transaction in the context.
// Use this as an escape hatch when writing un-rollbackable side-effects (such as audit logs
// or security alerts) that must persist even if the surrounding transaction fails.
func (db *Database) NonTx() DBTX {
	if db == nil || db.Pool == nil {
		return nil
	}
	return db.Pool
}

// WithTx executes fn within an atomic PostgreSQL transaction.
// If fn returns nil, the transaction is committed.
// If fn returns an error or panics, the transaction is safely rolled back.
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
			_ = tx.Rollback(ctx)
			panic(p) // Re-throw panic after rollback to preserve caller diagnostics
		} else if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
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

// WithinTx wraps fn in a transaction and propagates it via the returned context.
// If a transaction is already active in ctx, WithinTx creates an isolated SAVEPOINT.
// If the inner block fails, only the savepoint is rolled back, preventing whole-transaction aborts.
func (db *Database) WithinTx(ctx context.Context, fn func(txCtx context.Context) error) error {
	return db.WithinTxOptions(ctx, pgx.TxOptions{}, fn)
}

// WithinTxOptions executes WithinTx with customized pgx.TxOptions.
func (db *Database) WithinTxOptions(ctx context.Context, opts pgx.TxOptions, fn func(txCtx context.Context) error) error {
	if db == nil || db.Pool == nil {
		return apperror.New(apperror.CodeInternal, "database pool is not initialized")
	}

	parentTx, hasParent := TxFromContext(ctx)
	if !hasParent || parentTx == nil {
		// Top-level transaction: begin real transaction and store in context
		return db.WithTxOptions(ctx, opts, func(tx pgx.Tx) error {
			txCtx := ContextWithTx(ctx, tx)
			return fn(txCtx)
		})
	}

	// Nested transaction: execute inside PostgreSQL SAVEPOINT
	count := atomic.AddUint64(&savepointCounter, 1)
	spName := fmt.Sprintf("sp_%d", count)

	if _, err := parentTx.Exec(ctx, "SAVEPOINT "+spName); err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to create savepoint", err)
	}

	err := fn(ctx)
	if err != nil {
		// Roll back to savepoint on error
		if _, rbErr := parentTx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+spName); rbErr != nil {
			if db.log != nil {
				db.log.ErrorContext(ctx, "failed to rollback to savepoint", "savepoint", spName, "error", rbErr)
			}
		}
		return err
	}

	// Release savepoint on success
	if _, err := parentTx.Exec(ctx, "RELEASE SAVEPOINT "+spName); err != nil {
		return apperror.Wrap(apperror.CodeInternal, "failed to release savepoint", err)
	}

	return nil
}

// WithRetryTx executes fn in a transaction, automatically retrying if a transient
// serialization failure (PostgreSQL SQLSTATE 40001) or deadlock (PostgreSQL SQLSTATE 40P01) occurs.
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
