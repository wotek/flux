package mysql

import (
	"context"
	"database/sql"
)

type txContextKey struct{}

// ContextWithTx returns a new context with the provided [*sql.Tx] injected.
func ContextWithTx(parent context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(parent, txContextKey{}, tx)
}

// TxFromContext extracts an active [*sql.Tx] from the context if present.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*sql.Tx)
	return tx, ok
}
