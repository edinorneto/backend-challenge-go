package database

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type transactionKey struct{}

type TransactionManager struct {
	DB *pgxpool.Pool
}

func NewTransactionManager(db *pgxpool.Pool) *TransactionManager {
	return &TransactionManager{DB: db}
}

func (m *TransactionManager) Begin(ctx context.Context) (context.Context, ports.Transaction, error) {
	tx, err := m.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, transactionKey{}, tx), txAdapter{tx: tx}, nil
}

type txAdapter struct {
	tx pgx.Tx
}

func (t txAdapter) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t txAdapter) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func txFromContext(ctx context.Context) pgx.Tx {
	tx, _ := ctx.Value(transactionKey{}).(pgx.Tx)
	return tx
}
