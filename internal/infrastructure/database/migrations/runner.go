package migrations

import (
	"context"
	"embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

//go:embed sql/000001_init.up.sql
var migrationFS embed.FS

type Runner struct {
	pool *pgxpool.Pool
}

func NewRunner(
	lc fx.Lifecycle,
	pool *pgxpool.Pool,
) *Runner {
	runner := &Runner{
		pool: pool,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return runner.Up(ctx)
		},
	})

	return runner
}

func (r *Runner) Up(ctx context.Context) error {
	migrationCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := r.pool.Exec(migrationCtx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	const version int64 = 1

	var applied bool

	err = r.pool.QueryRow(
		migrationCtx,
		`SELECT EXISTS (
			SELECT 1
			FROM schema_migrations
			WHERE version = $1
		)`,
		version,
	).Scan(&applied)
	if err != nil {
		return fmt.Errorf("check migration version: %w", err)
	}

	if applied {
		return nil
	}

	sqlBytes, err := migrationFS.ReadFile("sql/000001_init.up.sql")
	if err != nil {
		return fmt.Errorf("read migration file: %w", err)
	}

	tx, err := r.pool.Begin(migrationCtx)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}

	defer tx.Rollback(migrationCtx)

	if _, err := tx.Exec(migrationCtx, string(sqlBytes)); err != nil {
		return fmt.Errorf("execute migration: %w", err)
	}

	if _, err := tx.Exec(
		migrationCtx,
		`INSERT INTO schema_migrations (version)
		 VALUES ($1)`,
		version,
	); err != nil {
		return fmt.Errorf("register migration: %w", err)
	}

	if err := tx.Commit(migrationCtx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}

	return nil
}
