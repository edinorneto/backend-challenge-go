package migrations

import (
	"context"
	"embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

//go:embed sql/*.sql
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

	conn, err := r.pool.Acquire(migrationCtx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}

	defer conn.Release()

	const lockKey = "backend-challenge-go:migrations"
	if _, err := conn.Exec(migrationCtx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey)
	}()

	_, err = conn.Exec(migrationCtx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	for version := int64(1); version <= 3; version++ {
		var applied bool
		err = conn.QueryRow(
			migrationCtx,
			`SELECT EXISTS (
				SELECT 1
				FROM schema_migrations
				WHERE version = $1
			)`,
			version,
		).Scan(&applied)
		if err != nil {
			return fmt.Errorf("check migration version %d: %w", version, err)
		}
		if applied {
			continue
		}

		filename := fmt.Sprintf("%06d_", version)
		entries, err := migrationFS.ReadDir("sql")
		if err != nil {
			return fmt.Errorf("list migration files: %w", err)
		}
		var migrationFile string
		for _, entry := range entries {
			if len(entry.Name()) >= len(filename) && entry.Name()[:len(filename)] == filename && len(entry.Name()) > len(".up.sql") && entry.Name()[len(entry.Name())-len(".up.sql"):] == ".up.sql" {
				migrationFile = entry.Name()
				break
			}
		}
		if migrationFile == "" {
			return fmt.Errorf("migration file not found for version %d", version)
		}

		sqlBytes, err := migrationFS.ReadFile("sql/" + migrationFile)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", migrationFile, err)
		}

		tx, err := conn.Begin(migrationCtx)
		if err != nil {
			return fmt.Errorf("begin migration %d transaction: %w", version, err)
		}
		if _, err := tx.Exec(migrationCtx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(migrationCtx)
			return fmt.Errorf("execute migration %d: %w", version, err)
		}
		if _, err := tx.Exec(
			migrationCtx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`,
			version,
		); err != nil {
			_ = tx.Rollback(migrationCtx)
			return fmt.Errorf("register migration %d: %w", version, err)
		}
		if err := tx.Commit(migrationCtx); err != nil {
			return fmt.Errorf("commit migration %d: %w", version, err)
		}
	}

	return nil
}

func Run(ctx context.Context, pool *pgxpool.Pool) error {
	return (&Runner{pool: pool}).Up(ctx)
}
