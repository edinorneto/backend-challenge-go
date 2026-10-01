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

		migrationFile, err := findMigrationFile(version, "up")
		if err != nil {
			return err
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

func (r *Runner) Down(ctx context.Context, steps int) error {
	if steps < 1 {
		return fmt.Errorf("migration down steps must be positive")
	}

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

	rows, err := conn.Query(migrationCtx, `
		SELECT version
		FROM schema_migrations
		ORDER BY version DESC
		LIMIT $1
	`, steps)
	if err != nil {
		return fmt.Errorf("list applied migrations: %w", err)
	}
	versions := make([]int64, 0, steps)
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return fmt.Errorf("scan applied migration: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate applied migrations: %w", err)
	}
	rows.Close()

	for _, version := range versions {
		migrationFile, err := findMigrationFile(version, "down")
		if err != nil {
			return err
		}
		sqlBytes, err := migrationFS.ReadFile("sql/" + migrationFile)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", migrationFile, err)
		}

		tx, err := conn.Begin(migrationCtx)
		if err != nil {
			return fmt.Errorf("begin migration %d revert transaction: %w", version, err)
		}
		if _, err := tx.Exec(migrationCtx, `DELETE FROM schema_migrations WHERE version = $1`, version); err != nil {
			_ = tx.Rollback(migrationCtx)
			return fmt.Errorf("remove migration %d marker: %w", version, err)
		}
		if _, err := tx.Exec(migrationCtx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(migrationCtx)
			return fmt.Errorf("revert migration %d: %w", version, err)
		}
		if err := tx.Commit(migrationCtx); err != nil {
			return fmt.Errorf("commit migration %d revert: %w", version, err)
		}
	}

	return nil
}

func findMigrationFile(version int64, direction string) (string, error) {
	prefix := fmt.Sprintf("%06d_", version)
	suffix := "." + direction + ".sql"
	entries, err := migrationFS.ReadDir("sql")
	if err != nil {
		return "", fmt.Errorf("list migration files: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if len(name) >= len(prefix)+len(suffix) && name[:len(prefix)] == prefix && name[len(name)-len(suffix):] == suffix {
			return name, nil
		}
	}
	return "", fmt.Errorf("migration %s file not found for version %d", direction, version)
}

func Run(ctx context.Context, pool *pgxpool.Pool) error {
	return (&Runner{pool: pool}).Up(ctx)
}

func Revert(ctx context.Context, pool *pgxpool.Pool, steps int) error {
	return (&Runner{pool: pool}).Down(ctx, steps)
}
