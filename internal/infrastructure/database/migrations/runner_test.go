package migrations

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunnerConcurrentUpIsSerialized(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	basePool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	defer basePool.Close()
	if err := basePool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}

	schema := "migration_test_" + uuid.New().String()[:8]
	if _, err := basePool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = basePool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
	}()

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	runners := []*Runner{{pool: pool}, {pool: pool}}
	start := make(chan struct{})
	errs := make(chan error, len(runners))
	var wg sync.WaitGroup
	for _, runner := range runners {
		wg.Add(1)
		go func(runner *Runner) {
			defer wg.Done()
			<-start
			errs <- runner.Up(context.Background())
		}(runner)
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migration runner failed: %v", err)
		}
	}

	var migrationCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version IN (1, 2, 3, 4)`).Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if migrationCount != 4 {
		t.Fatalf("expected four applied migrations, got %d", migrationCount)
	}
}

func TestRunnerDownRevertsLatestMigrations(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	basePool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	defer basePool.Close()
	if err := basePool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}

	schema := "migration_down_test_" + uuid.New().String()[:8]
	if _, err := basePool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = basePool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) }()

	pCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	pCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, pCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	runner := &Runner{pool: pool}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
	if err := runner.Down(ctx, 1); err != nil {
		t.Fatalf("down one: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("expected three migrations after one revert, got %d", count)
	}
	var outboxTriggers int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pg_trigger
		WHERE tgname = 'trg_prevent_outbox_snapshot_update' AND tgrelid = to_regclass('outbox_events')
	`).Scan(&outboxTriggers); err != nil {
		t.Fatal(err)
	}
	if outboxTriggers != 0 {
		t.Fatal("expected outbox snapshot trigger to be dropped by its revert")
	}

	if err := runner.Down(ctx, 3); err != nil {
		t.Fatalf("down remaining: %v", err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("expected no applied migrations after full revert, got %d", remaining)
	}
	var walletsTable *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('wallets')`).Scan(&walletsTable); err != nil {
		t.Fatal(err)
	}
	if walletsTable != nil {
		t.Fatal("expected initial migration revert to drop application tables")
	}
}

func TestEmbeddedMigrationsAreContiguousAndReversible(t *testing.T) {
	latest, err := latestMigrationVersion()
	if err != nil {
		t.Fatal(err)
	}
	if latest < 4 {
		t.Fatalf("expected at least four embedded migrations, got %d", latest)
	}
	for version := int64(1); version <= latest; version++ {
		for _, direction := range []string{"up", "down"} {
			if _, err := findMigrationFile(version, direction); err != nil {
				t.Fatalf("migration %d is missing its %s file: %v", version, direction, err)
			}
		}
	}
}
