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
	latest, err := latestMigrationVersion()
	if err != nil {
		t.Fatal(err)
	}
	// Migrations newer than 000005 are reverted first, one step at a time; the
	// steps below then check the reverts of 000005 and 000004 as before.
	for version := latest; version > 5; version-- {
		if err := runner.Down(ctx, 1); err != nil {
			t.Fatalf("down migration %d: %v", version, err)
		}
		assertMigrationCount(t, pool, int(version-1))
	}
	assertExists(t, pool, `SELECT to_regclass('uq_wager_processed_reversal_reference') IS NOT NULL`, false, "one reversal per reference index")
	assertExists(t, pool, `SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_prevent_ledger_truncate' AND tgrelid = to_regclass('wallet_ledger_entries'))`, false, "ledger TRUNCATE trigger")
	latest = min(latest, 5)

	if err := runner.Down(ctx, 1); err != nil {
		t.Fatalf("down one: %v", err)
	}
	assertMigrationCount(t, pool, int(latest-1))
	assertExists(t, pool, `SELECT to_regclass('uq_wager_opening_per_wallet') IS NOT NULL`, false, "OPENING uniqueness index")

	if err := runner.Down(ctx, 1); err != nil {
		t.Fatalf("down two: %v", err)
	}
	assertMigrationCount(t, pool, int(latest-2))
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

	if err := runner.Down(ctx, int(latest-2)); err != nil {
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

// Reverting to the initial schema and applying again must keep existing data and
// restore every protection added by the later migrations.
func TestRunnerRoundTripPreservesDataAndRestoresProtections(t *testing.T) {
	pool := isolatedMigrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	runner := &Runner{pool: pool}
	latest, err := latestMigrationVersion()
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}

	walletID := uuid.New()
	transactionID := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_cents, version, created_at, updated_at)
		VALUES ($1, $2, 'BRL', 10000, 1, NOW(), NOW())
	`, walletID, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO wager_transactions (id, source, player_id, wallet_id, kind, status, amount_cents, currency,
			result_balance_cents, result_wallet_version, reference_attempts, created_at, updated_at, processed_at)
		SELECT $1, 'INTERNAL', player_id, id, 'OPENING', 'PROCESSED', 10000, 'BRL', 10000, 1, 0, NOW(), NOW(), NOW()
		FROM wallets WHERE id = $2
	`, transactionID, walletID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_cents, currency,
			balance_before_cents, balance_after_cents, created_at)
		VALUES ($1, $2, $3, 'CREDIT', 10000, 'BRL', 0, 10000, NOW())
	`, uuid.New(), walletID, transactionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, correlation_id,
			occurred_at, version, payload)
		VALUES ($1, 'wallet', $2, 'WalletBalanceChanged', $3, NOW(), 1, '{"walletId":"x"}')
	`, uuid.New(), walletID, transactionID); err != nil {
		t.Fatal(err)
	}

	if err := runner.Down(ctx, int(latest-1)); err != nil {
		t.Fatalf("down to the initial migration: %v", err)
	}
	assertMigrationCount(t, pool, 1)
	assertExists(t, pool, `SELECT to_regclass('uq_wager_processed_reversal_reference_kind') IS NOT NULL`, false, "reversal uniqueness index")
	assertExists(t, pool, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'outbox_events' AND column_name = 'last_error')`, false, "outbox last_error column")
	assertExists(t, pool, `SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_prevent_outbox_snapshot_update' AND tgrelid = to_regclass('outbox_events'))`, false, "outbox snapshot trigger")
	assertDataIntact(t, pool, walletID)

	if err := runner.Up(ctx); err != nil {
		t.Fatalf("re-apply after revert: %v", err)
	}
	assertMigrationCount(t, pool, int(latest))
	assertExists(t, pool, `SELECT to_regclass('uq_wager_processed_reversal_reference_kind') IS NOT NULL`, true, "reversal uniqueness index")
	assertExists(t, pool, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'outbox_events' AND column_name = 'last_error')`, true, "outbox last_error column")
	assertDataIntact(t, pool, walletID)

	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET payload = '{}' WHERE aggregate_id = $1`, walletID); err == nil {
		t.Fatal("re-applied migrations must protect the outbox snapshot again")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID); err == nil {
		t.Fatal("the ledger must stay append-only after the round trip")
	}

	if err := runner.Down(ctx, 100); err != nil {
		t.Fatalf("reverting more steps than applied must revert everything: %v", err)
	}
	assertMigrationCount(t, pool, 0)
}

func isolatedMigrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
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
	t.Cleanup(basePool.Close)
	if err := basePool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	schema := "migration_roundtrip_" + uuid.New().String()[:8]
	if _, err := basePool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = basePool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func assertMigrationCount(t *testing.T, pool *pgxpool.Pool, expected int) {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("expected %d applied migrations, got %d", expected, count)
	}
}

func assertExists(t *testing.T, pool *pgxpool.Pool, query string, expected bool, object string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(), query).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != expected {
		t.Fatalf("expected %s present=%v, got %v", object, expected, exists)
	}
}

func assertDataIntact(t *testing.T, pool *pgxpool.Pool, walletID uuid.UUID) {
	t.Helper()
	var balance, ledger, events int
	if err := pool.QueryRow(context.Background(), `
		SELECT w.balance_cents,
		       (SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = w.id),
		       (SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = w.id)
		FROM wallets w WHERE w.id = $1
	`, walletID).Scan(&balance, &ledger, &events); err != nil {
		t.Fatal(err)
	}
	if balance != 10000 || ledger != 1 || events != 1 {
		t.Fatalf("migration round trip changed data: balance=%d ledger=%d events=%d", balance, ledger, events)
	}
}
