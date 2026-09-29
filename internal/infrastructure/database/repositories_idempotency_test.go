package database_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

func TestProcessTransactionIdempotencyWithPostgres(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	request := testRequest(playerID, walletID, "transaction-replay", "key-replay", "hash-replay", amount)

	first, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	replay, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	if replay.TransactionID != first.TransactionID {
		t.Fatalf("expected original transaction ID %s, got %s", first.TransactionID, replay.TransactionID)
	}
	if !replay.IdempotentReplay {
		t.Fatal("expected idempotent replay")
	}
	if replay.Balance.String() != "75.00" {
		t.Fatalf("expected original balance 75.00, got %s", replay.Balance.String())
	}
}

func TestProcessTransactionRejectsIdempotencyPayloadConflict(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	first := testRequest(playerID, walletID, "transaction-payload", "key-payload", "hash-one", amount)
	if _, err := repo.ProcessTransaction(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	conflicting := first
	conflicting.PayloadHash = "hash-two"
	if _, err := repo.ProcessTransaction(context.Background(), conflicting); !errors.Is(err, database.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestProcessTransactionRejectsExternalTransactionConflict(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	first := testRequest(playerID, walletID, "transaction-external", "key-one", "hash-one", amount)
	if _, err := repo.ProcessTransaction(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	conflicting := first
	conflicting.IdempotencyKey = "key-two"
	conflicting.PayloadHash = "hash-two"
	if _, err := repo.ProcessTransaction(context.Background(), conflicting); !errors.Is(err, database.ErrExternalTransactionConflict) {
		t.Fatalf("expected external transaction conflict, got %v", err)
	}
}

func TestProcessTransactionConcurrentSameIdempotencyKey(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	request := testRequest(playerID, walletID, "transaction-concurrent-replay", "key-concurrent-replay", "hash", testMoney(t, "25.00"))

	results := make([]ports.ProcessTransactionResult, 2)
	errs := make([]error, 2)
	var group sync.WaitGroup
	group.Add(2)
	for index := range results {
		go func(index int) {
			defer group.Done()
			results[index], errs[index] = repo.ProcessTransaction(context.Background(), request)
		}(index)
	}
	group.Wait()

	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if results[0].TransactionID != results[1].TransactionID {
		t.Fatalf("expected same transaction ID, got %s and %s", results[0].TransactionID, results[1].TransactionID)
	}

	var ledgerCount int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("expected one ledger entry, got %d", ledgerCount)
	}
}

func TestProcessTransactionConcurrentBetsLockOneWallet(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "80.00")

	requests := []ports.ProcessTransactionRequest{
		testRequest(playerID, walletID, "transaction-bet-one", "key-bet-one", "hash-bet-one", amount),
		testRequest(playerID, walletID, "transaction-bet-two", "key-bet-two", "hash-bet-two", amount),
	}
	results := make([]ports.ProcessTransactionResult, 2)
	errs := make([]error, 2)
	var group sync.WaitGroup
	group.Add(2)
	for index := range requests {
		go func(index int) {
			defer group.Done()
			results[index], errs[index] = repo.ProcessTransaction(context.Background(), requests[index])
		}(index)
	}
	group.Wait()

	processed := 0
	rejected := 0
	for index := range results {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		switch results[index].Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			rejected++
			if results[index].FailureCode != "insufficient_funds" {
				t.Fatalf("expected insufficient_funds, got %s", results[index].FailureCode)
			}
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("expected one processed and one rejected result, got %d processed and %d rejected", processed, rejected)
	}

	var balanceCents int64
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 2000 {
		t.Fatalf("expected final balance 20.00, got %d cents", balanceCents)
	}
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgresql://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("PostgreSQL unavailable: %v", err)
	}

	ensureSchema(t, pool)
	t.Cleanup(pool.Close)
	return pool
}

func ensureSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	var exists bool
	if err := pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'wallets')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		return
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test file")
	}
	migrationPath := filepath.Join(filepath.Dir(currentFile), "migrations", "sql", "000001_init.up.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), string(migration)); err != nil {
		t.Fatal(err)
	}
}

func createTestWallet(t *testing.T, repo *database.WalletRepo, pool *pgxpool.Pool, amount string) (uuid.UUID, uuid.UUID) {
	t.Helper()

	playerID := uuid.New()
	walletID := uuid.New()
	balance := testMoney(t, amount)
	w, err := wallet.New(walletID, playerID, balance, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(context.Background(), w); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id = $1 OR correlation_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
	})
	return playerID, walletID
}

func testMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	value, err := money.ParseExternal(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func testRequest(
	playerID uuid.UUID,
	walletID uuid.UUID,
	externalTransactionID string,
	idempotencyKey string,
	payloadHash string,
	amount money.Money,
) ports.ProcessTransactionRequest {
	return ports.ProcessTransactionRequest{
		ProviderID:            "provider-test-" + walletID.String(),
		ExternalTransactionID: externalTransactionID,
		IdempotencyKey:        idempotencyKey,
		PayloadHash:           payloadHash,
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               "round-test",
		GameID:                "game-test",
		Kind:                  "BET",
		Amount:                amount,
	}
}
