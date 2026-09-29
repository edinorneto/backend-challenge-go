package database_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestProcessTransactionRefundsProcessedBet(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	bet := testRequest(playerID, walletID, "refund-bet", "refund-bet-key", "refund-bet-hash", amount)
	_, err := repo.ProcessTransaction(context.Background(), bet)
	if err != nil {
		t.Fatal(err)
	}
	refund := testRequest(playerID, walletID, "refund-operation", "refund-operation-key", "refund-operation-hash", amount)
	refund.Kind = "REFUND"
	refund.ReferenceExternalTransactionID = bet.ExternalTransactionID
	result, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PROCESSED" || result.Balance.String() != "100.00" {
		t.Fatalf("expected processed refund with 100.00, got %s and %s", result.Status, result.Balance.String())
	}

	var ledgerCount int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE l.wallet_id = $1 AND t.kind = 'REFUND'`, walletID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("expected one refund ledger entry, got %d", ledgerCount)
	}
}

func TestProcessTransactionRollsBackWin(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	win := testRequest(playerID, walletID, "rollback-win", "rollback-win-key", "rollback-win-hash", amount)
	win.Kind = "WIN"
	if result, err := repo.ProcessTransaction(context.Background(), win); err != nil {
		t.Fatal(err)
	} else if result.Status != "PROCESSED" || result.Balance.String() != "125.00" {
		t.Fatalf("expected processed WIN with 125.00, got %s and %s", result.Status, result.Balance.String())
	}

	rollback := testRequest(playerID, walletID, "rollback-operation", "rollback-operation-key", "rollback-operation-hash", amount)
	rollback.Kind = "ROLLBACK"
	rollback.ReferenceExternalTransactionID = win.ExternalTransactionID
	result, err := repo.ProcessTransaction(context.Background(), rollback)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PROCESSED" || result.Balance.String() != "100.00" {
		t.Fatalf("expected processed rollback with 100.00, got %s and %s", result.Status, result.Balance.String())
	}
}

func TestProcessTransactionPendingReferencePersistsRetryState(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	request := testRequest(playerID, walletID, "pending-refund", "pending-refund-key", "pending-refund-hash", amount)
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "missing-bet"
	result, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected PENDING_REFERENCE, got %s", result.Status)
	}

	var attempts int
	var nextAttempt time.Time
	var eventType string
	if err := pool.QueryRow(context.Background(), `SELECT reference_attempts, reference_next_attempt_at FROM wager_transactions WHERE id = $1`, result.TransactionID).Scan(&attempts, &nextAttempt); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || nextAttempt.IsZero() {
		t.Fatalf("expected one reference attempt and next retry, got %d and %v", attempts, nextAttempt)
	}
	if err := pool.QueryRow(context.Background(), `SELECT event_type FROM outbox_events WHERE correlation_id = $1`, result.TransactionID).Scan(&eventType); err != nil {
		t.Fatal(err)
	}
	if eventType != "WagerTransactionPendingReference" {
		t.Fatalf("expected pending reference event, got %s", eventType)
	}
}

func TestRetryPendingReferenceUsesExponentialBackoffAndRejectsAfterLimit(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	request := testRequest(playerID, walletID, "retry-refund", "retry-refund-key", "retry-refund-hash", testMoney(t, "25.00"))
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "retry-missing-reference"

	result, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending reference, got %s", result.Status)
	}

	expectedBackoff := []time.Duration{2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute}
	for index, expected := range expectedBackoff {
		before := time.Now().UTC()
		result, err = repo.RetryPendingReference(context.Background(), result.TransactionID)
		if err != nil {
			t.Fatal(err)
		}
		var attempts int
		var nextAttempt time.Time
		if err := pool.QueryRow(context.Background(), `SELECT reference_attempts, reference_next_attempt_at FROM wager_transactions WHERE id = $1`, result.TransactionID).Scan(&attempts, &nextAttempt); err != nil {
			t.Fatal(err)
		}
		if attempts != index+2 {
			t.Fatalf("expected attempt %d, got %d", index+2, attempts)
		}
		if nextAttempt.Sub(before) < expected-time.Second || nextAttempt.Sub(before) > expected+time.Second {
			t.Fatalf("expected retry delay near %s, got %s", expected, nextAttempt.Sub(before))
		}
	}

	result, err = repo.RetryPendingReference(context.Background(), result.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "REJECTED" || result.FailureCode != "reference_not_found" {
		t.Fatalf("expected final reference rejection, got %s/%s", result.Status, result.FailureCode)
	}

	var balanceCents, ledgerCount, attempts int
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, result.TransactionID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT reference_attempts FROM wager_transactions WHERE id = $1`, result.TransactionID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 10000 || ledgerCount != 0 || attempts != 6 {
		t.Fatalf("pending retry changed financial state: balance=%d ledger=%d attempts=%d", balanceCents, ledgerCount, attempts)
	}
	var eventType string
	if err := pool.QueryRow(context.Background(), `SELECT event_type FROM outbox_events WHERE correlation_id = $1 ORDER BY occurred_at DESC LIMIT 1`, result.TransactionID).Scan(&eventType); err != nil {
		t.Fatal(err)
	}
	if eventType != "WagerTransactionRejected" {
		t.Fatalf("expected rejection event, got %s", eventType)
	}
}

func TestProcessTransactionConcurrentReversalsProcessOnlyOne(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")
	bet := testRequest(playerID, walletID, "concurrent-reversal-bet", "concurrent-reversal-bet-key", "concurrent-reversal-bet-hash", amount)
	if result, err := repo.ProcessTransaction(context.Background(), bet); err != nil {
		t.Fatal(err)
	} else if result.Status != "PROCESSED" {
		t.Fatalf("expected processed bet, got %s", result.Status)
	}

	requests := []ports.ProcessTransactionRequest{
		testRequest(playerID, walletID, "concurrent-refund-one", "concurrent-refund-key-one", "concurrent-refund-hash-one", amount),
		testRequest(playerID, walletID, "concurrent-refund-two", "concurrent-refund-key-two", "concurrent-refund-hash-two", amount),
	}
	for index := range requests {
		requests[index].Kind = "REFUND"
		requests[index].ReferenceExternalTransactionID = bet.ExternalTransactionID
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

	processed, rejected := 0, 0
	for index := range results {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		switch results[index].Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			rejected++
			if results[index].FailureCode != "reversal_already_processed" {
				t.Fatalf("unexpected concurrent rejection: %s", results[index].FailureCode)
			}
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("expected one processed and one rejected reversal, got %d/%d", processed, rejected)
	}
	var balanceCents, ledgerCount int
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE l.wallet_id = $1 AND t.kind = 'REFUND'`, walletID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 10000 || ledgerCount != 1 {
		t.Fatalf("unexpected concurrent reversal state: balance=%d ledger=%d", balanceCents, ledgerCount)
	}
}

func TestRollbackWinOutboxDirectionMatchesLedger(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")
	win := testRequest(playerID, walletID, "direction-win", "direction-win-key", "direction-win-hash", amount)
	win.Kind = "WIN"
	if _, err := repo.ProcessTransaction(context.Background(), win); err != nil {
		t.Fatal(err)
	}
	rollback := testRequest(playerID, walletID, "direction-rollback", "direction-rollback-key", "direction-rollback-hash", amount)
	rollback.Kind = "ROLLBACK"
	rollback.ReferenceExternalTransactionID = win.ExternalTransactionID
	result, err := repo.ProcessTransaction(context.Background(), rollback)
	if err != nil {
		t.Fatal(err)
	}
	var direction string
	var payload []byte
	if err := pool.QueryRow(context.Background(), `SELECT l.direction, o.payload FROM wallet_ledger_entries l JOIN outbox_events o ON o.correlation_id = l.transaction_id WHERE l.transaction_id = $1 AND o.event_type = 'WalletBalanceChanged'`, result.TransactionID).Scan(&direction, &payload); err != nil {
		t.Fatal(err)
	}
	if direction != "DEBIT" || !strings.Contains(string(payload), `"direction": "DEBIT"`) {
		t.Fatalf("expected debit direction in ledger and outbox, got %s/%s", direction, payload)
	}
}
func TestProcessTransactionRejectsIncompatibleAndDuplicateReversals(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	win := testRequest(playerID, walletID, "incompatible-win", "incompatible-win-key", "incompatible-win-hash", amount)
	win.Kind = "WIN"
	if _, err := repo.ProcessTransaction(context.Background(), win); err != nil {
		t.Fatal(err)
	}
	refund := testRequest(playerID, walletID, "invalid-refund", "invalid-refund-key", "invalid-refund-hash", amount)
	refund.Kind = "REFUND"
	refund.ReferenceExternalTransactionID = win.ExternalTransactionID
	result, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "REJECTED" || result.FailureCode != "reference_incompatible" {
		t.Fatalf("expected incompatible rejection, got %s/%s", result.Status, result.FailureCode)
	}
	replay, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.FailureCode != "reference_incompatible" {
		t.Fatalf("expected rejection failure code on replay, got %+v", replay)
	}

	bet := testRequest(playerID, walletID, "duplicate-bet", "duplicate-bet-key", "duplicate-bet-hash", amount)
	if _, err := repo.ProcessTransaction(context.Background(), bet); err != nil {
		t.Fatal(err)
	}
	firstRefund := testRequest(playerID, walletID, "duplicate-refund-one", "duplicate-refund-key-one", "duplicate-refund-hash-one", amount)
	firstRefund.Kind = "REFUND"
	firstRefund.ReferenceExternalTransactionID = bet.ExternalTransactionID
	if result, err := repo.ProcessTransaction(context.Background(), firstRefund); err != nil {
		t.Fatal(err)
	} else if result.Status != "PROCESSED" {
		t.Fatalf("expected first refund to process, got %s", result.Status)
	}
	secondRefund := firstRefund
	secondRefund.ExternalTransactionID = "duplicate-refund-two"
	secondRefund.IdempotencyKey = "duplicate-refund-key-two"
	secondRefund.PayloadHash = "duplicate-refund-hash-two"
	result, err = repo.ProcessTransaction(context.Background(), secondRefund)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "REJECTED" || result.FailureCode != "reversal_already_processed" {
		t.Fatalf("expected duplicate reversal rejection, got %s/%s", result.Status, result.FailureCode)
	}
}

func TestProcessTransactionRefundReplay(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	bet := testRequest(playerID, walletID, "replay-refund-bet", "replay-refund-bet-key", "replay-refund-bet-hash", amount)
	if _, err := repo.ProcessTransaction(context.Background(), bet); err != nil {
		t.Fatal(err)
	}
	refund := testRequest(playerID, walletID, "replay-refund", "replay-refund-key", "replay-refund-hash", amount)
	refund.Kind = "REFUND"
	refund.ReferenceExternalTransactionID = bet.ExternalTransactionID
	first, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.TransactionID != first.TransactionID || replay.Balance.String() != "100.00" {
		t.Fatalf("unexpected refund replay: %+v", replay)
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
