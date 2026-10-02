package database_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
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

func TestProcessTransactionPendingReferenceReplay(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	request := testRequest(playerID, walletID, "pending-replay", "pending-replay-key", "pending-replay-hash", amount)
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "missing-pending-replay-reference"

	first, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected first request to be pending, got %s", first.Status)
	}
	if first.FailureCode != "reference_pending" {
		t.Fatalf("expected reference_pending failure code, got %s", first.FailureCode)
	}

	replay, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.TransactionID != first.TransactionID {
		t.Fatalf("expected original transaction ID %s, got %s", first.TransactionID, replay.TransactionID)
	}
	if replay.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending replay, got %s", replay.Status)
	}
	if !replay.IdempotentReplay {
		t.Fatal("expected idempotent replay")
	}
	if replay.FailureCode != first.FailureCode {
		t.Fatalf("expected persisted failure code %q, got %q", first.FailureCode, replay.FailureCode)
	}

	var transactionCount int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`,
		request.ProviderID,
		request.IdempotencyKey,
	).Scan(&transactionCount); err != nil {
		t.Fatal(err)
	}
	if transactionCount != 1 {
		t.Fatalf("expected one wager transaction, got %d", transactionCount)
	}

	var ledgerCount int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`,
		walletID,
	).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("expected only the opening ledger entry, got %d", ledgerCount)
	}

	var balanceCents int64
	if err := pool.QueryRow(
		context.Background(),
		`SELECT balance_cents FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 10000 {
		t.Fatalf("expected wallet balance to remain 100.00, got %d cents", balanceCents)
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

func TestProcessTransactionFiftyConcurrentDuplicateBets(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	request := testRequest(
		playerID,
		walletID,
		"transaction-fifty-duplicates",
		"key-fifty-duplicates",
		"hash-fifty-duplicates",
		testMoney(t, "25.00"),
	)

	const duplicateCount = 50
	results := make([]ports.ProcessTransactionResult, duplicateCount)
	errs := make([]error, duplicateCount)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(duplicateCount)
	for index := range results {
		go func(index int) {
			defer group.Done()
			<-start
			results[index], errs[index] = repo.ProcessTransaction(context.Background(), request)
		}(index)
	}
	close(start)
	group.Wait()

	var transactionID uuid.UUID
	replayCount := 0
	for index, err := range errs {
		if err != nil {
			t.Fatalf("duplicate request %d failed: %v", index, err)
		}
		if results[index].Status != "PROCESSED" {
			t.Fatalf("duplicate request %d returned status %s", index, results[index].Status)
		}
		if transactionID == uuid.Nil {
			transactionID = results[index].TransactionID
		}
		if results[index].TransactionID != transactionID {
			t.Fatalf("duplicate request %d returned transaction %s, expected %s", index, results[index].TransactionID, transactionID)
		}
		if results[index].IdempotentReplay {
			replayCount++
		}
		if results[index].Balance.String() != "75.00" {
			t.Fatalf("duplicate request %d returned balance %s, expected 75.00", index, results[index].Balance)
		}
	}
	if replayCount != duplicateCount-1 {
		t.Fatalf("expected %d idempotent replays, got %d", duplicateCount-1, replayCount)
	}

	var transactionCount int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT COUNT(*) FROM wager_transactions
		 WHERE provider_id = $1 AND external_transaction_id = $2 AND idempotency_key = $3 AND payload_hash = $4`,
		request.ProviderID,
		request.ExternalTransactionID,
		request.IdempotencyKey,
		request.PayloadHash,
	).Scan(&transactionCount); err != nil {
		t.Fatal(err)
	}
	if transactionCount != 1 {
		t.Fatalf("expected one wager transaction, got %d", transactionCount)
	}

	var ledgerCount int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries
		 WHERE wallet_id = $1 AND direction = 'DEBIT' AND transaction_id = $2`,
		walletID,
		transactionID,
	).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("expected one debit ledger entry, got %d", ledgerCount)
	}

	var balanceCents int64
	if err := pool.QueryRow(
		context.Background(),
		`SELECT balance_cents FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 7500 {
		t.Fatalf("expected wallet balance 75.00, got %d cents", balanceCents)
	}
	if balanceCents < 0 {
		t.Fatalf("wallet balance must not be negative, got %d cents", balanceCents)
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
	var debits int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID).Scan(&debits); err != nil {
		t.Fatal(err)
	}
	if debits != 1 {
		t.Fatalf("expected a single debit in the ledger, got %d", debits)
	}

	// Resending both bets replays the original outcomes without changing anything.
	for index, request := range requests {
		replay, err := repo.ProcessTransaction(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if !replay.IdempotentReplay || replay.TransactionID != results[index].TransactionID ||
			replay.Status != results[index].Status || replay.FailureCode != results[index].FailureCode ||
			replay.Balance.String() != results[index].Balance.String() {
			t.Fatalf("resend %d changed the outcome: original %+v, replay %+v", index, results[index], replay)
		}
	}
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID).Scan(&debits); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 2000 || debits != 1 {
		t.Fatalf("resends changed the wallet: balance=%d debits=%d", balanceCents, debits)
	}
	assertStoredBalanceMatchesLedger(t, pool, walletID)
}

// Wallet coordination is a row lock on that wallet only: while one wallet is
// locked by an open transaction, an operation on another wallet must complete.
func TestProcessTransactionDifferentWalletsDoNotWaitForEachOther(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	_, lockedWalletID := createTestWallet(t, repo, pool, "100.00")
	playerID, freeWalletID := createTestWallet(t, repo, pool, "100.00")

	ctx := context.Background()
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, `SELECT id FROM wallets WHERE id = $1 FOR UPDATE`, lockedWalletID); err != nil {
		t.Fatal(err)
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	request := testRequest(playerID, freeWalletID, "parallel-free-wallet", "parallel-free-wallet-key", "parallel-free-wallet-hash", testMoney(t, "25.00"))
	result, err := repo.ProcessTransaction(opCtx, request)
	if err != nil {
		t.Fatalf("operation on another wallet waited for the locked wallet: %v", err)
	}
	if result.Status != "PROCESSED" || result.Balance.String() != "75.00" {
		t.Fatalf("expected the free wallet to be debited, got %+v", result)
	}
}

func assertStoredBalanceMatchesLedger(t *testing.T, pool *pgxpool.Pool, walletID uuid.UUID) {
	t.Helper()
	var stored, calculated int64
	if err := pool.QueryRow(context.Background(), `
		SELECT w.balance_cents,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_cents ELSE -l.amount_cents END), 0)
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1 GROUP BY w.balance_cents
	`, walletID).Scan(&stored, &calculated); err != nil {
		t.Fatal(err)
	}
	if stored != calculated {
		t.Fatalf("stored balance %d differs from ledger credits minus debits %d", stored, calculated)
	}
}

func TestProcessTransactionConcurrentBetsDifferentWallets(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerOneID, walletOneID := createTestWallet(t, repo, pool, "100.00")
	playerTwoID, walletTwoID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	requests := []ports.ProcessTransactionRequest{
		testRequest(playerOneID, walletOneID, "transaction-wallet-one", "key-wallet-one", "hash-wallet-one", amount),
		testRequest(playerTwoID, walletTwoID, "transaction-wallet-two", "key-wallet-two", "hash-wallet-two", amount),
	}
	results := make([]ports.ProcessTransactionResult, len(requests))
	errs := make([]error, len(requests))
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(len(requests))
	for index := range requests {
		go func(index int) {
			defer group.Done()
			<-start
			results[index], errs[index] = repo.ProcessTransaction(context.Background(), requests[index])
		}(index)
	}
	close(start)
	group.Wait()

	for index, result := range results {
		if errs[index] != nil {
			t.Fatalf("wallet %d operation failed: %v", index+1, errs[index])
		}
		if result.Status != "PROCESSED" {
			t.Fatalf("wallet %d operation returned status %s", index+1, result.Status)
		}
		if result.IdempotentReplay {
			t.Fatalf("wallet %d operation was unexpectedly treated as an idempotent replay", index+1)
		}
		if result.Balance.String() != "75.00" {
			t.Fatalf("wallet %d operation returned balance %s, expected 75.00", index+1, result.Balance)
		}
	}

	for index, walletID := range []uuid.UUID{walletOneID, walletTwoID} {
		var balanceCents int64
		if err := pool.QueryRow(
			context.Background(),
			`SELECT balance_cents FROM wallets WHERE id = $1`,
			walletID,
		).Scan(&balanceCents); err != nil {
			t.Fatalf("wallet %d balance query failed: %v", index+1, err)
		}
		if balanceCents != 7500 {
			t.Fatalf("wallet %d expected final balance 75.00, got %d cents", index+1, balanceCents)
		}
		if balanceCents < 0 {
			t.Fatalf("wallet %d balance must not be negative, got %d cents", index+1, balanceCents)
		}

		var ledgerCount int
		if err := pool.QueryRow(
			context.Background(),
			`SELECT COUNT(*) FROM wallet_ledger_entries
			 WHERE wallet_id = $1 AND direction = 'DEBIT'`,
			walletID,
		).Scan(&ledgerCount); err != nil {
			t.Fatalf("wallet %d ledger query failed: %v", index+1, err)
		}
		if ledgerCount != 1 {
			t.Fatalf("wallet %d expected one debit ledger entry, got %d", index+1, ledgerCount)
		}
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

func TestProcessNextPendingReferenceProcessesOnlyDueRows(t *testing.T) {
	pool := isolatedTestPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	request := testRequest(playerID, walletID, "worker-due-reference", "worker-due-key", "worker-due-hash", amount)
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "worker-due-missing"
	pending, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW() + INTERVAL '1 hour'
		WHERE id = $1
	`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}
	processed, err := repo.ProcessNextPendingReference(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed {
		t.Fatal("worker processed a reference before next_attempt_at")
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
	`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}
	processed, err = repo.ProcessNextPendingReference(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("worker did not process a due pending reference")
	}

	var attempts int
	if err := pool.QueryRow(context.Background(), `
		SELECT reference_attempts FROM wager_transactions WHERE id = $1
	`, pending.TransactionID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected retry attempt 2, got %d", attempts)
	}
}

func TestProcessNextPendingReferenceClaimsOnceAcrossConcurrentWorkers(t *testing.T) {
	pool := isolatedTestPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	request := testRequest(playerID, walletID, "worker-concurrent-reference", "worker-concurrent-key", "worker-concurrent-hash", amount)
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "worker-concurrent-missing"
	pending, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
	`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processed, err := repo.ProcessNextPendingReference(context.Background())
			results <- processed
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	processedCount := 0
	for processed := range results {
		if processed {
			processedCount++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if processedCount != 1 {
		t.Fatalf("expected one worker to claim the pending reference, got %d", processedCount)
	}
}

func TestProcessNextPendingReferenceRecoversPersistedRow(t *testing.T) {
	pool := isolatedTestPool(t)
	firstRepo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, firstRepo, pool, "100.00")
	amount := testMoney(t, "25.00")

	request := testRequest(playerID, walletID, "worker-restart-reference", "worker-restart-key", "worker-restart-hash", amount)
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "worker-restart-missing"
	pending, err := firstRepo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
	`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}

	restartedRepo := database.NewWalletRepo(pool)
	processed, err := restartedRepo.ProcessNextPendingReference(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("new repository instance did not recover the persisted pending reference")
	}

	var attempts int
	if err := pool.QueryRow(context.Background(), `
		SELECT reference_attempts FROM wager_transactions WHERE id = $1
	`, pending.TransactionID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected recovered retry attempt 2, got %d", attempts)
	}
}

func TestRetryPendingReferenceProcessesExistingBet(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	refund := testRequest(playerID, walletID, "retry-refund", "retry-refund-key", "retry-refund-hash", amount)
	refund.Kind = "REFUND"
	refund.ReferenceExternalTransactionID = "retry-bet"
	pending, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil || pending.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending refund, got %+v/%v", pending, err)
	}

	bet := testRequest(playerID, walletID, "retry-bet", "retry-bet-key", "retry-bet-hash", amount)
	if result, err := repo.ProcessTransaction(context.Background(), bet); err != nil || result.Status != "PROCESSED" {
		t.Fatalf("expected processed reference bet, got %+v/%v", result, err)
	}

	result, err := repo.RetryPendingReference(context.Background(), pending.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PROCESSED" || result.Balance.String() != "100.00" {
		t.Fatalf("expected processed retry with 100.00, got %+v", result)
	}

	var referenceID *uuid.UUID
	var resultBalance int64
	var resultVersion int64
	if err := pool.QueryRow(context.Background(), `
			SELECT reference_transaction_id, result_balance_cents, result_wallet_version
			FROM wager_transactions WHERE id = $1
		`, pending.TransactionID).Scan(&referenceID, &resultBalance, &resultVersion); err != nil {
		t.Fatal(err)
	}
	var betID uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT id FROM wager_transactions WHERE external_transaction_id = 'retry-bet' AND provider_id = $1`, refund.ProviderID).Scan(&betID); err != nil {
		t.Fatal(err)
	}
	if referenceID == nil || *referenceID != betID || resultBalance != 10000 || resultVersion < 1 {
		t.Fatalf("retry reference state invalid: reference=%v balance=%d version=%d", referenceID, resultBalance, resultVersion)
	}
	var direction string
	if err := pool.QueryRow(context.Background(), `
			SELECT direction FROM wallet_ledger_entries WHERE transaction_id = $1
		`, pending.TransactionID).Scan(&direction); err != nil {
		t.Fatal(err)
	}
	if direction != "CREDIT" {
		t.Fatalf("expected refund credit ledger, got %s", direction)
	}
	var eventCount int
	if err := pool.QueryRow(context.Background(), `
			SELECT COUNT(*) FROM outbox_events
			WHERE correlation_id = $1 AND event_type IN ('WagerTransactionProcessed', 'WalletBalanceChanged')
		`, pending.TransactionID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 {
		t.Fatalf("expected two processed retry events, got %d", eventCount)
	}
}

func TestRetryPendingReferenceProcessesRollbackWin(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	rollback := testRequest(playerID, walletID, "retry-rollback", "retry-rollback-key", "retry-rollback-hash", amount)
	rollback.Kind = "ROLLBACK"
	rollback.ReferenceExternalTransactionID = "retry-win"
	pending, err := repo.ProcessTransaction(context.Background(), rollback)
	if err != nil || pending.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending rollback, got %+v/%v", pending, err)
	}
	win := testRequest(playerID, walletID, "retry-win", "retry-win-key", "retry-win-hash", amount)
	win.Kind = "WIN"
	if result, err := repo.ProcessTransaction(context.Background(), win); err != nil || result.Status != "PROCESSED" {
		t.Fatalf("expected processed reference win, got %+v/%v", result, err)
	}

	result, err := repo.RetryPendingReference(context.Background(), pending.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PROCESSED" || result.Balance.String() != "100.00" {
		t.Fatalf("expected processed rollback retry with 100.00, got %+v", result)
	}
	var direction string
	var payload []byte
	if err := pool.QueryRow(context.Background(), `
			SELECT l.direction, o.payload
			FROM wallet_ledger_entries l
			JOIN outbox_events o ON o.correlation_id = l.transaction_id
			WHERE l.transaction_id = $1 AND o.event_type = 'WalletBalanceChanged'
		`, pending.TransactionID).Scan(&direction, &payload); err != nil {
		t.Fatal(err)
	}
	if direction != "DEBIT" || !strings.Contains(string(payload), `"direction": "DEBIT"`) {
		t.Fatalf("expected retry rollback debit direction, got %s/%s", direction, payload)
	}
}

func TestRetryPendingReferenceDoesNotDuplicateProcessedReversal(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")
	bet := testRequest(playerID, walletID, "retry-duplicate-bet", "retry-duplicate-bet-key", "retry-duplicate-bet-hash", amount)
	if _, err := repo.ProcessTransaction(context.Background(), bet); err != nil {
		t.Fatal(err)
	}
	refund := testRequest(playerID, walletID, "retry-duplicate-refund", "retry-duplicate-refund-key", "retry-duplicate-refund-hash", amount)
	refund.Kind = "REFUND"
	refund.ReferenceExternalTransactionID = bet.ExternalTransactionID
	processedRefund, err := repo.ProcessTransaction(context.Background(), refund)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.RetryPendingReference(context.Background(), processedRefund.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	var balanceCents, ledgerCount int
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, processedRefund.TransactionID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 10000 || ledgerCount != 1 {
		t.Fatalf("retry of terminal reversal changed state: balance=%d ledger=%d", balanceCents, ledgerCount)
	}
}

func TestRetryPendingReferenceIncrementsWhenReferenceStillPending(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")
	first := testRequest(playerID, walletID, "still-pending-reference", "still-pending-reference-key", "still-pending-reference-hash", amount)
	first.Kind = "REFUND"
	first.ReferenceExternalTransactionID = "missing-underlying"
	pendingReference, err := repo.ProcessTransaction(context.Background(), first)
	if err != nil || pendingReference.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending reference, got %+v/%v", pendingReference, err)
	}
	second := testRequest(playerID, walletID, "retry-against-pending", "retry-against-pending-key", "retry-against-pending-hash", amount)
	second.Kind = "REFUND"
	second.ReferenceExternalTransactionID = first.ExternalTransactionID
	pending, err := repo.ProcessTransaction(context.Background(), second)
	if err != nil || pending.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending second reversal, got %+v/%v", pending, err)
	}
	result, err := repo.RetryPendingReference(context.Background(), pending.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected retry to remain pending, got %s", result.Status)
	}
	var attempts int
	if err := pool.QueryRow(context.Background(), `SELECT reference_attempts FROM wager_transactions WHERE id = $1`, pending.TransactionID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected attempts to increment to 2, got %d", attempts)
	}
	var balanceCents, ledgerCount int
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, pending.TransactionID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 10000 || ledgerCount != 0 {
		t.Fatalf("pending reference retry changed financial state: balance=%d ledger=%d", balanceCents, ledgerCount)
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

func TestReversalOfRejectedReferenceIsRejectedWithoutWaiting(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "10.00")
	bet := testRequest(playerID, walletID, "rejected-reference-bet", "rejected-reference-bet-key", "rejected-reference-bet-hash", testMoney(t, "25.00"))
	betResult, err := repo.ProcessTransaction(context.Background(), bet)
	if err != nil || betResult.Status != "REJECTED" || betResult.FailureCode != "insufficient_funds" {
		t.Fatalf("expected rejected bet, got %+v/%v", betResult, err)
	}

	for _, kind := range []string{"REFUND", "ROLLBACK"} {
		reversal := testRequest(playerID, walletID, "rejected-reference-"+kind, "rejected-reference-"+kind+"-key", "rejected-reference-"+kind+"-hash", testMoney(t, "25.00"))
		reversal.Kind = kind
		reversal.ReferenceExternalTransactionID = bet.ExternalTransactionID
		result, err := repo.ProcessTransaction(context.Background(), reversal)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != "REJECTED" || result.FailureCode != "reference_incompatible" {
			t.Fatalf("%s of a rejected bet: expected reference_incompatible rejection, got %s/%s", kind, result.Status, result.FailureCode)
		}
	}

	var balanceCents, ledgerCount int
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 1000 || ledgerCount != 1 {
		t.Fatalf("reversal of a rejected bet changed financial state: balance=%d ledger=%d", balanceCents, ledgerCount)
	}
}

// isolatedTestPool returns a pool bound to a fresh schema with every migration
// applied. Tests that claim pending references globally use it so they neither
// see nor reschedule rows from other tests or from a running stack.
func isolatedTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := testPool(t)
	ctx := context.Background()
	schema := "reference_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := base.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = base.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })

	poolConfig := base.Config().Copy()
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// Concurrent operations on one wallet both insert a wager transaction (taking
// FOR KEY SHARE on the wallet through the foreign key) before locking the wallet.
// The wallet lock must not conflict with that, or the pair deadlocks and one
// request fails. Many rounds make the interleaving very likely.
func TestProcessTransactionConcurrentBetsDoNotDeadlock(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	for round := 0; round < 40; round++ {
		playerID, walletID := createTestWallet(t, repo, pool, "100.00")
		requests := []ports.ProcessTransactionRequest{
			testRequest(playerID, walletID, fmt.Sprintf("deadlock-a-%d", round), fmt.Sprintf("deadlock-a-key-%d", round), "deadlock-a-hash", testMoney(t, "80.00")),
			testRequest(playerID, walletID, fmt.Sprintf("deadlock-b-%d", round), fmt.Sprintf("deadlock-b-key-%d", round), "deadlock-b-hash", testMoney(t, "80.00")),
		}
		errs := make([]error, len(requests))
		statuses := make([]string, len(requests))
		start := make(chan struct{})
		var group sync.WaitGroup
		for index := range requests {
			group.Add(1)
			go func(index int) {
				defer group.Done()
				<-start
				result, err := repo.ProcessTransaction(context.Background(), requests[index])
				errs[index], statuses[index] = err, result.Status
			}(index)
		}
		close(start)
		group.Wait()
		for index, err := range errs {
			if err != nil {
				t.Fatalf("round %d request %d failed: %v", round, index, err)
			}
		}
		if (statuses[0] != "PROCESSED" || statuses[1] != "REJECTED") && (statuses[0] != "REJECTED" || statuses[1] != "PROCESSED") {
			t.Fatalf("round %d: expected one processed and one rejected bet, got %v", round, statuses)
		}
	}
}

// An operation that waits for another writer of its wallet is visible in the
// wallet lock metrics; an uncontended one only adds to the wait summary.
func TestProcessTransactionRecordsWalletLockContention(t *testing.T) {
	pool := testPool(t)
	metrics := observability.NewMetrics()
	repo := database.NewWalletRepo(pool).WithMetrics(metrics)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")

	if _, err := repo.ProcessTransaction(context.Background(), testRequest(playerID, walletID, "lock-free", "lock-free-key", "lock-free-hash", testMoney(t, "1.00"))); err != nil {
		t.Fatal(err)
	}
	if metrics.Snapshot("wallet_lock_contended_total") != 0 {
		t.Fatal("an uncontended lock must not count as contention")
	}

	holder, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(context.Background(), `SELECT id FROM wallets WHERE id = $1 FOR NO KEY UPDATE`, walletID); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = holder.Rollback(context.Background())
		close(released)
	}()
	if _, err := repo.ProcessTransaction(context.Background(), testRequest(playerID, walletID, "lock-contended", "lock-contended-key", "lock-contended-hash", testMoney(t, "1.00"))); err != nil {
		t.Fatal(err)
	}
	<-released
	if got := metrics.Snapshot("wallet_lock_contended_total"); got != 1 {
		t.Fatalf("expected the waiting operation to be counted as contention, got %d", got)
	}
}

// The schema, not only the wallet-opening code, prevents a second initial credit.
func TestSchemaRejectsSecondOpeningForAWallet(t *testing.T) {
	pool := testPool(t)
	var exists bool
	if err := pool.QueryRow(context.Background(), `SELECT to_regclass('uq_wager_opening_per_wallet') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("migration 000005 is not applied: run the migrations before the integration tests")
	}
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")

	_, err := pool.Exec(context.Background(), `
		INSERT INTO wager_transactions (id, source, player_id, wallet_id, kind, status, amount_cents, currency,
			result_balance_cents, result_wallet_version, reference_attempts, created_at, updated_at, processed_at)
		VALUES ($1, 'INTERNAL', $2, $3, 'OPENING', 'PROCESSED', 5000, 'BRL', 5000, 1, 0, NOW(), NOW(), NOW())
	`, uuid.New(), playerID, walletID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_wager_opening_per_wallet" {
		t.Fatalf("expected the second OPENING to violate uq_wager_opening_per_wallet, got %v", err)
	}
}

// The ledger is append-only in the database: no entry can be edited or removed,
// whatever the client.
func TestLedgerEntriesCannotBeUpdatedOrDeleted(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	_, walletID := createTestWallet(t, repo, pool, "100.00")

	for name, statement := range map[string]string{
		"amount":        `UPDATE wallet_ledger_entries SET amount_cents = 1 WHERE wallet_id = $1`,
		"balance after": `UPDATE wallet_ledger_entries SET balance_after_cents = 999999 WHERE wallet_id = $1`,
		"direction":     `UPDATE wallet_ledger_entries SET direction = 'DEBIT' WHERE wallet_id = $1`,
		"delete":        `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(context.Background(), statement, walletID)
			if err == nil || !strings.Contains(err.Error(), "wallet ledger is append-only") {
				t.Fatalf("expected the ledger trigger to refuse the change, got %v", err)
			}
		})
	}
	var amount, after int64
	if err := pool.QueryRow(context.Background(), `SELECT amount_cents, balance_after_cents FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&amount, &after); err != nil {
		t.Fatal(err)
	}
	if amount != 10000 || after != 10000 {
		t.Fatalf("ledger entry changed: amount=%d after=%d", amount, after)
	}
}

// A wallet opened with zero balance has no OPENING, no ledger entry and no
// financial events; a positive opening creates exactly one of each kind.
func TestOpeningWithZeroBalanceCreatesNoFinancialRecords(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	_, walletID := createTestWallet(t, repo, pool, "0.00")

	var openings, entries, events, version int
	if err := pool.QueryRow(context.Background(), `
		SELECT (SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1),
		       (SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1),
		       (SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1),
		       (SELECT version FROM wallets WHERE id = $1)
	`, walletID).Scan(&openings, &entries, &events, &version); err != nil {
		t.Fatal(err)
	}
	if openings != 0 || entries != 0 || events != 0 || version != 1 {
		t.Fatalf("zero opening created records: openings=%d entries=%d events=%d version=%d", openings, entries, events, version)
	}
}
