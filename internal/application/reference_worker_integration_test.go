package application

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

// Three workers with their own pools stand in for independent instances. The
// pending reversal is registered by a pool that is closed before they start, as
// if its instance had stopped.
func TestReferenceWorkersResolveReversalRegisteredByStoppedInstance(t *testing.T) {
	pool, cleanup := isolatedOutboxPool(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	firstInstance := schemaPool(t, pool)
	firstRepo := database.NewWalletRepo(firstInstance)
	playerID, walletID := createWorkerTestWallet(t, firstRepo, "100.00")

	refund := workerTestRequest(t, playerID, walletID, "worker-refund", "25.00")
	refund.Kind = "REFUND"
	refund.ReferenceExternalTransactionID = "worker-bet"
	pending, err := firstRepo.ProcessTransaction(ctx, refund)
	if err != nil || pending.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending reference, got %+v/%v", pending, err)
	}
	bet := workerTestRequest(t, playerID, walletID, "worker-bet", "25.00")
	if result, err := firstRepo.ProcessTransaction(ctx, bet); err != nil || result.Status != "PROCESSED" {
		t.Fatalf("expected processed bet, got %+v/%v", result, err)
	}
	firstInstance.Close()

	makeDue(t, pool, pending.TransactionID)
	stop := startWorkers(t, pool, 3)
	waitForStatus(t, pool, pending.TransactionID, "PROCESSED")
	stop()

	var balanceCents int64
	var version int64
	if err := pool.QueryRow(ctx, `SELECT balance_cents, version FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents, &version); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 10000 || version != 3 {
		t.Fatalf("expected balance 100.00 at version 3 after bet and refund, got %d/%d", balanceCents, version)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE transaction_id = $1 AND direction = 'CREDIT'`, pending.TransactionID, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM outbox_events WHERE correlation_id = $1 AND event_type = 'WagerTransactionPendingReference'`, pending.TransactionID, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM outbox_events WHERE correlation_id = $1 AND event_type = 'WagerTransactionProcessed'`, pending.TransactionID, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM outbox_events WHERE correlation_id = $1 AND event_type = 'WalletBalanceChanged'`, pending.TransactionID, 1)
	assertLedgerMatchesBalance(t, pool, walletID)

	replay, err := database.NewWalletRepo(pool).ProcessTransaction(ctx, refund)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.Status != "PROCESSED" || replay.Balance.String() != "100.00" {
		t.Fatalf("expected replay of the resolved refund, got %+v", replay)
	}
}

func TestReferenceWorkerRejectsExpiredReferenceWithObservedBalance(t *testing.T) {
	pool, cleanup := isolatedOutboxPool(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createWorkerTestWallet(t, repo, "100.00")

	rollback := workerTestRequest(t, playerID, walletID, "worker-expired-rollback", "25.00")
	rollback.Kind = "ROLLBACK"
	rollback.ReferenceExternalTransactionID = "worker-never-arrives"
	pending, err := repo.ProcessTransaction(ctx, rollback)
	if err != nil || pending.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending reference, got %+v/%v", pending, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_attempts = 5 WHERE id = $1`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}
	makeDue(t, pool, pending.TransactionID)

	stop := startWorkers(t, pool, 1)
	waitForStatus(t, pool, pending.TransactionID, "REJECTED")
	stop()

	var failureCode string
	var resultBalance, resultVersion int64
	if err := pool.QueryRow(ctx, `
		SELECT failure_code, result_balance_cents, result_wallet_version FROM wager_transactions WHERE id = $1
	`, pending.TransactionID).Scan(&failureCode, &resultBalance, &resultVersion); err != nil {
		t.Fatal(err)
	}
	if failureCode != "reference_not_found" || resultBalance != 10000 || resultVersion != 1 {
		t.Fatalf("expected reference_not_found with observed balance 100.00 v1, got %s/%d/%d", failureCode, resultBalance, resultVersion)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, pending.TransactionID, 0)
	assertCount(t, pool, `SELECT COUNT(*) FROM outbox_events WHERE correlation_id = $1 AND event_type = 'WagerTransactionRejected'`, pending.TransactionID, 1)

	replay, err := repo.ProcessTransaction(ctx, rollback)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.Status != "REJECTED" || replay.FailureCode != "reference_not_found" || replay.Balance.String() != "100.00" {
		t.Fatalf("expected rejected replay with the observed balance, got %+v", replay)
	}
}

func schemaPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	instancePool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instancePool.Close)
	return instancePool
}

func startWorkers(t *testing.T, pool *pgxpool.Pool, count int) func() {
	t.Helper()
	workers := make([]*ReferenceWorker, 0, count)
	for i := 0; i < count; i++ {
		worker := NewReferenceWorker(database.NewWalletRepo(schemaPool(t, pool)), config.Config{ReferencePollInterval: 10 * time.Millisecond})
		if err := worker.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		workers = append(workers, worker)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		for _, worker := range workers {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := worker.Stop(stopCtx); err != nil {
				t.Errorf("stop reference worker: %v", err)
			}
			cancel()
		}
	}
	t.Cleanup(stop)
	return stop
}

func createWorkerTestWallet(t *testing.T, repo *database.WalletRepo, amount string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	playerID := uuid.New()
	walletID := uuid.New()
	w, err := wallet.New(walletID, playerID, mustMoney(t, amount), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	return playerID, walletID
}

func workerTestRequest(t *testing.T, playerID, walletID uuid.UUID, externalID, amount string) ports.ProcessTransactionRequest {
	return ports.ProcessTransactionRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: externalID,
		IdempotencyKey:        "provider-a:" + externalID,
		PayloadHash:           "hash-" + externalID,
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               "worker-round",
		GameID:                "worker-game",
		Kind:                  "BET",
		Amount:                mustMoney(t, amount),
	}
}

func makeDue(t *testing.T, pool *pgxpool.Pool, transactionID uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		UPDATE wager_transactions SET reference_next_attempt_at = NOW() - INTERVAL '1 second' WHERE id = $1
	`, transactionID); err != nil {
		t.Fatal(err)
	}
}

func waitForStatus(t *testing.T, pool *pgxpool.Pool, transactionID uuid.UUID, expected string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(context.Background(), `SELECT status FROM wager_transactions WHERE id = $1`, transactionID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected transaction %s to reach %s, last status %s", transactionID, expected, status)
}

func assertCount(t *testing.T, pool *pgxpool.Pool, query string, transactionID uuid.UUID, expected int) {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), query, transactionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("expected %d rows for %q, got %d", expected, query, count)
	}
}

func assertLedgerMatchesBalance(t *testing.T, pool *pgxpool.Pool, walletID uuid.UUID) {
	t.Helper()
	var stored, calculated int64
	if err := pool.QueryRow(context.Background(), `
		SELECT w.balance_cents,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_cents ELSE -l.amount_cents END), 0)
		FROM wallets w
		LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1
		GROUP BY w.balance_cents
	`, walletID).Scan(&stored, &calculated); err != nil {
		t.Fatal(err)
	}
	if stored != calculated {
		t.Fatalf("stored balance %d differs from ledger %d", stored, calculated)
	}
}
