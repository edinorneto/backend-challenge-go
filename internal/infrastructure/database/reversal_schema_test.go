package database_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
)

// The schema, not only the repository, allows one processed reversal per
// reference across kinds: after a ROLLBACK of a bet, a processed REFUND of the
// same bet violates the index.
func TestSchemaRejectsSecondProcessedReversalOfAReference(t *testing.T) {
	pool := testPool(t)
	var exists bool
	if err := pool.QueryRow(context.Background(), `SELECT to_regclass('uq_wager_processed_reversal_reference') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("migration 000006 is not applied: run the migrations before the integration tests")
	}
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	amount := testMoney(t, "25.00")

	bet := testRequest(playerID, walletID, "schema-reversal-bet", "schema-reversal-bet-key", "schema-reversal-bet-hash", amount)
	if _, err := repo.ProcessTransaction(context.Background(), bet); err != nil {
		t.Fatal(err)
	}
	rollback := testRequest(playerID, walletID, "schema-reversal-rollback", "schema-reversal-rollback-key", "schema-reversal-rollback-hash", amount)
	rollback.Kind = "ROLLBACK"
	rollback.ReferenceExternalTransactionID = bet.ExternalTransactionID
	result, err := repo.ProcessTransaction(context.Background(), rollback)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "PROCESSED" {
		t.Fatalf("expected processed rollback, got %s", result.Status)
	}

	var betID uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT id FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, bet.ProviderID, bet.ExternalTransactionID).Scan(&betID); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), `
		INSERT INTO wager_transactions (id, source, provider_id, external_transaction_id, idempotency_key, payload_hash,
			player_id, wallet_id, round_id, game_id, kind, status, amount_cents, currency,
			reference_external_transaction_id, reference_transaction_id, created_at, updated_at, processed_at)
		VALUES ($1, 'EXTERNAL', $2, 'schema-reversal-refund', 'schema-reversal-refund-key', 'schema-reversal-refund-hash',
			$3, $4, 'round-test', 'game-test', 'REFUND', 'PROCESSED', 2500, 'BRL', $5, $6, NOW(), NOW(), NOW())
	`, uuid.New(), bet.ProviderID, playerID, walletID, bet.ExternalTransactionID, betID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_wager_processed_reversal_reference" {
		t.Fatalf("expected the second reversal to violate uq_wager_processed_reversal_reference, got %v", err)
	}
}
