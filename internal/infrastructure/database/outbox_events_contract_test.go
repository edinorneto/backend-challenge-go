package database_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/messaging"
)

type storedOutboxEvent struct {
	eventID       uuid.UUID
	aggregateType string
	aggregateID   uuid.UUID
	eventType     string
	correlationID uuid.UUID
	occurredAt    time.Time
	version       int
	payload       map[string]any
}

func TestOpeningOutboxEventsFollowContract(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")

	var openingID uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT id FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, walletID).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	events := loadOutboxEvents(t, pool, openingID)
	if len(events) != 2 {
		t.Fatalf("expected processed and balance events for OPENING, got %d", len(events))
	}

	processed := findEvent(t, events, messaging.EventTypeWagerTransactionProcessed)
	assertAggregate(t, processed, messaging.AggregateTypeWagerTransaction, openingID)
	assertPayload(t, processed, map[string]any{
		"transactionId": openingID.String(),
		"walletId":      walletID.String(),
		"playerId":      playerID.String(),
		"kind":          "OPENING",
		"status":        "PROCESSED",
	})
	assertMoney(t, processed.payload["money"], "100.00")
	for _, key := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceExternalTransactionId"} {
		if _, ok := processed.payload[key]; ok {
			t.Fatalf("internal OPENING event must not carry external field %s: %v", key, processed.payload)
		}
	}

	balance := findEvent(t, events, messaging.EventTypeWalletBalanceChanged)
	assertAggregate(t, balance, messaging.AggregateTypeWallet, walletID)
	assertPayload(t, balance, map[string]any{
		"walletId":      walletID.String(),
		"transactionId": openingID.String(),
		"direction":     "CREDIT",
		"walletVersion": float64(1),
	})
	assertMoney(t, balance.payload["money"], "100.00")
	assertMoney(t, balance.payload["balanceBefore"], "0.00")
	assertMoney(t, balance.payload["balanceAfter"], "100.00")
}

func TestExternalOperationOutboxEventsFollowContract(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")

	bet := testRequest(playerID, walletID, "contract-bet", "contract-bet-key", "contract-bet-hash", testMoney(t, "25.00"))
	betResult, err := repo.ProcessTransaction(context.Background(), bet)
	if err != nil {
		t.Fatal(err)
	}
	betEvents := loadOutboxEvents(t, pool, betResult.TransactionID)
	if len(betEvents) != 2 {
		t.Fatalf("expected processed and balance events for BET, got %d", len(betEvents))
	}
	processed := findEvent(t, betEvents, messaging.EventTypeWagerTransactionProcessed)
	assertAggregate(t, processed, messaging.AggregateTypeWagerTransaction, betResult.TransactionID)
	assertPayload(t, processed, map[string]any{
		"transactionId":         betResult.TransactionID.String(),
		"walletId":              walletID.String(),
		"playerId":              playerID.String(),
		"providerId":            bet.ProviderID,
		"externalTransactionId": bet.ExternalTransactionID,
		"roundId":               bet.RoundID,
		"gameId":                bet.GameID,
		"kind":                  "BET",
		"status":                "PROCESSED",
	})
	assertMoney(t, processed.payload["money"], "25.00")
	result, _ := processed.payload["result"].(map[string]any)
	assertMoney(t, result["balance"], "75.00")

	balance := findEvent(t, betEvents, messaging.EventTypeWalletBalanceChanged)
	assertAggregate(t, balance, messaging.AggregateTypeWallet, walletID)
	assertPayload(t, balance, map[string]any{
		"direction":       "DEBIT",
		"walletVersion":   float64(2),
		"previousVersion": float64(1),
	})
	assertMoney(t, balance.payload["balanceBefore"], "100.00")
	assertMoney(t, balance.payload["balanceAfter"], "75.00")

	loss := testRequest(playerID, walletID, "contract-loss", "contract-loss-key", "contract-loss-hash", testMoney(t, "0.00"))
	loss.Kind = "LOSS"
	lossResult, err := repo.ProcessTransaction(context.Background(), loss)
	if err != nil {
		t.Fatal(err)
	}
	lossEvents := loadOutboxEvents(t, pool, lossResult.TransactionID)
	if len(lossEvents) != 1 || lossEvents[0].eventType != messaging.EventTypeWagerTransactionProcessed {
		t.Fatalf("expected only WagerTransactionProcessed for LOSS, got %+v", lossEvents)
	}

	overdraft := testRequest(playerID, walletID, "contract-overdraft", "contract-overdraft-key", "contract-overdraft-hash", testMoney(t, "500.00"))
	rejectedResult, err := repo.ProcessTransaction(context.Background(), overdraft)
	if err != nil {
		t.Fatal(err)
	}
	rejectedEvents := loadOutboxEvents(t, pool, rejectedResult.TransactionID)
	if len(rejectedEvents) != 1 {
		t.Fatalf("expected only the rejection event, got %d", len(rejectedEvents))
	}
	rejected := findEvent(t, rejectedEvents, messaging.EventTypeWagerTransactionRejected)
	assertAggregate(t, rejected, messaging.AggregateTypeWagerTransaction, rejectedResult.TransactionID)
	assertPayload(t, rejected, map[string]any{
		"playerId":              playerID.String(),
		"providerId":            overdraft.ProviderID,
		"externalTransactionId": overdraft.ExternalTransactionID,
		"status":                "REJECTED",
		"failureCode":           rejectedResult.FailureCode,
	})
	if rejectedResult.FailureCode == "" {
		t.Fatal("expected a failure code for the rejected BET")
	}
	assertMoney(t, rejected.payload["money"], "500.00")
}

func TestReferenceExpiryRejectionEventCarriesOperationData(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "100.00")
	request := testRequest(playerID, walletID, "contract-expiry-refund", "contract-expiry-key", "contract-expiry-hash", testMoney(t, "25.00"))
	request.Kind = "REFUND"
	request.ReferenceExternalTransactionID = "contract-expiry-missing"

	result, err := repo.ProcessTransaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; result.Status == "PENDING_REFERENCE"; attempt++ {
		if attempt > 10 {
			t.Fatal("pending reference never expired")
		}
		if result, err = repo.RetryPendingReference(context.Background(), result.TransactionID); err != nil {
			t.Fatal(err)
		}
	}
	if result.Status != "REJECTED" {
		t.Fatalf("expected expiry rejection, got %s", result.Status)
	}

	rejected := findEvent(t, loadOutboxEvents(t, pool, result.TransactionID), messaging.EventTypeWagerTransactionRejected)
	assertPayload(t, rejected, map[string]any{
		"playerId":                       playerID.String(),
		"walletId":                       walletID.String(),
		"providerId":                     request.ProviderID,
		"externalTransactionId":          request.ExternalTransactionID,
		"roundId":                        request.RoundID,
		"gameId":                         request.GameID,
		"kind":                           "REFUND",
		"failureCode":                    "reference_not_found",
		"referenceExternalTransactionId": request.ReferenceExternalTransactionID,
	})
	assertMoney(t, rejected.payload["money"], "25.00")
}

func TestOutboxEventSnapshotIsImmutable(t *testing.T) {
	pool := testPool(t)
	requireOutboxSnapshotTrigger(t, pool)
	repo := database.NewWalletRepo(pool)
	_, walletID := createTestWallet(t, repo, pool, "10.00")

	var eventID uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT event_id FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WalletBalanceChanged'`, walletID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}

	for name, statement := range map[string]string{
		"payload":        `UPDATE outbox_events SET payload = '{"walletId":"tampered"}' WHERE event_id = $1`,
		"event type":     `UPDATE outbox_events SET event_type = 'WagerTransactionProcessed' WHERE event_id = $1`,
		"version":        `UPDATE outbox_events SET version = 2 WHERE event_id = $1`,
		"occurred at":    `UPDATE outbox_events SET occurred_at = occurred_at + INTERVAL '1 second' WHERE event_id = $1`,
		"aggregate":      `UPDATE outbox_events SET aggregate_id = gen_random_uuid() WHERE event_id = $1`,
		"correlation id": `UPDATE outbox_events SET correlation_id = gen_random_uuid() WHERE event_id = $1`,
		"event id":       `UPDATE outbox_events SET event_id = gen_random_uuid() WHERE event_id = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(context.Background(), statement, eventID)
			if err == nil || !strings.Contains(err.Error(), "outbox event snapshot is immutable") {
				t.Fatalf("expected snapshot mutation to be rejected, got %v", err)
			}
		})
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE outbox_events
		SET attempts = attempts + 1, next_attempt_at = NOW(), last_error = 'transient', locked_at = NULL, locked_by = NULL
		WHERE event_id = $1
	`, eventID); err != nil {
		t.Fatalf("delivery state must remain updatable: %v", err)
	}
}

func requireOutboxSnapshotTrigger(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_prevent_outbox_snapshot_update')
	`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("migration 000004 is not applied: run the migrations before the integration tests")
	}
}

func loadOutboxEvents(t *testing.T, pool *pgxpool.Pool, transactionID uuid.UUID) []storedOutboxEvent {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT event_id, aggregate_type, aggregate_id, event_type, correlation_id, occurred_at, version, payload
		FROM outbox_events
		WHERE correlation_id = $1
		ORDER BY event_type
	`, transactionID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var events []storedOutboxEvent
	for rows.Next() {
		var event storedOutboxEvent
		var payload []byte
		if err := rows.Scan(&event.eventID, &event.aggregateType, &event.aggregateID, &event.eventType,
			&event.correlationID, &event.occurredAt, &event.version, &payload); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload, &event.payload); err != nil {
			t.Fatal(err)
		}
		envelope := messaging.EventEnvelope{
			EventID:       event.eventID,
			EventType:     event.eventType,
			AggregateID:   event.aggregateID,
			CorrelationID: event.correlationID,
			OccurredAt:    event.occurredAt.UTC(),
			Version:       event.version,
			Data:          payload,
		}
		if err := envelope.Validate(); err != nil {
			t.Fatalf("stored %s does not form a valid envelope: %v", event.eventType, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}

func findEvent(t *testing.T, events []storedOutboxEvent, eventType string) storedOutboxEvent {
	t.Helper()
	var found []storedOutboxEvent
	for _, event := range events {
		if event.eventType == eventType {
			found = append(found, event)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one %s event, got %d", eventType, len(found))
	}
	return found[0]
}

func assertAggregate(t *testing.T, event storedOutboxEvent, aggregateType string, aggregateID uuid.UUID) {
	t.Helper()
	if event.aggregateType != aggregateType || event.aggregateID != aggregateID {
		t.Fatalf("%s: expected aggregate %s/%s, got %s/%s", event.eventType, aggregateType, aggregateID, event.aggregateType, event.aggregateID)
	}
}

func assertPayload(t *testing.T, event storedOutboxEvent, expected map[string]any) {
	t.Helper()
	for key, value := range expected {
		if event.payload[key] != value {
			t.Fatalf("%s: expected %s=%v, got %v", event.eventType, key, value, event.payload[key])
		}
	}
}

func assertMoney(t *testing.T, value any, amount string) {
	t.Helper()
	moneyValue, ok := value.(map[string]any)
	if !ok || moneyValue["amount"] != amount || moneyValue["currency"] != "BRL" {
		t.Fatalf("expected money %s BRL as decimal strings, got %#v", amount, value)
	}
}
