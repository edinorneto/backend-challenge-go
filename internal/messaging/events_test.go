package messaging

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestEventConstructorsDefineConcreteDataAndVersion(t *testing.T) {
	walletID := uuid.New()
	transactionID := uuid.New()
	processed := NewWagerTransactionProcessed(WagerTransactionProcessedData{
		TransactionID: transactionID,
		WalletID:      walletID,
	})
	assertEventData(t, processed, "WagerTransactionProcessed", WagerTransactionProcessedVersion, WagerTransactionProcessedData{})

	rejected := NewWagerTransactionRejected(WagerTransactionRejectedData{
		TransactionID: transactionID,
		WalletID:      walletID,
	})
	assertEventData(t, rejected, "WagerTransactionRejected", WagerTransactionRejectedVersion, WagerTransactionRejectedData{})

	event := NewWalletBalanceChanged(WalletBalanceChangedData{
		WalletID:      walletID,
		TransactionID: transactionID,
		Direction:     "CREDIT",
		Money:         MoneyData{Amount: "10.00", Currency: "BRL"},
		BalanceBefore: MoneyData{Amount: "0.00", Currency: "BRL"},
		BalanceAfter:  MoneyData{Amount: "10.00", Currency: "BRL"},
		WalletVersion: 2,
	})

	assertEventData(t, event, "WalletBalanceChanged", WalletBalanceChangedVersion, WalletBalanceChangedData{})

	pending := NewWagerTransactionPendingReference(WagerTransactionPendingReferenceData{
		TransactionID: transactionID,
		WalletID:      walletID,
	})
	assertEventData(t, pending, "WagerTransactionPendingReference", WagerTransactionPendingReferenceVersion, WagerTransactionPendingReferenceData{})

	data := event.Data.(WalletBalanceChangedData)
	if data.WalletID != walletID || data.TransactionID != transactionID ||
		data.Direction == "" || data.Money.Amount == "" ||
		data.BalanceBefore.Amount == "" || data.BalanceAfter.Amount == "" ||
		data.WalletVersion != 2 {
		t.Fatalf("incomplete WalletBalanceChanged data: %+v", data)
	}
}

func assertEventData(t *testing.T, event Event, eventType string, version int, expected any) {
	t.Helper()
	if event.EventType != eventType || event.Version != version {
		t.Fatalf("unexpected event metadata: %+v", event)
	}
	switch expected.(type) {
	case WagerTransactionProcessedData:
		if _, ok := event.Data.(WagerTransactionProcessedData); !ok {
			t.Fatalf("expected concrete WagerTransactionProcessedData, got %T", event.Data)
		}
	case WagerTransactionRejectedData:
		if _, ok := event.Data.(WagerTransactionRejectedData); !ok {
			t.Fatalf("expected concrete WagerTransactionRejectedData, got %T", event.Data)
		}
	case WalletBalanceChangedData:
		if _, ok := event.Data.(WalletBalanceChangedData); !ok {
			t.Fatalf("expected concrete WalletBalanceChangedData, got %T", event.Data)
		}
	case WagerTransactionPendingReferenceData:
		if _, ok := event.Data.(WagerTransactionPendingReferenceData); !ok {
			t.Fatalf("expected concrete WagerTransactionPendingReferenceData, got %T", event.Data)
		}
	default:
		t.Fatalf("unsupported expected event data type %T", expected)
	}
}

func TestWagerTransactionRejectedPreservesPendingReferenceData(t *testing.T) {
	event := NewWagerTransactionRejected(WagerTransactionRejectedData{
		TransactionID:                  uuid.New(),
		WalletID:                       uuid.New(),
		ProviderID:                     "provider-a",
		Kind:                           "REFUND",
		Status:                         "REJECTED",
		FailureCode:                    "reference_not_found",
		ReferenceExternalTransactionID: "bet-123",
		ReferenceAttempts:              5,
		NextAttemptAt:                  "2026-09-30T12:00:00Z",
	})

	body, err := json.Marshal(event.Data)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatal(err)
	}
	for key, expected := range map[string]any{
		"providerId":                     "provider-a",
		"referenceExternalTransactionId": "bet-123",
		"referenceAttempts":              float64(5),
		"nextAttemptAt":                  "2026-09-30T12:00:00Z",
	} {
		if data[key] != expected {
			t.Fatalf("expected %s=%v, got %v", key, expected, data[key])
		}
	}
}

func TestEventConstructorsDefineAggregateRouting(t *testing.T) {
	walletID := uuid.New()
	transactionID := uuid.New()
	tests := []struct {
		event         Event
		aggregateType string
		aggregateID   uuid.UUID
	}{
		{NewWagerTransactionProcessed(WagerTransactionProcessedData{TransactionID: transactionID, WalletID: walletID}), AggregateTypeWagerTransaction, transactionID},
		{NewWagerTransactionRejected(WagerTransactionRejectedData{TransactionID: transactionID, WalletID: walletID}), AggregateTypeWagerTransaction, transactionID},
		{NewWagerTransactionPendingReference(WagerTransactionPendingReferenceData{TransactionID: transactionID, WalletID: walletID}), AggregateTypeWagerTransaction, transactionID},
		{NewWalletBalanceChanged(WalletBalanceChangedData{TransactionID: transactionID, WalletID: walletID}), AggregateTypeWallet, walletID},
	}
	for _, tt := range tests {
		t.Run(tt.event.EventType, func(t *testing.T) {
			if tt.event.AggregateType != tt.aggregateType || tt.event.AggregateID != tt.aggregateID {
				t.Fatalf("expected aggregate %s/%s, got %s/%s", tt.aggregateType, tt.aggregateID, tt.event.AggregateType, tt.event.AggregateID)
			}
			version, ok := EventVersion(tt.event.EventType)
			if !ok || version != tt.event.Version {
				t.Fatalf("constructor version %d does not match the registered contract %d", tt.event.Version, version)
			}
		})
	}
}

func TestEventVersionRejectsUnknownTypes(t *testing.T) {
	if _, ok := EventVersion("WagerTransactionRequested"); ok {
		t.Fatal("input commands must not be accepted as integration events")
	}
}

func TestWagerTransactionProcessedJSONContract(t *testing.T) {
	external := marshalEventData(t, NewWagerTransactionProcessed(WagerTransactionProcessedData{
		TransactionID:         uuid.New(),
		WalletID:              uuid.New(),
		PlayerID:              uuid.New(),
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Status:                "PROCESSED",
		Money:                 MoneyData{Amount: "25.00", Currency: "BRL"},
		Result:                TransactionResultData{Balance: MoneyData{Amount: "975.00", Currency: "BRL"}, Version: 2},
	}))
	for _, key := range []string{"transactionId", "walletId", "playerId", "providerId", "externalTransactionId", "roundId", "gameId", "kind", "status", "money", "result"} {
		if _, ok := external[key]; !ok {
			t.Fatalf("processed event is missing %s: %v", key, external)
		}
	}
	if money := external["money"].(map[string]any); money["amount"] != "25.00" || money["currency"] != "BRL" {
		t.Fatalf("money must be serialized as decimal strings, got %v", external["money"])
	}

	opening := marshalEventData(t, NewWagerTransactionProcessed(WagerTransactionProcessedData{
		TransactionID: uuid.New(),
		WalletID:      uuid.New(),
		PlayerID:      uuid.New(),
		Kind:          "OPENING",
		Status:        "PROCESSED",
		Money:         MoneyData{Amount: "1000.00", Currency: "BRL"},
	}))
	for _, key := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceExternalTransactionId"} {
		if _, ok := opening[key]; ok {
			t.Fatalf("OPENING event must omit external field %s: %v", key, opening)
		}
	}
}

func TestWalletBalanceChangedJSONContract(t *testing.T) {
	data := marshalEventData(t, NewWalletBalanceChanged(WalletBalanceChangedData{
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     "DEBIT",
		Money:         MoneyData{Amount: "25.00", Currency: "BRL"},
		BalanceBefore: MoneyData{Amount: "1000.00", Currency: "BRL"},
		BalanceAfter:  MoneyData{Amount: "975.00", Currency: "BRL"},
		WalletVersion: 2,
	}))
	for _, key := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[key]; !ok {
			t.Fatalf("WalletBalanceChanged is missing %s: %v", key, data)
		}
	}
	if data["walletVersion"] != float64(2) {
		t.Fatalf("expected walletVersion 2, got %v", data["walletVersion"])
	}
}

func marshalEventData(t *testing.T, event Event) map[string]any {
	t.Helper()
	body, err := json.Marshal(event.Data)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatal(err)
	}
	return data
}
