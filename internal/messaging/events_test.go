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
