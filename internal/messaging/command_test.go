package messaging

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWagerTransactionRequestedRequiresEnvelopeMetadata(t *testing.T) {
	command := WagerTransactionRequested{
		MessageID:  "message-1",
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC(),
		Data: WagerTransactionData{
			ProviderID:            "provider",
			ExternalTransactionID: "external",
			IdempotencyKey:        "idempotency",
			PlayerID:              uuid.New(),
			WalletID:              uuid.New(),
			RoundID:               "round",
			GameID:                "game",
			Kind:                  "BET",
			Money:                 MoneyData{Amount: "1.00", Currency: "BRL"},
		},
	}
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}

	for name, invalid := range map[string]WagerTransactionRequested{
		"missing message id":  {Type: command.Type, OccurredAt: command.OccurredAt, Data: command.Data},
		"missing occurred at": {MessageID: command.MessageID, Type: command.Type, Data: command.Data},
		"wrong type":          {MessageID: command.MessageID, Type: "Other", OccurredAt: command.OccurredAt, Data: command.Data},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invalid.Validate(); err == nil {
				t.Fatal("expected invalid command metadata")
			}
		})
	}
}
