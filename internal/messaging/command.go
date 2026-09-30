package messaging

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

type WagerTransactionRequested struct {
	Type string               `json:"type"`
	Data WagerTransactionData `json:"data"`
}

type WagerTransactionData struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	IdempotencyKey                 string    `json:"idempotencyKey"`
	PlayerID                       uuid.UUID `json:"playerId"`
	WalletID                       uuid.UUID `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          MoneyData `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

type MoneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (c WagerTransactionRequested) Validate() error {
	if c.Type != "WagerTransactionRequested" {
		return errors.New("invalid transaction command type")
	}
	if strings.TrimSpace(c.Data.ProviderID) == "" ||
		strings.TrimSpace(c.Data.ExternalTransactionID) == "" ||
		strings.TrimSpace(c.Data.IdempotencyKey) == "" ||
		c.Data.PlayerID == uuid.Nil || c.Data.WalletID == uuid.Nil ||
		strings.TrimSpace(c.Data.RoundID) == "" || strings.TrimSpace(c.Data.GameID) == "" ||
		strings.TrimSpace(c.Data.Kind) == "" ||
		strings.TrimSpace(c.Data.Money.Amount) == "" ||
		strings.TrimSpace(c.Data.Money.Currency) == "" {
		return errors.New("invalid transaction command data")
	}
	return nil
}
