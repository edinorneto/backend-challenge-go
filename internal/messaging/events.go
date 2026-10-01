package messaging

import (
	"github.com/google/uuid"
)

const (
	WagerTransactionProcessedVersion        = 1
	WagerTransactionRejectedVersion         = 1
	WalletBalanceChangedVersion             = 1
	WagerTransactionPendingReferenceVersion = 1
)

type Event struct {
	EventType string
	Version   int
	Data      any
}

type TransactionResultData struct {
	Balance MoneyData `json:"balance"`
	Version int64     `json:"version"`
}

type WagerTransactionProcessedData struct {
	TransactionID uuid.UUID             `json:"transactionId"`
	WalletID      uuid.UUID             `json:"walletId"`
	PlayerID      uuid.UUID             `json:"playerId"`
	Kind          string                `json:"kind"`
	Status        string                `json:"status"`
	Amount        MoneyData             `json:"amount"`
	Result        TransactionResultData `json:"result"`
}

type WagerTransactionRejectedData struct {
	TransactionID                  uuid.UUID             `json:"transactionId"`
	WalletID                       uuid.UUID             `json:"walletId"`
	PlayerID                       uuid.UUID             `json:"playerId"`
	ProviderID                     string                `json:"providerId,omitempty"`
	Kind                           string                `json:"kind"`
	Status                         string                `json:"status"`
	FailureCode                    string                `json:"failureCode"`
	Amount                         MoneyData             `json:"amount"`
	ReferenceExternalTransactionID string                `json:"referenceExternalTransactionId,omitempty"`
	ReferenceAttempts              int                   `json:"referenceAttempts,omitempty"`
	NextAttemptAt                  string                `json:"nextAttemptAt,omitempty"`
	Result                         TransactionResultData `json:"result"`
}

type WalletBalanceChangedData struct {
	WalletID        uuid.UUID `json:"walletId"`
	TransactionID   uuid.UUID `json:"transactionId"`
	Direction       string    `json:"direction"`
	Money           MoneyData `json:"money"`
	BalanceBefore   MoneyData `json:"balanceBefore"`
	BalanceAfter    MoneyData `json:"balanceAfter"`
	WalletVersion   int64     `json:"walletVersion"`
	PreviousVersion int64     `json:"previousVersion"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	WalletID                       uuid.UUID `json:"walletId"`
	ProviderID                     string    `json:"providerId"`
	Kind                           string    `json:"kind"`
	Status                         string    `json:"status"`
	FailureCode                    string    `json:"failureCode"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	ReferenceAttempts              int       `json:"referenceAttempts"`
	NextAttemptAt                  string    `json:"nextAttemptAt"`
}

func NewWagerTransactionProcessed(data WagerTransactionProcessedData) Event {
	return Event{EventType: "WagerTransactionProcessed", Version: WagerTransactionProcessedVersion, Data: data}
}

func NewWagerTransactionRejected(data WagerTransactionRejectedData) Event {
	return Event{EventType: "WagerTransactionRejected", Version: WagerTransactionRejectedVersion, Data: data}
}

func NewWalletBalanceChanged(data WalletBalanceChangedData) Event {
	return Event{EventType: "WalletBalanceChanged", Version: WalletBalanceChangedVersion, Data: data}
}

func NewWagerTransactionPendingReference(data WagerTransactionPendingReferenceData) Event {
	return Event{EventType: "WagerTransactionPendingReference", Version: WagerTransactionPendingReferenceVersion, Data: data}
}
