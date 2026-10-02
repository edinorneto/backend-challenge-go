package messaging

import (
	"github.com/google/uuid"
)

const (
	EventTypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventTypeWagerTransactionRejected         = "WagerTransactionRejected"
	EventTypeWalletBalanceChanged             = "WalletBalanceChanged"
	EventTypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

const (
	WagerTransactionProcessedVersion        = 1
	WagerTransactionRejectedVersion         = 1
	WalletBalanceChangedVersion             = 1
	WagerTransactionPendingReferenceVersion = 1
)

const (
	AggregateTypeWallet           = "wallet"
	AggregateTypeWagerTransaction = "wager_transaction"
)

var eventVersions = map[string]int{
	EventTypeWagerTransactionProcessed:        WagerTransactionProcessedVersion,
	EventTypeWagerTransactionRejected:         WagerTransactionRejectedVersion,
	EventTypeWalletBalanceChanged:             WalletBalanceChangedVersion,
	EventTypeWagerTransactionPendingReference: WagerTransactionPendingReferenceVersion,
}

// EventVersion returns the contract version published for a known event type.
func EventVersion(eventType string) (int, bool) {
	version, ok := eventVersions[eventType]
	return version, ok
}

// EventData is implemented only by the concrete payloads of this package, so an
// Event can never carry an arbitrary value as its data.
type EventData interface {
	eventType() string
}

// Event is built only by the constructors below, which define its type,
// version and aggregate. The aggregate ID is also the FIFO MessageGroupId.
type Event struct {
	EventType     string
	Version       int
	AggregateType string
	AggregateID   uuid.UUID
	Data          EventData
}

type TransactionResultData struct {
	Balance MoneyData `json:"balance"`
	Version int64     `json:"version"`
}

// External identifiers are omitted for the internal OPENING operation, which has
// no provider, external ID, round or game.
type WagerTransactionProcessedData struct {
	TransactionID                  uuid.UUID             `json:"transactionId"`
	WalletID                       uuid.UUID             `json:"walletId"`
	PlayerID                       uuid.UUID             `json:"playerId"`
	ProviderID                     string                `json:"providerId,omitempty"`
	ExternalTransactionID          string                `json:"externalTransactionId,omitempty"`
	RoundID                        string                `json:"roundId,omitempty"`
	GameID                         string                `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string                `json:"referenceExternalTransactionId,omitempty"`
	Kind                           string                `json:"kind"`
	Status                         string                `json:"status"`
	Money                          MoneyData             `json:"money"`
	Result                         TransactionResultData `json:"result"`
}

type WagerTransactionRejectedData struct {
	TransactionID                  uuid.UUID             `json:"transactionId"`
	WalletID                       uuid.UUID             `json:"walletId"`
	PlayerID                       uuid.UUID             `json:"playerId"`
	ProviderID                     string                `json:"providerId,omitempty"`
	ExternalTransactionID          string                `json:"externalTransactionId,omitempty"`
	RoundID                        string                `json:"roundId,omitempty"`
	GameID                         string                `json:"gameId,omitempty"`
	Kind                           string                `json:"kind"`
	Status                         string                `json:"status"`
	FailureCode                    string                `json:"failureCode"`
	Money                          MoneyData             `json:"money"`
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

func (WagerTransactionProcessedData) eventType() string { return EventTypeWagerTransactionProcessed }
func (WagerTransactionRejectedData) eventType() string  { return EventTypeWagerTransactionRejected }
func (WalletBalanceChangedData) eventType() string      { return EventTypeWalletBalanceChanged }
func (WagerTransactionPendingReferenceData) eventType() string {
	return EventTypeWagerTransactionPendingReference
}

func NewWagerTransactionProcessed(data WagerTransactionProcessedData) Event {
	return newTransactionEvent(data.TransactionID, data)
}

func NewWagerTransactionRejected(data WagerTransactionRejectedData) Event {
	return newTransactionEvent(data.TransactionID, data)
}

func NewWalletBalanceChanged(data WalletBalanceChangedData) Event {
	return newEvent(AggregateTypeWallet, data.WalletID, data)
}

func NewWagerTransactionPendingReference(data WagerTransactionPendingReferenceData) Event {
	return newTransactionEvent(data.TransactionID, data)
}

func newTransactionEvent(transactionID uuid.UUID, data EventData) Event {
	return newEvent(AggregateTypeWagerTransaction, transactionID, data)
}

func newEvent(aggregateType string, aggregateID uuid.UUID, data EventData) Event {
	eventType := data.eventType()
	return Event{
		EventType:     eventType,
		Version:       eventVersions[eventType],
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Data:          data,
	}
}
