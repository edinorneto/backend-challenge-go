package ports

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
)

type WalletRepository interface {
	Create(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
}

type ProcessTransactionRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         money.Money
	ReferenceExternalTransactionID string
}

type ProcessTransactionResult struct {
	TransactionID    uuid.UUID
	Status           string
	Balance          money.Money
	IdempotentReplay bool
	FailureCode      string
}

type WageringRepository interface {
	ProcessTransaction(
		ctx context.Context,
		req ProcessTransactionRequest,
	) (ProcessTransactionResult, error)
	RetryPendingReference(
		ctx context.Context,
		transactionID uuid.UUID,
	) (ProcessTransactionResult, error)
}

type OutboxEvent struct {
	EventID       uuid.UUID       `json:"eventId"`
	AggregateType string          `json:"aggregateType"`
	AggregateID   uuid.UUID       `json:"aggregateId"`
	EventType     string          `json:"eventType"`
	CorrelationID uuid.UUID       `json:"correlationId"`
	CausationID   *uuid.UUID      `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
	Attempts      int             `json:"attempts"`
	NextAttemptAt time.Time       `json:"nextAttemptAt"`
	LastError     *string         `json:"lastError,omitempty"`
	LockedAt      *time.Time      `json:"lockedAt,omitempty"`
	LockedBy      string          `json:"lockedBy,omitempty"`
	PublishedAt   *time.Time      `json:"publishedAt,omitempty"`
	CreatedAt     time.Time       `json:"createdAt"`
}

type OutboxMessage struct {
	Body                   string
	MessageGroupID         string
	MessageDeduplicationID string
}

type OutboxMessagePublisher interface {
	Publish(ctx context.Context, message OutboxMessage) error
}

type OutboxRepository interface {
	ClaimPending(ctx context.Context, limit int, leaseDuration time.Duration, owner string) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID, owner string) error
	Reschedule(ctx context.Context, eventID uuid.UUID, attempts int, nextAttemptAt time.Time, lastError string, owner string) error
}
