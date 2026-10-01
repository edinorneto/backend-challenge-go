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
	GetLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]LedgerEntryView, string, error)
	Reconcile(ctx context.Context, walletID uuid.UUID) (ReconciliationView, error)
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

type WageringRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
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
	GetTransaction(ctx context.Context, transactionID uuid.UUID) (TransactionView, error)
	GetTransactionByExternal(ctx context.Context, providerID, externalTransactionID string) (TransactionView, error)
}

type LedgerEntryView struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     string
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

type ReconciliationView struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int
}

type TransactionView struct {
	ID                     uuid.UUID
	ProviderID             string
	ExternalTransactionID  string
	IdempotencyKey         string
	PlayerID               uuid.UUID
	WalletID               uuid.UUID
	RoundID                string
	GameID                 string
	Kind                   string
	Status                 string
	Amount                 money.Money
	FailureCode            string
	ReferenceExternalID    string
	ReferenceTransactionID uuid.UUID
	ResultBalance          money.Money
	ResultWalletVersion    int64
	CreatedAt              time.Time
	UpdatedAt              time.Time
	ProcessedAt            *time.Time
}

type PendingReferenceRepository interface {
	ProcessNextPendingReference(ctx context.Context) (bool, error)
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type TransactionManager interface {
	Begin(ctx context.Context) (context.Context, Transaction, error)
}

type WageringService interface {
	ProcessTransaction(ctx context.Context, idempotencyKey string, req WageringRequest) (ProcessTransactionResult, error)
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

type QueueMessage struct {
	MessageID     string
	ReceiptHandle string
	Body          string
	MessageGroup  string
}

type QueueReceiver interface {
	Receive(ctx context.Context, batchSize int, waitTimeSeconds int, visibilityTimeoutSeconds int) ([]QueueMessage, error)
	Delete(ctx context.Context, receiptHandle string) error
}

type InboxEffect = func(ctx context.Context, envelope []byte) error

type InboxRepository interface {
	Process(ctx context.Context, consumerName string, messageID string, payload []byte, effect InboxEffect) (duplicate bool, err error)
}
