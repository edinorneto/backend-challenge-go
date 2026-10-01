package wagertransaction

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

type Source string

const (
	SourceInternal Source = "INTERNAL"
	SourceExternal Source = "EXTERNAL"
)

var (
	ErrInvalidTransaction       = errors.New("invalid wager transaction")
	ErrInvalidTransactionKind   = errors.New("invalid wager transaction kind")
	ErrInvalidTransactionStatus = errors.New("invalid wager transaction status")
	ErrInvalidReference         = errors.New("invalid transaction reference")
	ErrInvalidTransition        = errors.New("invalid transaction state transition")
	ErrTerminalTransaction      = errors.New("transaction is terminal")
)

type WagerTransaction struct {
	id         uuid.UUID
	source     Source
	providerID string

	externalTransactionID string
	idempotencyKey        string
	payloadHash           string

	playerID uuid.UUID
	walletID uuid.UUID

	roundID string
	gameID  string

	kind   Kind
	status Status

	money money.Money

	referenceExternalTransactionID string
	referenceTransactionID         uuid.UUID

	failureCode string

	resultBalance       money.Money
	resultWalletVersion int64

	createdAt   time.Time
	updatedAt   time.Time
	processedAt *time.Time
}

type Rehydration struct {
	ID                             uuid.UUID
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Amount                         money.Money
	Status                         Status
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	FailureCode                    string
	ResultBalance                  money.Money
	ResultWalletVersion            int64
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	ProcessedAt                    *time.Time
}

func Rehydrate(data Rehydration) (*WagerTransaction, error) {
	transaction, err := NewExternal(
		data.ID, data.ProviderID, data.ExternalTransactionID, data.IdempotencyKey,
		data.PayloadHash, data.PlayerID, data.WalletID, data.RoundID, data.GameID,
		data.Kind, data.Amount, data.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	if data.UpdatedAt.IsZero() {
		return nil, ErrInvalidTransaction
	}

	switch data.Status {
	case StatusPending:
	case StatusPendingReference:
		if err := transaction.MarkPendingReference(data.UpdatedAt); err != nil {
			return nil, err
		}
	case StatusProcessed:
		if err := transaction.MarkProcessed(data.ResultBalance, data.ResultWalletVersion, data.UpdatedAt); err != nil {
			return nil, err
		}
	case StatusRejected:
		if err := transaction.Reject(data.FailureCode, data.UpdatedAt); err != nil {
			return nil, err
		}
	case StatusFailed:
		if err := transaction.Fail(data.FailureCode, data.UpdatedAt); err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidTransactionStatus
	}

	transaction.referenceExternalTransactionID = data.ReferenceExternalTransactionID
	transaction.referenceTransactionID = data.ReferenceTransactionID
	transaction.createdAt = data.CreatedAt
	transaction.updatedAt = data.UpdatedAt
	if data.Status == StatusProcessed {
		if data.ProcessedAt == nil || data.ProcessedAt.IsZero() {
			return nil, ErrInvalidTransaction
		}
		processedAt := *data.ProcessedAt
		transaction.processedAt = &processedAt
	} else if data.ProcessedAt != nil {
		return nil, ErrInvalidTransaction
	}
	return transaction, nil
}

func NewExternal(
	id uuid.UUID,
	providerID string,
	externalTransactionID string,
	idempotencyKey string,
	payloadHash string,
	playerID uuid.UUID,
	walletID uuid.UUID,
	roundID string,
	gameID string,
	kind Kind,
	amount money.Money,
	now time.Time,
) (*WagerTransaction, error) {
	if id == uuid.Nil ||
		playerID == uuid.Nil ||
		walletID == uuid.Nil ||
		now.IsZero() {
		return nil, ErrInvalidTransaction
	}

	if providerID == "" ||
		externalTransactionID == "" ||
		idempotencyKey == "" ||
		payloadHash == "" {
		return nil, ErrInvalidTransaction
	}

	if roundID == "" || gameID == "" {
		return nil, ErrInvalidTransaction
	}

	if kind == KindOpening || !isExternalKind(kind) {
		return nil, ErrInvalidTransactionKind
	}

	if err := validateAmount(kind, amount); err != nil {
		return nil, err
	}

	return &WagerTransaction{
		id:                    id,
		source:                SourceExternal,
		providerID:            providerID,
		externalTransactionID: externalTransactionID,
		idempotencyKey:        idempotencyKey,
		payloadHash:           payloadHash,
		playerID:              playerID,
		walletID:              walletID,
		roundID:               roundID,
		gameID:                gameID,
		kind:                  kind,
		status:                StatusPending,
		money:                 amount,
		createdAt:             now,
		updatedAt:             now,
	}, nil
}

func NewOpening(
	id uuid.UUID,
	playerID uuid.UUID,
	walletID uuid.UUID,
	amount money.Money,
	now time.Time,
) (*WagerTransaction, error) {
	if id == uuid.Nil ||
		playerID == uuid.Nil ||
		walletID == uuid.Nil ||
		now.IsZero() {
		return nil, ErrInvalidTransaction
	}

	if amount.IsNegative() {
		return nil, ErrInvalidTransaction
	}

	return &WagerTransaction{
		id:        id,
		source:    SourceInternal,
		playerID:  playerID,
		walletID:  walletID,
		kind:      KindOpening,
		status:    StatusPending,
		money:     amount,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func (t *WagerTransaction) SetReference(
	externalTransactionID string,
) error {
	if t.isTerminal() {
		return ErrTerminalTransaction
	}

	if t.kind != KindRefund && t.kind != KindRollback {
		return ErrInvalidReference
	}

	if externalTransactionID == "" {
		return ErrInvalidReference
	}

	t.referenceExternalTransactionID = externalTransactionID

	return nil
}

func (t *WagerTransaction) AssociateReference(transactionID uuid.UUID) error {
	if t.isTerminal() {
		return ErrTerminalTransaction
	}
	if t.kind != KindRefund && t.kind != KindRollback {
		return ErrInvalidReference
	}
	if transactionID == uuid.Nil {
		return ErrInvalidReference
	}

	t.referenceTransactionID = transactionID
	return nil
}

func (t *WagerTransaction) MarkPendingReference(now time.Time) error {
	if err := t.canTransitionTo(StatusPendingReference); err != nil {
		return err
	}

	t.status = StatusPendingReference
	t.updatedAt = now

	return nil
}

func (t *WagerTransaction) MarkProcessed(
	resultBalance money.Money,
	resultWalletVersion int64,
	now time.Time,
) error {
	if err := t.canTransitionTo(StatusProcessed); err != nil {
		return err
	}

	if resultBalance.Currency() != t.money.Currency() {
		return money.ErrCurrencyMismatch
	}

	if resultWalletVersion < 1 {
		return ErrInvalidTransaction
	}

	t.status = StatusProcessed
	t.resultBalance = resultBalance
	t.resultWalletVersion = resultWalletVersion
	t.updatedAt = now

	processedAt := now
	t.processedAt = &processedAt

	return nil
}

func (t *WagerTransaction) Reject(
	failureCode string,
	now time.Time,
) error {
	if err := t.canTransitionTo(StatusRejected); err != nil {
		return err
	}

	if failureCode == "" {
		return ErrInvalidTransaction
	}

	t.status = StatusRejected
	t.failureCode = failureCode
	t.updatedAt = now

	return nil
}

func (t *WagerTransaction) Fail(
	failureCode string,
	now time.Time,
) error {
	if err := t.canTransitionTo(StatusFailed); err != nil {
		return err
	}

	if failureCode == "" {
		return ErrInvalidTransaction
	}

	t.status = StatusFailed
	t.failureCode = failureCode
	t.updatedAt = now

	return nil
}

func (t *WagerTransaction) IsTerminal() bool {
	return t.isTerminal()
}

func (t *WagerTransaction) ID() uuid.UUID {
	return t.id
}

func (t *WagerTransaction) Source() Source {
	return t.source
}

func (t *WagerTransaction) ProviderID() string {
	return t.providerID
}

func (t *WagerTransaction) ExternalTransactionID() string {
	return t.externalTransactionID
}

func (t *WagerTransaction) IdempotencyKey() string {
	return t.idempotencyKey
}

func (t *WagerTransaction) PayloadHash() string {
	return t.payloadHash
}

func (t *WagerTransaction) PlayerID() uuid.UUID {
	return t.playerID
}

func (t *WagerTransaction) WalletID() uuid.UUID {
	return t.walletID
}

func (t *WagerTransaction) RoundID() string {
	return t.roundID
}

func (t *WagerTransaction) GameID() string {
	return t.gameID
}

func (t *WagerTransaction) Kind() Kind {
	return t.kind
}

func (t *WagerTransaction) Status() Status {
	return t.status
}

func (t *WagerTransaction) Money() money.Money {
	return t.money
}

func (t *WagerTransaction) ReferenceExternalTransactionID() string {
	return t.referenceExternalTransactionID
}

func (t *WagerTransaction) ReferenceTransactionID() uuid.UUID {
	return t.referenceTransactionID
}

func (t *WagerTransaction) FailureCode() string {
	return t.failureCode
}

func (t *WagerTransaction) ResultBalance() money.Money {
	return t.resultBalance
}

func (t *WagerTransaction) ResultWalletVersion() int64 {
	return t.resultWalletVersion
}

func (t *WagerTransaction) CreatedAt() time.Time {
	return t.createdAt
}

func (t *WagerTransaction) UpdatedAt() time.Time {
	return t.updatedAt
}

func (t *WagerTransaction) ProcessedAt() *time.Time {
	return t.processedAt
}

func (t *WagerTransaction) canTransitionTo(target Status) error {
	if t.isTerminal() {
		return ErrTerminalTransaction
	}

	switch t.status {
	case StatusPending:
		switch target {
		case StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
			return nil
		}

	case StatusPendingReference:
		switch target {
		case StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
			return nil
		}
	}

	return ErrInvalidTransition
}

func (t *WagerTransaction) isTerminal() bool {
	return t.status == StatusProcessed ||
		t.status == StatusRejected ||
		t.status == StatusFailed
}

func isExternalKind(kind Kind) bool {
	switch kind {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	default:
		return false
	}
}

func validateAmount(
	kind Kind,
	amount money.Money,
) error {
	if amount.IsNegative() {
		return ErrInvalidTransaction
	}

	switch kind {
	case KindBet, KindWin, KindRefund, KindRollback:
		if amount.IsZero() {
			return ErrInvalidTransaction
		}

	case KindLoss:
		if !amount.IsZero() {
			return ErrInvalidTransaction
		}

	default:
		return ErrInvalidTransactionKind
	}

	return nil
}
