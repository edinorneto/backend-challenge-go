package ports

import (
	"context"

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
}
