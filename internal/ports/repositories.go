package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
)

type WalletRepository interface {
	Create(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
}
