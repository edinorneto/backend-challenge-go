package application

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type WalletService struct {
	repo ports.WalletRepository
}

func NewWalletService(repo ports.WalletRepository) *WalletService {
	return &WalletService{
		repo: repo,
	}
}

func (s *WalletService) CreateWallet(
	ctx context.Context,
	playerID uuid.UUID,
	initial money.Money,
) (*wallet.Wallet, error) {
	w, err := wallet.New(
		uuid.New(),
		playerID,
		initial,
		time.Now().UTC(),
	)
	if err != nil {
		return nil, err
	}

	if err := s.repo.Create(ctx, w); err != nil {
		return nil, err
	}

	return w, nil
}

func (s *WalletService) GetWallet(
	ctx context.Context,
	id uuid.UUID,
) (*wallet.Wallet, error) {
	return s.repo.Get(ctx, id)
}
