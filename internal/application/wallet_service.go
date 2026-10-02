package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type WalletService struct {
	repo    ports.WalletRepository
	logger  *observability.Logger
	metrics *observability.Metrics
}

func NewWalletService(repo ports.WalletRepository, options ...any) *WalletService {
	service := &WalletService{repo: repo}
	for _, option := range options {
		switch value := option.(type) {
		case *observability.Metrics:
			service.metrics = value
		case *observability.Logger:
			service.logger = value
		}
	}
	return service
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

func (s *WalletService) GetLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]ports.LedgerEntryView, string, error) {
	return s.repo.GetLedger(ctx, walletID, cursor, limit)
}

func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (ports.ReconciliationView, error) {
	if s.metrics != nil {
		s.metrics.Inc("reconciliation_total")
	}
	result, err := s.repo.Reconcile(ctx, walletID)
	if err != nil || result.Consistent {
		return result, err
	}
	// A divergence is reported in the response, in this metric and in the log;
	// reconciliation never repairs the wallet.
	if s.metrics != nil {
		s.metrics.Inc("reconciliation_divergences_total")
	}
	if s.logger != nil {
		s.logger.Error(ctx, "reconciliation_divergence", errReconciliationDivergence, map[string]string{
			"walletId":       walletID.String(),
			"difference":     result.Difference.String(),
			"currency":       result.Difference.Currency(),
			"checkedEntries": fmt.Sprint(result.CheckedEntries),
		})
	}
	return result, nil
}

var errReconciliationDivergence = errors.New("stored balance differs from the ledger")
