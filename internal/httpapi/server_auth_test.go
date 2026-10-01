package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type authTestWageringRepo struct {
	request ports.ProcessTransactionRequest
}

func (r *authTestWageringRepo) ProcessTransaction(_ context.Context, request ports.ProcessTransactionRequest) (ports.ProcessTransactionResult, error) {
	r.request = request
	balance, _ := money.FromCents(10000, "BRL")
	return ports.ProcessTransactionResult{TransactionID: uuid.New(), Status: "PROCESSED", Balance: balance}, nil
}

func (r *authTestWageringRepo) RetryPendingReference(context.Context, uuid.UUID) (ports.ProcessTransactionResult, error) {
	return ports.ProcessTransactionResult{}, nil
}

func (r *authTestWageringRepo) GetTransaction(context.Context, uuid.UUID) (ports.TransactionView, error) {
	return ports.TransactionView{}, nil
}

func (r *authTestWageringRepo) GetTransactionByExternal(context.Context, string, string) (ports.TransactionView, error) {
	return ports.TransactionView{}, nil
}

type authTestWalletRepo struct{}

func (authTestWalletRepo) Create(context.Context, *wallet.Wallet) error { return nil }
func (authTestWalletRepo) Get(context.Context, uuid.UUID) (*wallet.Wallet, error) {
	return nil, nil
}
func (authTestWalletRepo) GetLedger(context.Context, uuid.UUID, string, int) ([]ports.LedgerEntryView, string, error) {
	return nil, "", nil
}
func (authTestWalletRepo) Reconcile(context.Context, uuid.UUID) (ports.ReconciliationView, error) {
	return ports.ReconciliationView{}, nil
}

func TestWageringUsesAuthenticatedProviderInsteadOfBody(t *testing.T) {
	repo := &authTestWageringRepo{}
	server := &Server{
		wallets:  application.NewWalletService(authTestWalletRepo{}),
		wagering: application.NewWageringService(repo),
	}
	playerID := uuid.New()
	walletID := uuid.New()
	body := `{"providerId":"provider-b","externalTransactionId":"external-1","playerId":"` + playerID.String() + `","walletId":"` + walletID.String() + `","roundId":"round","gameId":"game","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}`
	request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", "idem-1")
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{ProviderID: "provider-a", Subject: "subject-a"}))
	response := httptest.NewRecorder()

	server.wageringHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if repo.request.ProviderID != "provider-a" {
		t.Fatalf("expected authenticated provider provider-a, got %q", repo.request.ProviderID)
	}
}
