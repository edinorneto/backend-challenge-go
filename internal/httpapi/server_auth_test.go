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
	request     ports.ProcessTransactionRequest
	transaction ports.TransactionView
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
	return r.transaction, nil
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

func TestGetTransactionRejectsMissingIdentityBeforeLookup(t *testing.T) {
	repo := &authTestWageringRepo{transaction: ports.TransactionView{ID: uuid.New(), ProviderID: "provider-a", Status: "PROCESSED"}}
	server := &Server{wagering: application.NewWageringService(repo)}
	request := httptest.NewRequest(http.MethodGet, "/wagering/transactions/"+repo.transaction.ID.String(), nil)
	request.SetPathValue("transactionID", repo.transaction.ID.String())
	response := httptest.NewRecorder()
	server.getTransactionHandler(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without identity, got %d: %s", response.Code, response.Body.String())
	}
}

func TestGetTransactionRequiresMatchingProviderOrInternalRole(t *testing.T) {
	transactionID := uuid.New()
	repo := &authTestWageringRepo{transaction: ports.TransactionView{
		ID: transactionID, ProviderID: "provider-a", Status: "PROCESSED",
	}}
	server := &Server{wagering: application.NewWageringService(repo)}

	request := httptest.NewRequest(http.MethodGet, "/wagering/transactions/"+transactionID.String(), nil)
	request.SetPathValue("transactionID", transactionID.String())
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{ProviderID: "provider-b"}))
	response := httptest.NewRecorder()
	server.getTransactionHandler(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected provider mismatch to be forbidden, got %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/wagering/transactions/"+transactionID.String(), nil)
	request.SetPathValue("transactionID", transactionID.String())
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{ProviderID: "provider-a"}))
	response = httptest.NewRecorder()
	server.getTransactionHandler(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected matching provider to be allowed, got %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/wagering/transactions/"+transactionID.String(), nil)
	request.SetPathValue("transactionID", transactionID.String())
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{
		ProviderID: "internal-service",
		Roles:      map[string]struct{}{"wallet-internal": {}},
	}))
	response = httptest.NewRecorder()
	server.getTransactionHandler(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected internal role to be allowed, got %d", response.Code)
	}
}
