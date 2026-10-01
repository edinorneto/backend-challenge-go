package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type contractWalletRepo struct {
	wallet         *wallet.Wallet
	ledger         []ports.LedgerEntryView
	nextCursor     string
	reconciliation ports.ReconciliationView
	ledgerErr      error
	getErr         error
	reconcileErr   error
	gotCursor      string
	gotLimit       int
}

func (r *contractWalletRepo) Create(context.Context, *wallet.Wallet) error { return nil }
func (r *contractWalletRepo) Get(context.Context, uuid.UUID) (*wallet.Wallet, error) {
	return r.wallet, r.getErr
}
func (r *contractWalletRepo) GetLedger(_ context.Context, _ uuid.UUID, cursor string, limit int) ([]ports.LedgerEntryView, string, error) {
	r.gotCursor, r.gotLimit = cursor, limit
	return r.ledger, r.nextCursor, r.ledgerErr
}
func (r *contractWalletRepo) Reconcile(context.Context, uuid.UUID) (ports.ReconciliationView, error) {
	return r.reconciliation, r.reconcileErr
}

func TestGetLedgerHandlerValidatesPaginationAndUsesOpaqueCursor(t *testing.T) {
	entryAmount, _ := money.FromCents(100, "BRL")
	balanceBefore, _ := money.FromCents(1000, "BRL")
	balanceAfter, _ := money.FromCents(900, "BRL")
	repo := &contractWalletRepo{
		ledger: []ports.LedgerEntryView{{
			ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: "DEBIT",
			Amount: entryAmount, BalanceBefore: balanceBefore, BalanceAfter: balanceAfter,
			CreatedAt: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC),
		}},
		nextCursor: "opaque-next-cursor",
	}
	server := &Server{wallets: application.NewWalletService(repo)}
	walletID := uuid.New()

	request := httptest.NewRequest(http.MethodGet, "/wallets/"+walletID.String()+"/ledger?limit=1&cursor=opaque-input", nil)
	request.SetPathValue("walletID", walletID.String())
	response := httptest.NewRecorder()
	server.getLedgerHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if repo.gotCursor != "opaque-input" || repo.gotLimit != 1 {
		t.Fatalf("expected cursor/limit to reach repository, got cursor=%q limit=%d", repo.gotCursor, repo.gotLimit)
	}
	if !strings.Contains(response.Body.String(), `"nextCursor":"opaque-next-cursor"`) {
		t.Fatalf("expected next cursor in response: %s", response.Body.String())
	}

	for _, rawLimit := range []string{"0", "101", "not-a-number"} {
		request = httptest.NewRequest(http.MethodGet, "/wallets/"+walletID.String()+"/ledger?limit="+rawLimit, nil)
		request.SetPathValue("walletID", walletID.String())
		response = httptest.NewRecorder()
		server.getLedgerHandler(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("limit %q: expected 400, got %d", rawLimit, response.Code)
		}
	}

	repo.ledgerErr = database.ErrInvalidLedgerCursor
	request = httptest.NewRequest(http.MethodGet, "/wallets/"+walletID.String()+"/ledger?cursor=broken", nil)
	request.SetPathValue("walletID", walletID.String())
	response = httptest.NewRecorder()
	server.getLedgerHandler(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"error":"invalid_cursor"`) {
		t.Fatalf("expected stable invalid cursor error, got %d: %s", response.Code, response.Body.String())
	}
}

func TestReconciliationHandlerReturnsReadOnlyResult(t *testing.T) {
	stored, _ := money.FromCents(11000, "BRL")
	calculated, _ := money.FromCents(11000, "BRL")
	difference, _ := money.FromCents(0, "BRL")
	walletID := uuid.New()
	repo := &contractWalletRepo{reconciliation: ports.ReconciliationView{
		WalletID: walletID, StoredBalance: stored, CalculatedBalance: calculated, Difference: difference,
		Consistent: true, CheckedEntries: 4,
	}}
	server := &Server{wallets: application.NewWalletService(repo)}
	request := httptest.NewRequest(http.MethodPost, "/wallets/"+walletID.String()+"/reconciliation", nil)
	request.SetPathValue("walletID", walletID.String())
	response := httptest.NewRecorder()

	server.reconciliationHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, expected := range []string{`"storedBalance":{"amount":"110.00","currency":"BRL"}`, `"calculatedBalance":{"amount":"110.00","currency":"BRL"}`, `"difference":{"amount":"0.00","currency":"BRL"}`, `"consistent":true`, `"checkedEntries":4`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s in reconciliation response: %s", expected, body)
		}
	}
}

func TestHTTPHandlersRejectTrailingJSON(t *testing.T) {
	playerID := uuid.New()
	body := `{"playerId":"` + playerID.String() + `","initialBalance":{"amount":"1.00","currency":"BRL"}} {"unexpected":true}`
	server := &Server{
		wallets: application.NewWalletService(&contractWalletRepo{}),
		auth:    nil,
	}
	request := httptest.NewRequest(http.MethodPost, "/wallets", strings.NewReader(body))
	response := httptest.NewRecorder()

	server.walletsHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected trailing JSON to be rejected, got %d: %s", response.Code, response.Body.String())
	}
}

func TestLedgerHandlerMapsRepositoryCursorErrorOnly(t *testing.T) {
	repo := &contractWalletRepo{ledgerErr: errors.New("database connection failed")}
	server := &Server{wallets: application.NewWalletService(repo)}
	walletID := uuid.New()
	request := httptest.NewRequest(http.MethodGet, "/wallets/"+walletID.String()+"/ledger", nil)
	request.SetPathValue("walletID", walletID.String())
	response := httptest.NewRecorder()
	server.getLedgerHandler(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("expected unrelated repository error to remain 500, got %d", response.Code)
	}
}

func TestInternalWalletRoleRemainsRequiredForLedgerAndReconciliation(t *testing.T) {
	server := &Server{
		wallets: application.NewWalletService(&contractWalletRepo{}),
		auth:    auth.NewMiddleware(nil),
	}
	handler := server.Handler()
	walletID := uuid.New()

	for _, path := range []string{
		"/wallets/" + walletID.String() + "/ledger",
		"/wallets/" + walletID.String() + "/reconciliation",
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		if strings.HasSuffix(path, "/reconciliation") {
			request = httptest.NewRequest(http.MethodPost, path, nil)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected auth middleware availability check for %s, got %d", path, response.Code)
		}
	}
}
