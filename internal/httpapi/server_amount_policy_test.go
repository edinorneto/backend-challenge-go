package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/auth"
)

func wageringRequestWithAmount(kind, amount string) *http.Request {
	reference := ""
	if kind == "REFUND" || kind == "ROLLBACK" {
		reference = `"referenceExternalTransactionId":"external-0",`
	}
	body := `{"externalTransactionId":"external-1","playerId":"` + uuid.NewString() + `","walletId":"` + uuid.NewString() + `","roundId":"round","gameId":"game","kind":"` + kind + `",` + reference + `"money":{"amount":"` + amount + `","currency":"BRL"}}`
	request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", "idem-1")
	return request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{
		ProviderID: "provider-a",
		Subject:    "subject-provider-a",
		Roles:      map[string]struct{}{auth.RoleWageringProvider: {}},
	}))
}

// An amount that breaks the kind's rule is invalid input: 400 before anything
// reaches the repository, never 500.
func TestWageringRejectsAmountOutsideKindPolicyAs400(t *testing.T) {
	cases := []struct{ kind, amount string }{
		{"BET", "0.00"},
		{"WIN", "0.00"},
		{"REFUND", "0.00"},
		{"ROLLBACK", "0.00"},
		{"LOSS", "1.00"},
	}
	for _, tc := range cases {
		repo := &authTestWageringRepo{}
		server := &Server{
			wallets:  application.NewWalletService(authTestWalletRepo{}),
			wagering: application.NewWageringService(repo),
		}
		response := httptest.NewRecorder()
		server.wageringHandler(response, wageringRequestWithAmount(tc.kind, tc.amount))
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "invalid_request") {
			t.Fatalf("%s %s: expected 400 invalid_request, got %d: %s", tc.kind, tc.amount, response.Code, response.Body.String())
		}
		if repo.request.ProviderID != "" {
			t.Fatalf("%s %s: invalid amount must not reach the repository", tc.kind, tc.amount)
		}
	}

	for _, tc := range []struct{ kind, amount string }{{"BET", "1.00"}, {"LOSS", "0.00"}} {
		repo := &authTestWageringRepo{}
		server := &Server{
			wallets:  application.NewWalletService(authTestWalletRepo{}),
			wagering: application.NewWageringService(repo),
		}
		response := httptest.NewRecorder()
		server.wageringHandler(response, wageringRequestWithAmount(tc.kind, tc.amount))
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s: expected 200, got %d: %s", tc.kind, tc.amount, response.Code, response.Body.String())
		}
	}
}
