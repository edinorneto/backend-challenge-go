//go:build integration

package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/config"
)

// A real Keycloak token is accepted while valid and refused with 401 once it
// expires. The backend-api-short-lived test client issues 2 s access tokens.
func TestKeycloakExpiredTokenIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := config.Config{
		OIDCIssuerURL:     envOrHTTP("OIDC_ISSUER_URL", "http://localhost:8081/realms/backend"),
		OIDCJWKSURL:       envOrHTTP("OIDC_JWKS_URL", "http://localhost:8081/realms/backend/protocol/openid-connect/certs"),
		OIDCAudience:      envOrHTTP("OIDC_AUDIENCE", "backend-api"),
		OIDCProviderClaim: envOrHTTP("OIDC_PROVIDER_CLAIM", "provider_id"),
	}

	values := url.Values{
		"grant_type": {"password"},
		"client_id":  {"backend-api-short-lived"},
		"username":   {"provider-a"},
		"password":   {"provider-a"},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.OIDCIssuerURL, "/")+"/protocol/openid-connect/token", strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Skipf("Keycloak unavailable: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("short-lived test client did not issue a token (status %d): re-create the keycloak container to import the realm", response.StatusCode)
	}
	var token struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(response.Body).Decode(&token); err != nil || token.AccessToken == "" {
		t.Fatalf("Keycloak returned no access token: %v", err)
	}
	if token.ExpiresIn <= 0 || token.ExpiresIn > 5 {
		t.Fatalf("expected a short-lived token, got expires_in=%d", token.ExpiresIn)
	}
	issued := time.Now()

	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	handler := auth.NewMiddleware(verifier).Require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), auth.RoleWageringProvider)
	call := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/providers/provider-a/transactions/x", nil)
		request.Header.Set("Authorization", "Bearer "+token.AccessToken)
		handler.ServeHTTP(recorder, request)
		return recorder
	}

	if fresh := call(); fresh.Code != http.StatusNoContent {
		t.Fatalf("expected the fresh token to be accepted, got %d %s", fresh.Code, fresh.Body.String())
	}
	time.Sleep(time.Until(issued.Add(time.Duration(token.ExpiresIn)*time.Second + 2*time.Second)))
	expired := call()
	if expired.Code != http.StatusUnauthorized || !strings.Contains(expired.Body.String(), "invalid_token") {
		t.Fatalf("expected 401 invalid_token for the expired token, got %d %s", expired.Code, expired.Body.String())
	}
}
