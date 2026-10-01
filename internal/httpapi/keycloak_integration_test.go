//go:build integration

package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
)

func TestKeycloakProviderIsolationAndInternalHTTPAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := config.Config{
		OIDCIssuerURL:     envOrHTTP("OIDC_ISSUER_URL", "http://localhost:8081/realms/backend"),
		OIDCJWKSURL:       envOrHTTP("OIDC_JWKS_URL", "http://localhost:8081/realms/backend/protocol/openid-connect/certs"),
		OIDCAudience:      envOrHTTP("OIDC_AUDIENCE", "backend-api"),
		OIDCProviderClaim: envOrHTTP("OIDC_PROVIDER_CLAIM", "provider_id"),
	}
	providerAToken := keycloakToken(t, ctx, "password", "provider-a", "provider-a", "", cfg)
	providerBToken := keycloakToken(t, ctx, "password", "provider-b", "provider-b", "", cfg)
	internalToken := keycloakToken(t, ctx, "client_credentials", "", "", envOrHTTP("BACKEND_INTERNAL_SECRET", "backend-internal-secret"), cfg)

	pool := integrationHTTPPool(t, ctx)
	walletRepo := database.NewWalletRepo(pool)
	wagering := application.NewWageringService(walletRepo)
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(
		application.NewWalletService(walletRepo),
		wagering,
		auth.NewMiddleware(verifier),
		pool,
		nil,
	)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	playerID := uuid.New()
	walletID := createIntegrationWallet(t, httpServer.Client(), httpServer.URL, internalToken, playerID)
	defer cleanupIntegrationWallet(t, pool, walletID)

	externalID := "keycloak-http-" + uuid.NewString()
	idempotencyKey := "keycloak-idem-" + uuid.NewString()
	body := fmt.Sprintf(`{"providerId":"provider-b","externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"round","gameId":"game","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}`, externalID, playerID, walletID)
	response := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions", providerAToken, "POST", idempotencyKey, body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("provider-a operation failed: %d %s", response.StatusCode, response.Body)
	}
	var operation struct {
		TransactionID uuid.UUID `json:"transactionId"`
		Status        string    `json:"status"`
	}
	decodeHTTP(t, response, &operation)
	if operation.TransactionID == uuid.Nil || operation.Status != "PROCESSED" {
		t.Fatalf("unexpected operation response: %+v", operation)
	}

	own := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions/"+operation.TransactionID.String(), providerAToken, "GET", "", "")
	if own.StatusCode != http.StatusOK {
		t.Fatalf("provider-a could not read its transaction: %d %s", own.StatusCode, own.Body)
	}
	foreign := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions/"+operation.TransactionID.String(), providerBToken, "GET", "", "")
	if foreign.StatusCode != http.StatusForbidden {
		t.Fatalf("provider-b should be denied transaction access: %d %s", foreign.StatusCode, foreign.Body)
	}

	ownExternal := doHTTP(t, httpServer.Client(), httpServer.URL+"/providers/provider-a/wagering/transactions/"+externalID, providerAToken, "GET", "", "")
	if ownExternal.StatusCode != http.StatusOK {
		t.Fatalf("provider-a could not read external transaction: %d %s", ownExternal.StatusCode, ownExternal.Body)
	}
	mismatch := doHTTP(t, httpServer.Client(), httpServer.URL+"/providers/provider-b/wagering/transactions/"+externalID, providerAToken, "GET", "", "")
	if mismatch.StatusCode != http.StatusForbidden {
		t.Fatalf("provider URL mismatch should be denied: %d %s", mismatch.StatusCode, mismatch.Body)
	}

	replay := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions", providerAToken, "POST", idempotencyKey, body)
	if replay.StatusCode != http.StatusOK || !strings.Contains(replay.Body, `"idempotentReplay":true`) {
		t.Fatalf("expected HTTP idempotent replay: %d %s", replay.StatusCode, replay.Body)
	}
	conflictBody := strings.Replace(body, `"amount":"1.00"`, `"amount":"2.00"`, 1)
	conflict := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions", providerAToken, "POST", idempotencyKey, conflictBody)
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("expected idempotency conflict: %d %s", conflict.StatusCode, conflict.Body)
	}

	unauthenticated := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions/"+operation.TransactionID.String(), "", "GET", "", "")
	if unauthenticated.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthenticated request to fail: %d %s", unauthenticated.StatusCode, unauthenticated.Body)
	}
	internalRead := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions/"+operation.TransactionID.String(), internalToken, "GET", "", "")
	if internalRead.StatusCode != http.StatusOK {
		t.Fatalf("expected internal token to read transaction: %d %s", internalRead.StatusCode, internalRead.Body)
	}
}

func keycloakToken(t *testing.T, ctx context.Context, grantType, username, password, secret string, cfg config.Config) string {
	t.Helper()
	values := url.Values{"grant_type": {grantType}, "client_id": {"backend-api"}}
	if grantType == "password" {
		values.Set("username", username)
		values.Set("password", password)
	} else {
		values.Set("client_id", "backend-internal")
		values.Set("client_secret", secret)
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
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Skipf("Keycloak token endpoint unavailable or realm not imported: status=%d", response.StatusCode)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&token); err != nil || token.AccessToken == "" {
		t.Fatalf("Keycloak returned no access token")
	}
	return token.AccessToken
}

func integrationHTTPPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := envOrHTTP("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createIntegrationWallet(t *testing.T, client *http.Client, baseURL, token string, playerID uuid.UUID) uuid.UUID {
	t.Helper()
	body := fmt.Sprintf(`{"playerId":"%s","initialBalance":{"amount":"100.00","currency":"BRL"}}`, playerID)
	response := doHTTP(t, client, baseURL+"/wallets", token, "POST", "", body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create wallet failed: %d %s", response.StatusCode, response.Body)
	}
	var result struct {
		ID uuid.UUID `json:"id"`
	}
	decodeHTTP(t, response, &result)
	if result.ID == uuid.Nil {
		t.Fatal("wallet response did not contain an ID")
	}
	return result.ID
}

type httpResponse struct {
	StatusCode int
	Body       string
}

func doHTTP(t *testing.T, client *http.Client, endpoint, token, method, idempotencyKey, body string) httpResponse {
	t.Helper()
	request, err := http.NewRequest(method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	content, _ := io.ReadAll(response.Body)
	return httpResponse{StatusCode: response.StatusCode, Body: string(content)}
}

func decodeHTTP(t *testing.T, response httpResponse, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(response.Body), target); err != nil {
		t.Fatalf("decode HTTP response: %v: %s", err, response.Body)
	}
}

func cleanupIntegrationWallet(t *testing.T, pool *pgxpool.Pool, walletID uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = pool.Exec(ctx, `
		DELETE FROM outbox_events
		WHERE aggregate_id = $1
		   OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)
		   OR correlation_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)
	`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
}

func envOrHTTP(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
