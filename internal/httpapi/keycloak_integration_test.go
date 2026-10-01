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
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
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

	pool, cleanupPool := integrationHTTPPool(t, ctx)
	t.Cleanup(cleanupPool)
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

	providerWalletCreateBody := fmt.Sprintf(`{"playerId":"%s","initialBalance":{"amount":"1.00","currency":"BRL"}}`, uuid.New())
	providerWalletCreate := doHTTP(t, httpServer.Client(), httpServer.URL+"/wallets", providerAToken, "POST", "", providerWalletCreateBody)
	if providerWalletCreate.StatusCode != http.StatusForbidden {
		t.Fatalf("provider should not create wallets: %d %s", providerWalletCreate.StatusCode, providerWalletCreate.Body)
	}

	providerWalletRead := doHTTP(t, httpServer.Client(), httpServer.URL+"/wallets/"+walletID.String(), providerAToken, "GET", "", "")
	if providerWalletRead.StatusCode != http.StatusForbidden {
		t.Fatalf("provider should not read internal wallet: %d %s", providerWalletRead.StatusCode, providerWalletRead.Body)
	}
	internalWalletRead := doHTTP(t, httpServer.Client(), httpServer.URL+"/wallets/"+walletID.String(), internalToken, "GET", "", "")
	if internalWalletRead.StatusCode != http.StatusOK {
		t.Fatalf("internal service could not read wallet: %d %s", internalWalletRead.StatusCode, internalWalletRead.Body)
	}

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

	ledgerPage1 := doHTTP(t, httpServer.Client(), httpServer.URL+"/wallets/"+walletID.String()+"/ledger?limit=1", internalToken, "GET", "", "")
	if ledgerPage1.StatusCode != http.StatusOK || !strings.Contains(ledgerPage1.Body, `"entries"`) || !strings.Contains(ledgerPage1.Body, `"nextCursor"`) {
		t.Fatalf("unexpected ledger page 1 response: %d %s", ledgerPage1.StatusCode, ledgerPage1.Body)
	}
	var ledgerResult struct {
		Entries    []json.RawMessage `json:"entries"`
		NextCursor string            `json:"nextCursor"`
	}
	decodeHTTP(t, ledgerPage1, &ledgerResult)
	if len(ledgerResult.Entries) != 1 || ledgerResult.NextCursor == "" {
		t.Fatalf("expected one ledger entry and a continuation cursor: %+v", ledgerResult)
	}
	ledgerPage2 := doHTTP(t, httpServer.Client(), httpServer.URL+"/wallets/"+walletID.String()+"/ledger?limit=1&cursor="+url.QueryEscape(ledgerResult.NextCursor), internalToken, "GET", "", "")
	if ledgerPage2.StatusCode != http.StatusOK {
		t.Fatalf("unexpected ledger page 2 response: %d %s", ledgerPage2.StatusCode, ledgerPage2.Body)
	}

	reconciliation := doHTTP(t, httpServer.Client(), httpServer.URL+"/wallets/"+walletID.String()+"/reconciliation", internalToken, "POST", "", "")
	if reconciliation.StatusCode != http.StatusOK || !strings.Contains(reconciliation.Body, `"consistent":true`) {
		t.Fatalf("wallet reconciliation failed: %d %s", reconciliation.StatusCode, reconciliation.Body)
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

	rejectedExternalID := "keycloak-rejected-" + uuid.NewString()
	rejectedBody := fmt.Sprintf(`{"externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"round","gameId":"game","kind":"BET","money":{"amount":"1000.00","currency":"BRL"}}`, rejectedExternalID, playerID, walletID)
	rejected := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions", providerAToken, "POST", "keycloak-rejected-idem-"+uuid.NewString(), rejectedBody)
	if rejected.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(rejected.Body, `"status":"REJECTED"`) {
		t.Fatalf("expected business rejection: %d %s", rejected.StatusCode, rejected.Body)
	}

	missingReferenceExternalID := "keycloak-pending-" + uuid.NewString()
	pendingBody := fmt.Sprintf(`{"externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"round","gameId":"game","kind":"REFUND","money":{"amount":"1.00","currency":"BRL"},"referenceExternalTransactionId":"missing-reference-%s"}`, missingReferenceExternalID, playerID, walletID, uuid.NewString())
	pending := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions", providerAToken, "POST", "keycloak-pending-idem-"+uuid.NewString(), pendingBody)
	if pending.StatusCode != http.StatusAccepted || !strings.Contains(pending.Body, `"status":"PENDING_REFERENCE"`) {
		t.Fatalf("expected pending reference response: %d %s", pending.StatusCode, pending.Body)
	}
	var pendingResponse struct {
		TransactionID uuid.UUID `json:"transactionId"`
	}
	decodeHTTP(t, pending, &pendingResponse)
	if pendingResponse.TransactionID == uuid.Nil {
		t.Fatalf("pending response missing transaction ID: %s", pending.Body)
	}
	pendingQuery := doHTTP(t, httpServer.Client(), httpServer.URL+"/wagering/transactions/"+pendingResponse.TransactionID.String(), providerAToken, "GET", "", "")
	if pendingQuery.StatusCode != http.StatusOK || !strings.Contains(pendingQuery.Body, `"status":"PENDING_REFERENCE"`) {
		t.Fatalf("pending transaction query failed: %d %s", pendingQuery.StatusCode, pendingQuery.Body)
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

func integrationHTTPPool(t *testing.T, ctx context.Context) (*pgxpool.Pool, func()) {
	t.Helper()
	basePool, err := pgxpool.New(ctx, envOrHTTP("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"))
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	if err := basePool.Ping(ctx); err != nil {
		basePool.Close()
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	schema := "http_integration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := basePool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		basePool.Close()
		t.Fatalf("create integration schema: %v", err)
	}
	databaseURL := envOrHTTP("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable")
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
		t.Fatalf("parse PostgreSQL URL: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
		t.Fatalf("create isolated PostgreSQL pool: %v", err)
	}
	if err := migrations.Run(ctx, pool); err != nil {
		pool.Close()
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
		t.Fatalf("run isolated PostgreSQL migrations: %v", err)
	}
	return pool, func() {
		pool.Close()
		if _, err := basePool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("drop integration schema %s: %v", schema, err)
		}
		basePool.Close()
	}
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
	_, _ = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
}

func envOrHTTP(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
