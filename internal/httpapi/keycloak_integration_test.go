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
	body := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"round","gameId":"game","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}`, externalID, playerID, walletID)
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

type keycloakHarness struct {
	server    *httptest.Server
	pool      *pgxpool.Pool
	providerA string
	providerB string
	internal  string
	walletID  uuid.UUID
	playerID  uuid.UUID
}

func newKeycloakHarness(t *testing.T) keycloakHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cfg := config.Config{
		OIDCIssuerURL:     envOrHTTP("OIDC_ISSUER_URL", "http://localhost:8081/realms/backend"),
		OIDCJWKSURL:       envOrHTTP("OIDC_JWKS_URL", "http://localhost:8081/realms/backend/protocol/openid-connect/certs"),
		OIDCAudience:      envOrHTTP("OIDC_AUDIENCE", "backend-api"),
		OIDCProviderClaim: envOrHTTP("OIDC_PROVIDER_CLAIM", "provider_id"),
	}
	h := keycloakHarness{
		providerA: keycloakToken(t, ctx, "password", "provider-a", "provider-a", "", cfg),
		providerB: keycloakToken(t, ctx, "password", "provider-b", "provider-b", "", cfg),
		internal:  keycloakToken(t, ctx, "client_credentials", "", "", envOrHTTP("BACKEND_INTERNAL_SECRET", "backend-internal-secret"), cfg),
	}
	pool, cleanupPool := integrationHTTPPool(t, ctx)
	t.Cleanup(cleanupPool)
	h.pool = pool
	walletRepo := database.NewWalletRepo(pool)
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(application.NewWalletService(walletRepo), application.NewWageringService(walletRepo), auth.NewMiddleware(verifier), pool, nil)
	h.server = httptest.NewServer(server.Handler())
	t.Cleanup(h.server.Close)
	h.playerID = uuid.New()
	h.walletID = createIntegrationWallet(t, h.server.Client(), h.server.URL, h.internal, h.playerID)
	t.Cleanup(func() { cleanupIntegrationWallet(t, pool, h.walletID) })
	return h
}

func (h keycloakHarness) do(t *testing.T, method, path, token, idempotencyKey, body string) httpResponse {
	t.Helper()
	return doHTTP(t, h.server.Client(), h.server.URL+path, token, method, idempotencyKey, body)
}

func (h keycloakHarness) wager(t *testing.T, token, idempotencyKey, providerField, externalID, kind, amount, reference string) httpResponse {
	t.Helper()
	body := fmt.Sprintf(`{%s"externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"round","gameId":"game","kind":"%s","money":{"amount":"%s","currency":"BRL"}%s}`,
		providerField, externalID, h.playerID, h.walletID, kind, amount, reference)
	return h.do(t, http.MethodPost, "/wagering/transactions", token, idempotencyKey, body)
}

// assertFinancialState checks the stored balance and the ledger directly in PostgreSQL.
func (h keycloakHarness) assertFinancialState(t *testing.T, wantBalance string, wantLedgerEntries int) {
	t.Helper()
	response := h.do(t, http.MethodGet, "/wallets/"+h.walletID.String(), h.internal, "", "")
	var wallet struct {
		Balance struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	decodeHTTP(t, response, &wallet)
	if wallet.Balance.Amount != wantBalance {
		t.Fatalf("expected balance %s, got %s", wantBalance, wallet.Balance.Amount)
	}
	var entries int
	if err := h.pool.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, h.walletID).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != wantLedgerEntries {
		t.Fatalf("expected %d ledger entries, got %d", wantLedgerEntries, entries)
	}
}

func (h keycloakHarness) transactionCount(t *testing.T, providerID, externalID string) int {
	t.Helper()
	var count int
	if err := h.pool.QueryRow(context.Background(), `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND provider_id = $2 AND external_transaction_id = $3`, h.walletID, providerID, externalID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestKeycloakProviderIsolationHasNoCrossProviderEffects(t *testing.T) {
	h := newKeycloakHarness(t)
	h.assertFinancialState(t, "100.00", 1)

	// 1. provider-a cannot act as provider-b through the body.
	spoofID := "spoof-" + uuid.NewString()
	spoof := h.wager(t, h.providerA, "spoof-key-"+uuid.NewString(), `"providerId":"provider-b",`, spoofID, "BET", "10.00", "")
	if spoof.StatusCode != http.StatusForbidden || !strings.Contains(spoof.Body, "provider_mismatch") {
		t.Fatalf("expected 403 provider_mismatch, got %d %s", spoof.StatusCode, spoof.Body)
	}
	if h.transactionCount(t, "provider-a", spoofID)+h.transactionCount(t, "provider-b", spoofID) != 0 {
		t.Fatal("mismatched provider request persisted a transaction")
	}
	h.assertFinancialState(t, "100.00", 1)

	// 2. The internal service is not a provider and cannot submit wagering operations.
	internalOp := h.wager(t, h.internal, "internal-key-"+uuid.NewString(), "", "internal-"+uuid.NewString(), "BET", "10.00", "")
	if internalOp.StatusCode != http.StatusForbidden {
		t.Fatalf("internal service should not submit wagering operations: %d %s", internalOp.StatusCode, internalOp.Body)
	}
	// Providers cannot use wallet operations.
	for _, path := range []string{"/wallets/" + h.walletID.String(), "/wallets/" + h.walletID.String() + "/ledger"} {
		if response := h.do(t, http.MethodGet, path, h.providerA, "", ""); response.StatusCode != http.StatusForbidden {
			t.Fatalf("provider should not access %s: %d", path, response.StatusCode)
		}
	}
	if response := h.do(t, http.MethodPost, "/wallets/"+h.walletID.String()+"/reconciliation", h.providerA, "", ""); response.StatusCode != http.StatusForbidden {
		t.Fatalf("provider should not reconcile wallets: %d", response.StatusCode)
	}
	h.assertFinancialState(t, "100.00", 1)

	// 3. provider-a processes an operation.
	sharedKey := "shared-key-" + uuid.NewString()
	sharedExternalID := "shared-" + uuid.NewString()
	first := h.wager(t, h.providerA, sharedKey, "", sharedExternalID, "BET", "10.00", "")
	if first.StatusCode != http.StatusOK {
		t.Fatalf("provider-a BET failed: %d %s", first.StatusCode, first.Body)
	}
	var aResult struct {
		TransactionID    uuid.UUID `json:"transactionId"`
		IdempotentReplay bool      `json:"idempotentReplay"`
	}
	decodeHTTP(t, first, &aResult)
	h.assertFinancialState(t, "90.00", 2)

	// 4. provider-b reusing provider-a's Idempotency-Key and external ID never sees
	//    provider-a's result: idempotency is scoped by provider.
	crossReplay := h.wager(t, h.providerB, sharedKey, "", sharedExternalID, "BET", "10.00", "")
	var bResult struct {
		TransactionID    uuid.UUID `json:"transactionId"`
		IdempotentReplay bool      `json:"idempotentReplay"`
	}
	decodeHTTP(t, crossReplay, &bResult)
	if bResult.TransactionID == aResult.TransactionID || bResult.IdempotentReplay {
		t.Fatalf("idempotency crossed providers: a=%s b=%+v body=%s", aResult.TransactionID, bResult, crossReplay.Body)
	}
	if strings.Contains(crossReplay.Body, aResult.TransactionID.String()) {
		t.Fatal("provider-b response exposed provider-a transaction ID")
	}
	h.assertFinancialState(t, "80.00", 3)
	// provider-b replay of its own operation returns its own result.
	bReplay := h.wager(t, h.providerB, sharedKey, "", sharedExternalID, "BET", "10.00", "")
	if !strings.Contains(bReplay.Body, `"idempotentReplay":true`) || !strings.Contains(bReplay.Body, bResult.TransactionID.String()) {
		t.Fatalf("provider-b replay did not return its own result: %s", bReplay.Body)
	}
	h.assertFinancialState(t, "80.00", 3)

	// 5. Reads are isolated: by internal ID and by (providerId, externalTransactionId).
	if response := h.do(t, http.MethodGet, "/wagering/transactions/"+aResult.TransactionID.String(), h.providerB, "", ""); response.StatusCode != http.StatusForbidden || strings.Contains(response.Body, sharedExternalID) {
		t.Fatalf("provider-b read provider-a transaction: %d %s", response.StatusCode, response.Body)
	}
	if response := h.do(t, http.MethodGet, "/providers/provider-a/wagering/transactions/"+sharedExternalID, h.providerB, "", ""); response.StatusCode != http.StatusForbidden {
		t.Fatalf("provider-b read provider-a external transaction: %d %s", response.StatusCode, response.Body)
	}
	ownB := h.do(t, http.MethodGet, "/providers/provider-b/wagering/transactions/"+sharedExternalID, h.providerB, "", "")
	if ownB.StatusCode != http.StatusOK || !strings.Contains(ownB.Body, bResult.TransactionID.String()) || strings.Contains(ownB.Body, aResult.TransactionID.String()) {
		t.Fatalf("provider-b external lookup did not resolve to its own transaction: %d %s", ownB.StatusCode, ownB.Body)
	}

	// 6. References never cross providers: provider-b cannot refund provider-a's bet.
	aOnlyBetID := "a-only-bet-" + uuid.NewString()
	if response := h.wager(t, h.providerA, "a-only-key-"+uuid.NewString(), "", aOnlyBetID, "BET", "5.00", ""); response.StatusCode != http.StatusOK {
		t.Fatalf("provider-a BET failed: %d %s", response.StatusCode, response.Body)
	}
	h.assertFinancialState(t, "75.00", 4)
	crossRefund := h.wager(t, h.providerB, "b-refund-key-"+uuid.NewString(), "", "b-refund-"+uuid.NewString(), "REFUND", "5.00",
		fmt.Sprintf(`,"referenceExternalTransactionId":"%s"`, aOnlyBetID))
	if crossRefund.StatusCode != http.StatusAccepted || !strings.Contains(crossRefund.Body, `"status":"PENDING_REFERENCE"`) {
		t.Fatalf("cross-provider reference must stay unresolved: %d %s", crossRefund.StatusCode, crossRefund.Body)
	}
	h.assertFinancialState(t, "75.00", 4)

	// 7. Stored balance still matches the ledger.
	reconciliation := h.do(t, http.MethodPost, "/wallets/"+h.walletID.String()+"/reconciliation", h.internal, "", "")
	if reconciliation.StatusCode != http.StatusOK || !strings.Contains(reconciliation.Body, `"consistent":true`) {
		t.Fatalf("reconciliation failed: %d %s", reconciliation.StatusCode, reconciliation.Body)
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
	defer func() { _ = response.Body.Close() }()
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
	defer func() { _ = response.Body.Close() }()
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
