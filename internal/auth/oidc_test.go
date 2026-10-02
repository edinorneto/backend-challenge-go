package auth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

type oidcTestServer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
}

func newOIDCTestServer(t *testing.T) oidcTestServer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1})
	body, _ := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": "test-key", "use": "sig", "alg": "RS256", "n": n, "e": e,
	}}})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return oidcTestServer{server: server, key: key}
}

func (s oidcTestServer) verifier(t *testing.T) *Verifier {
	t.Helper()
	verifier, err := NewVerifier(config.Config{
		OIDCIssuerURL:     s.server.URL,
		OIDCJWKSURL:       s.server.URL + "/keys",
		OIDCAudience:      "backend-api",
		OIDCProviderClaim: "provider_id",
	})
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func (s oidcTestServer) token(t *testing.T, provider string, expiry time.Time, issuer string, key *rsa.PrivateKey) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss":          issuer,
		"sub":          provider + "-subject",
		"aud":          "backend-api",
		"exp":          expiry.Unix(),
		"iat":          time.Now().Add(-time.Minute).Unix(),
		"provider_id":  provider,
		"realm_access": map[string]any{"roles": []string{"wallet-internal"}},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestVerifierAcceptsValidToken(t *testing.T) {
	server := newOIDCTestServer(t)
	token := server.token(t, "provider-a", time.Now().Add(time.Hour), server.server.URL, server.key)
	identity, err := server.verifier(t).Verify(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if identity.ProviderID != "provider-a" || !identity.HasRole("wallet-internal") {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestMiddlewareRejectsMissingSubject(t *testing.T) {
	server := newOIDCTestServer(t)
	claims := jwt.MapClaims{
		"iss":         server.server.URL,
		"aud":         "backend-api",
		"exp":         time.Now().Add(time.Hour).Unix(),
		"iat":         time.Now().Add(-time.Minute).Unix(),
		"provider_id": "provider-a",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"
	signed, err := token.SignedString(server.key)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewMiddleware(server.verifier(t)).Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+signed)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", response.Code)
	}
	if strings.TrimSpace(response.Body.String()) != `{"error":"invalid_token"}` {
		t.Fatalf("expected invalid_token response, got %q", response.Body.String())
	}
}

func TestVerifierRejectsExpiredToken(t *testing.T) {
	server := newOIDCTestServer(t)
	token := server.token(t, "provider-a", time.Now().Add(-time.Minute), server.server.URL, server.key)
	if _, err := server.verifier(t).Verify(context.Background(), token); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired token error, got %v", err)
	}
}

func TestVerifierRejectsInvalidSignature(t *testing.T) {
	server := newOIDCTestServer(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	token := server.token(t, "provider-a", time.Now().Add(time.Hour), server.server.URL, otherKey)
	if _, err := server.verifier(t).Verify(context.Background(), token); err == nil {
		t.Fatal("expected invalid signature error")
	}
}

func TestVerifierRejectsInvalidIssuer(t *testing.T) {
	server := newOIDCTestServer(t)
	token := server.token(t, "provider-a", time.Now().Add(time.Hour), "https://invalid-issuer.example", server.key)
	if _, err := server.verifier(t).Verify(context.Background(), token); err == nil {
		t.Fatal("expected invalid issuer error")
	}
}

func TestVerifierRejectsInvalidAudience(t *testing.T) {
	server := newOIDCTestServer(t)
	claims := jwt.MapClaims{"iss": server.server.URL, "sub": "provider-a", "aud": "other-api", "exp": time.Now().Add(time.Hour).Unix(), "provider_id": "provider-a"}
	replacement := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	replacement.Header["kid"] = "test-key"
	token, err := replacement.SignedString(server.key)
	if err != nil {
		t.Fatal(err)
	}
	_ = token
	if _, err := server.verifier(t).Verify(context.Background(), token); err == nil {
		t.Fatal("expected invalid audience error")
	}
}

func TestMiddlewareRejectsMissingToken(t *testing.T) {
	server := newOIDCTestServer(t)
	handler := NewMiddleware(server.verifier(t)).Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", response.Code)
	}
}

func TestMiddlewareRejectsMissingRole(t *testing.T) {
	server := newOIDCTestServer(t)
	handler := NewMiddleware(server.verifier(t)).Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), "some-other-role")
	token := server.token(t, "provider-a", time.Now().Add(time.Hour), server.server.URL, server.key)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", response.Code)
	}
}

func (s oidcTestServer) signClaims(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"
	signed, err := token.SignedString(s.key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestVerifierRejectsMissingProviderClaim(t *testing.T) {
	server := newOIDCTestServer(t)
	token := server.signClaims(t, jwt.MapClaims{
		"iss": server.server.URL,
		"sub": "subject-without-provider",
		"aud": "backend-api",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Add(-time.Minute).Unix(),
	})
	if _, err := server.verifier(t).Verify(context.Background(), token); err == nil {
		t.Fatal("expected missing provider claim error")
	}
}

func TestVerifierRejectsEmptyProviderClaim(t *testing.T) {
	server := newOIDCTestServer(t)
	token := server.signClaims(t, jwt.MapClaims{
		"iss":         server.server.URL,
		"sub":         "subject-empty-provider",
		"aud":         "backend-api",
		"exp":         time.Now().Add(time.Hour).Unix(),
		"iat":         time.Now().Add(-time.Minute).Unix(),
		"provider_id": "  ",
	})
	if _, err := server.verifier(t).Verify(context.Background(), token); err == nil {
		t.Fatal("expected empty provider claim error")
	}
}

func roleToken(t *testing.T, server oidcTestServer, roles ...string) string {
	t.Helper()
	return server.signClaims(t, jwt.MapClaims{
		"iss":          server.server.URL,
		"sub":          "subject",
		"aud":          "backend-api",
		"exp":          time.Now().Add(time.Hour).Unix(),
		"iat":          time.Now().Add(-time.Minute).Unix(),
		"provider_id":  "provider-a",
		"realm_access": map[string]any{"roles": roles},
	})
}

func serveWithToken(handler http.Handler, token string) int {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code
}

func TestMiddlewareRoleAuthorization(t *testing.T) {
	server := newOIDCTestServer(t)
	middleware := NewMiddleware(server.verifier(t))
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, found := IdentityFromContext(r.Context()); !found {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	providerToken := roleToken(t, server, RoleWageringProvider)
	internalToken := roleToken(t, server, RoleWalletInternal)
	noRoleToken := roleToken(t, server)

	cases := []struct {
		name    string
		handler http.Handler
		token   string
		want    int
	}{
		{"provider on provider route", middleware.Require(ok, RoleWageringProvider), providerToken, http.StatusNoContent},
		{"internal on provider route", middleware.Require(ok, RoleWageringProvider), internalToken, http.StatusForbidden},
		{"no role on provider route", middleware.Require(ok, RoleWageringProvider), noRoleToken, http.StatusForbidden},
		{"provider on internal route", middleware.Require(ok, RoleWalletInternal), providerToken, http.StatusForbidden},
		{"internal on internal route", middleware.Require(ok, RoleWalletInternal), internalToken, http.StatusNoContent},
		{"provider on shared route", middleware.RequireAny(ok, RoleWageringProvider, RoleWalletInternal), providerToken, http.StatusNoContent},
		{"internal on shared route", middleware.RequireAny(ok, RoleWageringProvider, RoleWalletInternal), internalToken, http.StatusNoContent},
		{"no role on shared route", middleware.RequireAny(ok, RoleWageringProvider, RoleWalletInternal), noRoleToken, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := serveWithToken(tc.handler, tc.token); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func TestMiddlewareRecordsAuthenticationFailures(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	server := newOIDCTestServer(t)
	metrics := observability.NewMetrics()
	middleware := NewMiddleware(server.verifier(t)).WithObservability(observability.NewLogger(), metrics)
	handler := middleware.Require(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), RoleWageringProvider)

	expired := server.token(t, "provider-a", time.Now().Add(-time.Minute), server.server.URL, server.key)
	internalOnly := server.token(t, "provider-a", time.Now().Add(time.Hour), server.server.URL, server.key)
	for _, tc := range []struct {
		header string
		status int
		reason string
	}{
		{"", http.StatusUnauthorized, "authentication_required"},
		{"Bearer " + expired, http.StatusUnauthorized, "invalid_token"},
		{"Bearer " + internalOnly, http.StatusForbidden, "forbidden"},
	} {
		request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", nil)
		if tc.header != "" {
			request.Header.Set("Authorization", tc.header)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != tc.status || !strings.Contains(recorder.Body.String(), tc.reason) {
			t.Fatalf("expected %d %s, got %d %s", tc.status, tc.reason, recorder.Code, recorder.Body.String())
		}
		if got := metrics.SnapshotLabeled("auth_failures_total", map[string]string{"reason": tc.reason}); got != 1 {
			t.Fatalf("expected one %s failure, got %d", tc.reason, got)
		}
	}
	logs := output.String()
	if strings.Count(logs, `"message":"authentication_failed"`) != 3 || !strings.Contains(logs, `"errorClass"`) {
		t.Fatalf("expected one structured log per rejected request: %s", logs)
	}
	if strings.Contains(logs, expired) || strings.Contains(logs, internalOnly) || strings.Contains(logs, "expired") {
		t.Fatalf("authentication logs must not contain tokens or verifier messages: %s", logs)
	}
}
