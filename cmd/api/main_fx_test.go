package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/fx"

	"github.com/edinorneto/backend-challenge-go/internal/httpapi"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

func TestApplicationFxComposition(t *testing.T) {
	if err := fx.ValidateApp(AppOptions()); err != nil {
		t.Fatalf("Fx application graph is invalid: %v", err)
	}
}

// The graph is built without starting it (no hook runs, so nothing connects):
// the server's /metrics must expose the Metrics instance that services and
// workers increment, not a private one.
func TestHTTPServerExposesSharedMetrics(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:1/betting?sslmode=disable")
	t.Setenv("OIDC_ISSUER_URL", "http://127.0.0.1:1/realms/backend")
	t.Setenv("OIDC_JWKS_URL", "http://127.0.0.1:1/realms/backend/protocol/openid-connect/certs")

	var server *httpapi.Server
	var metrics *observability.Metrics
	app := fx.New(AppOptions(), fx.Populate(&server, &metrics), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatalf("build application graph: %v", err)
	}

	metrics.Inc("reconciliation_divergences_total")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(response.Body.String(), "reconciliation_divergences_total 1") {
		t.Fatalf("/metrics does not expose the shared metrics:\n%s", response.Body.String())
	}
}
