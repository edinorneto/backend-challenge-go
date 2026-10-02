package httpapi

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

// The http_request log is written by the outermost middleware, but the provider
// is only known after authentication, further in. The line must still carry it.
func TestHTTPRequestLogIncludesAuthenticatedProvider(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	// Same step as the authentication middleware: the identity is attached to a
	// derived request context that the outer middleware never sees directly.
	authenticated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.WithContext(auth.WithIdentity(r.Context(), auth.Identity{ProviderID: "provider-a", Subject: "subject-a"}))
		w.WriteHeader(http.StatusNoContent)
	})
	handler := loggingMiddleware(authenticated, observability.NewLogger(), nil)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/providers/provider-a/transactions/x", nil))

	anonymous := loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}), observability.NewLogger(), nil)
	anonymous.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/wallets/x", nil))

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two http_request lines, got %q", output.String())
	}
	if !strings.Contains(lines[0], `"message":"http_request"`) || !strings.Contains(lines[0], `"providerId":"provider-a"`) {
		t.Fatalf("expected the authenticated provider in the request log: %s", lines[0])
	}
	if strings.Contains(lines[1], `"providerId"`) {
		t.Fatalf("an unauthenticated request must not log a provider: %s", lines[1])
	}
}
