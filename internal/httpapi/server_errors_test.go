package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestWriteServerErrorSeparatesTransientFailures(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		status     int
		body       string
		retryAfter string
	}{
		{"database connection lost", fmt.Errorf("lock wallet: %w", &pgconn.PgError{Code: "08006"}), http.StatusServiceUnavailable, `"service_unavailable"`, "1"},
		{"deadlock", fmt.Errorf("commit: %w", &pgconn.PgError{Code: "40P01"}), http.StatusServiceUnavailable, `"service_unavailable"`, "1"},
		{"defect", errors.New("unexpected nil pointer"), http.StatusInternalServerError, `"internal_error"`, ""},
		{"constraint violation", &pgconn.PgError{Code: "23514"}, http.StatusInternalServerError, `"internal_error"`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			(&Server{}).writeServerError(response, httptest.NewRequest(http.MethodGet, "/wallets/x", nil), tt.err)
			if response.Code != tt.status || !strings.Contains(response.Body.String(), tt.body) {
				t.Fatalf("expected %d %s, got %d %s", tt.status, tt.body, response.Code, response.Body.String())
			}
			if got := response.Header().Get("Retry-After"); got != tt.retryAfter {
				t.Fatalf("expected Retry-After %q, got %q", tt.retryAfter, got)
			}
		})
	}
}
