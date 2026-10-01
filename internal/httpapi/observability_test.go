package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

func TestHTTPObservabilityCorrelationAndMetrics(t *testing.T) {
	server := NewServer(nil, nil, nil, nil, nil, observability.NewLogger(), observability.NewMetrics())
	handler := server.Handler()

	request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	request.Header.Set("Correlation-ID", "correlation-http-test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || response.Header().Get("Correlation-ID") != "correlation-http-test" {
		t.Fatalf("expected public response with preserved correlation ID: status=%d header=%q", response.Code, response.Header().Get("Correlation-ID"))
	}

	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsResponse.Code != http.StatusOK || !strings.Contains(metricsResponse.Body.String(), "http_request_duration_seconds_count") {
		t.Fatalf("expected Prometheus metrics response: status=%d body=%s", metricsResponse.Code, metricsResponse.Body.String())
	}
}
