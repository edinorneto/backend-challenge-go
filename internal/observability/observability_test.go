package observability

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposeCountersAndHistograms(t *testing.T) {
	metrics := NewMetrics()
	metrics.Inc("inbox_duplicates_total")
	metrics.Observe("http_request_duration", 2)

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body := response.Body.String()
	if response.Code != http.StatusOK ||
		!strings.Contains(body, "inbox_duplicates_total 1") ||
		!strings.Contains(body, "http_request_duration_seconds_count 1") {
		t.Fatalf("unexpected metrics response: status=%d body=%s", response.Code, body)
	}
}

func TestCorrelationIDContext(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "correlation-test")
	if got := CorrelationID(ctx); got != "correlation-test" {
		t.Fatalf("expected correlation ID, got %q", got)
	}
}

func TestLoggerOnlyWritesAllowlistedFields(t *testing.T) {
	var output bytes.Buffer
	logger := &Logger{std: log.New(&output, "", 0)}

	logger.Info(context.Background(), "safe_event", map[string]string{
		"messageId":     "message-1",
		"Authorization": "Bearer secret",
		"payload":       `{"amount":"100.00"}`,
	})

	body := output.String()
	if !strings.Contains(body, `"messageId":"message-1"`) ||
		strings.Contains(body, "Bearer secret") ||
		strings.Contains(body, "100.00") {
		t.Fatalf("logger leaked a prohibited field: %s", body)
	}
}
