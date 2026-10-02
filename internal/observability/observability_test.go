package observability

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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

func TestMetricsExposeLabeledCountersAndGauges(t *testing.T) {
	metrics := NewMetrics()
	metrics.IncLabeled("wager_results_total", map[string]string{"status": "PROCESSED"})
	metrics.IncLabeled("wager_results_total", map[string]string{"status": "PROCESSED"})
	metrics.IncLabeled("wager_results_total", map[string]string{"status": "REJECTED"})
	metrics.Inc("wager_results_total_extra")
	metrics.SetGauge("outbox_pending_events", 3)

	if got := metrics.SnapshotLabeled("wager_results_total", map[string]string{"status": "PROCESSED"}); got != 2 {
		t.Fatalf("expected 2 processed results, got %d", got)
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{
		"# TYPE wager_results_total counter\nwager_results_total{status=\"PROCESSED\"} 2\nwager_results_total{status=\"REJECTED\"} 1\n",
		"# TYPE wager_results_total_extra counter\nwager_results_total_extra 1\n",
		"# TYPE outbox_pending_events gauge\noutbox_pending_events 3\n",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics output is missing %q:\n%s", expected, body)
		}
	}
	if strings.Count(body, "# TYPE wager_results_total counter") != 1 {
		t.Fatalf("each counter family must have one TYPE line:\n%s", body)
	}
}

type awsCodeError struct{ code string }

func (e awsCodeError) Error() string     { return "aws failure for queue https://secret-host/queue" }
func (e awsCodeError) ErrorCode() string { return e.code }

func TestClassifyErrorNamesTheCause(t *testing.T) {
	sentinel := errors.New("wallet not found")
	tests := []struct {
		err      error
		expected string
	}{
		{fmt.Errorf("lock wallet: %w", &pgconn.PgError{Code: "40P01"}), "postgres_40P01"},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), "timeout"},
		{fmt.Errorf("receive: %w", context.Canceled), "canceled"},
		{fmt.Errorf("publish: %w", awsCodeError{code: "AWS.SimpleQueueService.NonExistentQueue"}), "aws_AWS.SimpleQueueService.NonExistentQueue"},
		{fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", sentinel)), "*errors.errorString"},
	}
	for _, tt := range tests {
		if got := ErrorClass(tt.err); got != tt.expected {
			t.Fatalf("ErrorClass(%v) = %q, want %q", tt.err, got, tt.expected)
		}
	}
}
