package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

type healthPostgres struct {
	err error
}

func (p healthPostgres) Ping(context.Context) error {
	return p.err
}

type healthSQS struct {
	err error
}

func (s healthSQS) Check(context.Context) error {
	return s.err
}

func TestHealthReady(t *testing.T) {
	tests := []struct {
		name           string
		postgresErr    error
		sqsErr         error
		expectedStatus int
		expectedBody   map[string]any
	}{
		{
			name:           "all dependencies available",
			expectedStatus: http.StatusOK,
			expectedBody:   map[string]any{"status": "ready"},
		},
		{
			name:           "postgres unavailable",
			postgresErr:    errors.New("postgres unavailable"),
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody: map[string]any{
				"status": "not_ready",
				"checks": map[string]any{"postgres": "error", "sqs": "ok"},
			},
		},
		{
			name:           "sqs unavailable",
			sqsErr:         errors.New("sqs unavailable"),
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody: map[string]any{
				"status": "not_ready",
				"checks": map[string]any{"postgres": "ok", "sqs": "error"},
			},
		},
		{
			name:           "all dependencies unavailable",
			postgresErr:    errors.New("postgres unavailable"),
			sqsErr:         errors.New("sqs unavailable"),
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody: map[string]any{
				"status": "not_ready",
				"checks": map[string]any{"postgres": "error", "sqs": "error"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{
				postgres: healthPostgres{err: test.postgresErr},
				sqs:      healthSQS{err: test.sqsErr},
				auth:     &auth.Middleware{},
			}
			request := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
			response := httptest.NewRecorder()

			server.Handler().ServeHTTP(response, request)

			if response.Code != test.expectedStatus {
				t.Fatalf("expected status %d, got %d", test.expectedStatus, response.Code)
			}

			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if !equalJSON(body, test.expectedBody) {
				t.Fatalf("expected body %#v, got %#v", test.expectedBody, body)
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("expected JSON content type, got %q", response.Header().Get("Content-Type"))
			}
		})
	}
}

func TestHealthReadyIsPublic(t *testing.T) {
	server := &Server{
		postgres: healthPostgres{},
		sqs:      healthSQS{},
		auth:     &auth.Middleware{},
	}
	request := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code == http.StatusUnauthorized || response.Code == http.StatusForbidden {
		t.Fatalf("health endpoint must be public, got %d", response.Code)
	}
}

func TestHealthReadyRejectsNonGet(t *testing.T) {
	server := &Server{
		postgres: healthPostgres{},
		sqs:      healthSQS{},
		auth:     &auth.Middleware{},
	}
	request := httptest.NewRequest(http.MethodPost, "/health/ready", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, response.Code)
	}
}

func TestHealthLiveDoesNotCheckDependencies(t *testing.T) {
	server := &Server{
		postgres: healthPostgres{err: errors.New("postgres unavailable")},
		sqs:      healthSQS{err: errors.New("sqs unavailable")},
		auth:     &auth.Middleware{},
	}
	request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
}

func equalJSON(left, right map[string]any) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}

type hangingPostgres struct{}

func (hangingPostgres) Ping(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHealthReadyIsolatesSlowDependencyAndLogsReason(t *testing.T) {
	previousTimeout := readinessCheckTimeout
	readinessCheckTimeout = 50 * time.Millisecond
	t.Cleanup(func() { readinessCheckTimeout = previousTimeout })

	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	metrics := observability.NewMetrics()
	server := &Server{
		postgres: hangingPostgres{},
		sqs:      healthSQS{},
		auth:     &auth.Middleware{},
		logger:   observability.NewLogger(),
		metrics:  metrics,
	}
	request := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	response := httptest.NewRecorder()
	started := time.Now()
	server.Handler().ServeHTTP(response, request)

	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("readiness must be bounded by the per-dependency timeout, took %s", elapsed)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{"status": "not_ready", "checks": map[string]any{"postgres": "error", "sqs": "ok"}}
	if response.Code != http.StatusServiceUnavailable || !equalJSON(body, expected) {
		t.Fatalf("a hanging PostgreSQL must not mark SQS as failed: %d %#v", response.Code, body)
	}
	logs := output.String()
	if !strings.Contains(logs, `"message":"readiness_check_failed"`) ||
		!strings.Contains(logs, `"dependency":"postgres"`) ||
		!strings.Contains(logs, `"reason":"timeout"`) {
		t.Fatalf("expected a readiness failure log with dependency and reason, got %s", logs)
	}
	if strings.Contains(logs, `"dependency":"sqs"`) {
		t.Fatalf("healthy SQS must not be logged as failed: %s", logs)
	}
	if got := metrics.Snapshot("readiness_check_failures_total"); got != 1 {
		t.Fatalf("expected one readiness failure metric, got %d", got)
	}
}

func TestHealthReadyLogsUnavailableDependencyWithoutRawError(t *testing.T) {
	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	server := &Server{
		postgres: healthPostgres{},
		sqs:      healthSQS{err: errors.New("dial tcp localstack:4566 secret-detail")},
		auth:     &auth.Middleware{},
		logger:   observability.NewLogger(),
	}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	logs := output.String()
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(logs, `"dependency":"sqs"`) || !strings.Contains(logs, `"reason":"unavailable"`) {
		t.Fatalf("expected an unavailable SQS log, got %d %s", response.Code, logs)
	}
	if strings.Contains(logs, "secret-detail") {
		t.Fatalf("readiness log leaked the raw dependency error: %s", logs)
	}
}
