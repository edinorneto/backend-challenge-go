package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/auth"
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
