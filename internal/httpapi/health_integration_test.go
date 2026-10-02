//go:build integration

package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
)

// The readiness checks run against the real PostgreSQL and LocalStack; only the
// unavailable cases point at a missing queue or an unreachable database.
func TestHealthReadyWithRealDependencies(t *testing.T) {
	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}

	cfg := config.Config{
		AWSRegion:          envOrHTTP("AWS_REGION", "us-east-1"),
		AWSEndpoint:        envOrHTTP("AWS_ENDPOINT", "http://localhost:4566"),
		AWSAccessKeyID:     envOrHTTP("AWS_ACCESS_KEY_ID", "test"),
		AWSSecretAccessKey: envOrHTTP("AWS_SECRET_ACCESS_KEY", "test"),
		TransactionQueue:   "wager-transactions.fifo",
		TransactionDLQ:     "wager-transactions-dlq.fifo",
		EventQueue:         "wager-events.fifo",
	}
	client, err := sqs.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	healthyQueues := sqs.NewQueueManager(client, cfg)
	if err := healthyQueues.Check(ctx); err != nil {
		t.Skipf("LocalStack unavailable: %v", err)
	}

	missingQueueConfig := cfg
	missingQueueConfig.EventQueue = "missing-events-queue.fifo"
	missingQueue := sqs.NewQueueManager(client, missingQueueConfig)

	unreachableConfig, err := pgxpool.ParseConfig("postgres://postgres:postgres@127.0.0.1:1/betting?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	unreachable, err := pgxpool.NewWithConfig(ctx, unreachableConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unreachable.Close)

	tests := []struct {
		name     string
		postgres postgresHealthChecker
		queues   sqsHealthChecker
		status   int
		checks   map[string]any
	}{
		{"all dependencies ready", pool, healthyQueues, http.StatusOK, nil},
		{"event queue missing", pool, missingQueue, http.StatusServiceUnavailable, map[string]any{"postgres": "ok", "sqs": "error"}},
		{"postgres unreachable", unreachable, healthyQueues, http.StatusServiceUnavailable, map[string]any{"postgres": "error", "sqs": "ok"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{postgres: tt.postgres, sqs: tt.queues, auth: &auth.Middleware{}}
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
			if response.Code != tt.status {
				t.Fatalf("expected %d, got %d: %s", tt.status, response.Code, response.Body.String())
			}
			if tt.checks == nil {
				return
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			checks, _ := body["checks"].(map[string]any)
			if !equalJSON(checks, tt.checks) {
				t.Fatalf("expected checks %#v, got %#v", tt.checks, body["checks"])
			}
		})
	}
}
