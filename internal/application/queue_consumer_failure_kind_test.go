package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type failingWageringService struct {
	err error
}

func (s failingWageringService) ProcessTransaction(context.Context, string, ports.WageringRequest) (ports.ProcessTransactionResult, error) {
	return ports.ProcessTransactionResult{}, s.err
}

// Permanent failures (bad input, conflicts) are told apart from infrastructure
// failures in the metric and in the retry log line.
func TestQueueConsumerClassifiesFailedDeliveries(t *testing.T) {
	cases := []struct {
		name string
		body string
		err  error
		want string
	}{
		{"malformed JSON", "not-json", nil, "invalid_message"},
		{"invalid command", `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{}}`, nil, "invalid_message"},
		{"invalid wager request", validCommandBody(), fmt.Errorf("process: %w", ErrInvalidWagerRequest), "invalid_message"},
		{"wallet not found", validCommandBody(), ports.ErrWalletNotFound, "invalid_message"},
		{"idempotency conflict", validCommandBody(), ports.ErrIdempotencyConflict, "conflict"},
		{"external transaction conflict", validCommandBody(), ports.ErrExternalTransactionConflict, "conflict"},
		{"database down", validCommandBody(), errors.New("connection refused"), "infrastructure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			metrics := observability.NewMetrics()
			receiver := &fakeQueueReceiver{}
			consumer := NewFinancialQueueConsumer(receiver, &fakeInboxRepository{}, failingWageringService{err: tc.err}, &fakeTransactionManager{},
				QueueConsumerConfig{Name: "transaction-consumer"}, observability.NewLogger(), metrics)

			if err := consumer.processBatch(context.Background(), []ports.QueueMessage{{MessageID: "m1", ReceiptHandle: "r1", Body: tc.body, ReceiveCount: 1}}); err == nil {
				t.Fatal("expected the delivery to fail")
			}
			if len(receiver.deleted) != 0 {
				t.Fatalf("a failed delivery must not be deleted, got %v", receiver.deleted)
			}
			for _, kind := range []string{"invalid_message", "conflict", "infrastructure"} {
				want := uint64(0)
				if kind == tc.want {
					want = 1
				}
				if got := metrics.SnapshotLabeled("sqs_message_failures_total", map[string]string{"kind": kind}); got != want {
					t.Fatalf("sqs_message_failures_total{kind=%q}: expected %d, got %d", kind, want, got)
				}
			}
			if !strings.Contains(logs.String(), `"result":"`+tc.want+`"`) {
				t.Fatalf("expected result %s in the retry log: %s", tc.want, logs.String())
			}
		})
	}
}
