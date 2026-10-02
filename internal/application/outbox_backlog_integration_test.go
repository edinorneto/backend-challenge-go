package application

import (
	"context"
	"testing"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

// While publication fails the backlog gauges keep showing the waiting events and
// how old the oldest one is; once they are published the gauges return to zero.
func TestOutboxPublisherExposesBacklogWhilePublicationIsStuck(t *testing.T) {
	pool, cleanup := isolatedOutboxPool(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	createWorkerTestWallet(t, database.NewWalletRepo(pool), "100.00") // OPENING: two events

	metrics := observability.NewMetrics()
	sqsDown := &fakePublisher{fail: true}
	publisher := NewOutboxPublisher(database.NewOutboxRepo(pool), sqsDown, config.Config{OutboxRetryBaseDelay: time.Millisecond}, metrics)

	time.Sleep(50 * time.Millisecond)
	_ = publisher.publishPending(ctx)
	publisher.recordBacklog(ctx)
	if got := metrics.Gauge("outbox_pending_events"); got != 2 {
		t.Fatalf("expected two pending events while SQS is down, got %v", got)
	}
	if age := metrics.Gauge("outbox_oldest_pending_seconds"); age < 0.05 {
		t.Fatalf("expected the oldest pending event age to grow, got %vs", age)
	}
	if metrics.Snapshot("outbox_published_total") != 0 {
		t.Fatal("nothing can be published while SQS is down")
	}

	sqsDown.fail = false
	deadline := time.Now().Add(5 * time.Second)
	for metrics.Snapshot("outbox_published_total") < 2 && time.Now().Before(deadline) {
		_ = publisher.publishPending(ctx)
		time.Sleep(10 * time.Millisecond)
	}
	publisher.recordBacklog(ctx)
	if got := metrics.Gauge("outbox_pending_events"); got != 0 {
		t.Fatalf("expected an empty backlog after publication, got %v", got)
	}
	if got := metrics.Gauge("outbox_oldest_pending_seconds"); got != 0 {
		t.Fatalf("expected no pending age after publication, got %v", got)
	}
}
