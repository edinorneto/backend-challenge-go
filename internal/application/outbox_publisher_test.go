package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type fakeOutboxRepo struct {
	events        map[uuid.UUID]ports.OutboxEvent
	claimed       []ports.OutboxEvent
	published     []uuid.UUID
	didReschedule bool
	failed        bool
}

func (r *fakeOutboxRepo) ClaimPending(ctx context.Context, limit int, leaseDuration time.Duration, owner string) ([]ports.OutboxEvent, error) {
	return r.claimPendingAt(ctx, limit, leaseDuration, owner)
}

func (r *fakeOutboxRepo) claimPendingAt(ctx context.Context, limit int, leaseDuration time.Duration, owner string) ([]ports.OutboxEvent, error) {
	result := make([]ports.OutboxEvent, 0, len(r.events))
	for _, event := range r.events {
		if event.Status == "PENDING" &&
			(event.LockedAt == nil || event.LockedAt.Before(time.Now().UTC().Add(-leaseDuration))) {
			event.Status = "PENDING"
			event.LockedAt = ptrTime(time.Now().UTC())
			event.LockedBy = owner
			r.events[event.EventID] = event
			result = append(result, event)
		}
	}
	r.claimed = append(r.claimed, result...)
	return result, nil
}

func (r *fakeOutboxRepo) MarkPublished(ctx context.Context, eventID uuid.UUID, owner string) error {
	evt := r.events[eventID]
	if evt.LockedBy != owner {
		return errors.New("not owner")
	}
	evt.Status = "PUBLISHED"
	evt.PublishedAt = ptrTime(time.Now().UTC())
	evt.LockedAt = nil
	evt.LockedBy = ""
	r.events[eventID] = evt
	r.published = append(r.published, eventID)
	return nil
}

func (r *fakeOutboxRepo) Reschedule(ctx context.Context, eventID uuid.UUID, attempts int, nextAttemptAt time.Time, lastError string, owner string) error {
	r.didReschedule = true
	evt := r.events[eventID]
	if evt.LockedBy != owner {
		return errors.New("not owner")
	}
	evt.Attempts = attempts
	evt.NextAttemptAt = nextAttemptAt
	evt.LastError = &lastError
	evt.LockedAt = nil
	evt.LockedBy = ""
	r.events[eventID] = evt
	return nil
}

type fakePublisher struct {
	messages []ports.OutboxMessage
	fail     bool
}

func (p *fakePublisher) Publish(ctx context.Context, message ports.OutboxMessage) error {
	p.messages = append(p.messages, message)
	if p.fail {
		return errors.New("queue publish failed")
	}
	return nil
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func TestOutboxPublisherPublishesPendingEvent(t *testing.T) {
	eventID := uuid.New()
	aggregateID := uuid.New()
	correlationID := uuid.New()
	repo := &fakeOutboxRepo{events: map[uuid.UUID]ports.OutboxEvent{eventID: {
		EventID:       eventID,
		AggregateID:   aggregateID,
		EventType:     "WagerTransactionProcessed",
		CorrelationID: correlationID,
		OccurredAt:    time.Now().UTC(),
		Version:       1,
		Payload:       []byte(`{"amount":42}`),
		Status:        "PENDING",
		Attempts:      0,
		NextAttemptAt: time.Now().UTC(),
	}}}
	publisher := &fakePublisher{}
	worker := NewOutboxPublisher(repo, publisher, config.Config{OutboxBatchSize: 10, OutboxPollInterval: time.Second, OutboxLeaseDuration: 30 * time.Second, OutboxRetryBaseDelay: time.Second})

	if err := worker.publishPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(publisher.messages))
	}
	if publisher.messages[0].MessageDeduplicationID != eventID.String() {
		t.Fatalf("expected dedup id %s, got %s", eventID.String(), publisher.messages[0].MessageDeduplicationID)
	}
	if publisher.messages[0].MessageGroupID != aggregateID.String() {
		t.Fatalf("expected group id %s, got %s", aggregateID.String(), publisher.messages[0].MessageGroupID)
	}
	if len(repo.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(repo.published))
	}
}

func TestOutboxPublisherReschedulesOnFailure(t *testing.T) {
	eventID := uuid.New()
	aggregateID := uuid.New()
	repo := &fakeOutboxRepo{events: map[uuid.UUID]ports.OutboxEvent{eventID: {
		EventID:       eventID,
		AggregateID:   aggregateID,
		EventType:     "WalletBalanceChanged",
		CorrelationID: uuid.New(),
		OccurredAt:    time.Now().UTC(),
		Version:       1,
		Payload:       []byte(`{"balance":10}`),
		Status:        "PENDING",
		Attempts:      1,
		NextAttemptAt: time.Now().UTC(),
	}}}
	publisher := &fakePublisher{fail: true}
	worker := NewOutboxPublisher(repo, publisher, config.Config{OutboxBatchSize: 10, OutboxPollInterval: time.Second, OutboxLeaseDuration: 30 * time.Second, OutboxRetryBaseDelay: time.Second})

	if err := worker.publishPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.published) != 0 {
		t.Fatalf("expected no published event, got %d", len(repo.published))
	}
	if !repo.didReschedule {
		t.Fatal("expected failed publish to reschedule the event")
	}
	if repo.events[eventID].LastError == nil || *repo.events[eventID].LastError != "queue publish failed" {
		t.Fatalf("expected last error to be persisted, got %#v", repo.events[eventID].LastError)
	}
}

func TestOutboxPublisherReclaimsExpiredLease(t *testing.T) {
	eventID := uuid.New()
	repo := &fakeOutboxRepo{events: map[uuid.UUID]ports.OutboxEvent{eventID: {
		EventID:     eventID,
		AggregateID: uuid.New(),
		EventType:   "WalletBalanceChanged",
		Status:      "PENDING",
		LockedAt:    ptrTime(time.Now().UTC().Add(-time.Minute)),
		LockedBy:    "crashed-publisher",
	}}}

	events, err := repo.claimPendingAt(context.Background(), 1, 30*time.Second, "new-publisher")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected expired event to be reclaimed, got %d events", len(events))
	}
	if events[0].LockedBy != "new-publisher" {
		t.Fatalf("expected new owner, got %q", events[0].LockedBy)
	}
}

func TestMarshalEnvelopeReturnsPayloadError(t *testing.T) {
	_, err := marshalEnvelope(ports.OutboxEvent{
		EventID:       uuid.New(),
		AggregateID:   uuid.New(),
		CorrelationID: uuid.New(),
		EventType:     "WalletBalanceChanged",
		Version:       1,
		Payload:       []byte(`{"invalid"`),
	})
	if err == nil {
		t.Fatal("expected invalid payload to return a serialization error")
	}
}

func TestOutboxPublisherRejectsStaleOwner(t *testing.T) {
	eventID := uuid.New()
	repo := &fakeOutboxRepo{events: map[uuid.UUID]ports.OutboxEvent{eventID: {
		EventID:  eventID,
		Status:   "PENDING",
		LockedBy: "current-publisher",
		LockedAt: ptrTime(time.Now().UTC()),
	}}}

	if err := repo.MarkPublished(context.Background(), eventID, "expired-publisher"); err == nil {
		t.Fatal("expected stale publisher to be rejected")
	}
}
