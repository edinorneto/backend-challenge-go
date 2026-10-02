package application

import (
	"context"
	"encoding/json"
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
}

func TestMarshalEnvelopeUsesEventContract(t *testing.T) {
	eventID := uuid.New()
	occurredAt := time.Date(2026, time.September, 30, 12, 0, 0, 123000000, time.FixedZone("BRT", -3*60*60))
	body, err := marshalEnvelope(ports.OutboxEvent{
		EventID:       eventID,
		EventType:     "WalletBalanceChanged",
		AggregateID:   uuid.New(),
		CorrelationID: uuid.New(),
		OccurredAt:    occurredAt,
		Version:       1,
		Payload:       []byte(`{"walletId":"wallet"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"eventId", "eventType", "aggregateId", "correlationId", "occurredAt", "version", "data"} {
		if _, ok := envelope[key]; !ok {
			t.Fatalf("expected envelope field %q in %s", key, body)
		}
	}
	for _, key := range []string{"type", "timestamp", "payload"} {
		if _, ok := envelope[key]; ok {
			t.Fatalf("unexpected legacy envelope field %q in %s", key, body)
		}
	}

	var serializedTime string
	if err := json.Unmarshal(envelope["occurredAt"], &serializedTime); err != nil {
		t.Fatal(err)
	}
	if serializedTime != "2026-09-30T15:00:00.123Z" {
		t.Fatalf("expected UTC RFC3339 occurredAt, got %q", serializedTime)
	}
}

func TestOutboxPublisherKeepsEventIDOnRepublish(t *testing.T) {
	eventID := uuid.New()
	event := ports.OutboxEvent{
		EventID:       eventID,
		EventType:     "WagerTransactionProcessed",
		AggregateID:   uuid.New(),
		CorrelationID: uuid.New(),
		OccurredAt:    time.Now().UTC(),
		Version:       1,
		Payload:       []byte(`{"status":"PROCESSED"}`),
	}
	repo := &fakeOutboxRepo{}
	publisher := &fakePublisher{fail: true}
	worker := NewOutboxPublisher(repo, publisher, config.Config{})

	if err := worker.publishEvent(context.Background(), event); err == nil {
		t.Fatal("expected first publish attempt to fail before MarkPublished")
	}
	if err := worker.publishEvent(context.Background(), event); err == nil {
		t.Fatal("expected second publish attempt to fail before MarkPublished")
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("expected 2 published messages, got %d", len(publisher.messages))
	}

	var firstEnvelope, secondEnvelope struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal([]byte(publisher.messages[0].Body), &firstEnvelope); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(publisher.messages[1].Body), &secondEnvelope); err != nil {
		t.Fatal(err)
	}
	if firstEnvelope.EventID != eventID.String() || secondEnvelope.EventID != eventID.String() {
		t.Fatalf("expected eventId %s in both envelopes, got %s and %s", eventID, firstEnvelope.EventID, secondEnvelope.EventID)
	}
	if publisher.messages[0].MessageDeduplicationID != eventID.String() ||
		publisher.messages[1].MessageDeduplicationID != eventID.String() {
		t.Fatalf("expected stable SQS deduplication id %s, got %s and %s", eventID, publisher.messages[0].MessageDeduplicationID, publisher.messages[1].MessageDeduplicationID)
	}
	if publisher.messages[0].MessageDeduplicationID != publisher.messages[1].MessageDeduplicationID {
		t.Fatal("expected identical SQS deduplication IDs across publish attempts")
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

func TestMarshalEnvelopeRejectsInvalidMetadata(t *testing.T) {
	_, err := marshalEnvelope(ports.OutboxEvent{
		EventID:       uuid.New(),
		EventType:     "WalletBalanceChanged",
		AggregateID:   uuid.New(),
		CorrelationID: uuid.New(),
		OccurredAt:    time.Time{},
		Version:       1,
		Payload:       json.RawMessage(`{"walletId":"test"}`),
	})
	if err == nil {
		t.Fatal("expected invalid event timestamp to be rejected")
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
