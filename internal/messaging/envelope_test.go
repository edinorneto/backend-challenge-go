package messaging

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func validEventEnvelope() EventEnvelope {
	return EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "WalletBalanceChanged",
		AggregateID:   uuid.New(),
		CorrelationID: uuid.New(),
		OccurredAt:    time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC),
		Version:       1,
		Data:          json.RawMessage(`{"walletId":"00000000-0000-0000-0000-000000000001"}`),
	}
}

func TestEventEnvelopeValidateRequiresCompleteContract(t *testing.T) {
	tests := []struct {
		name  string
		alter func(*EventEnvelope)
	}{
		{"missing occurred at", func(e *EventEnvelope) { e.OccurredAt = time.Time{} }},
		{"non UTC occurred at", func(e *EventEnvelope) {
			e.OccurredAt = time.Date(2026, 9, 30, 9, 0, 0, 0, time.FixedZone("-03", -3*60*60))
		}},
		{"missing data", func(e *EventEnvelope) { e.Data = nil }},
		{"null data", func(e *EventEnvelope) { e.Data = json.RawMessage(`null`) }},
		{"invalid data", func(e *EventEnvelope) { e.Data = json.RawMessage(`{"walletId":`) }},
		{"missing event id", func(e *EventEnvelope) { e.EventID = uuid.Nil }},
		{"invalid version", func(e *EventEnvelope) { e.Version = 0 }},
		{"unknown event version", func(e *EventEnvelope) { e.Version = 2 }},
		{"unknown event type", func(e *EventEnvelope) { e.EventType = "WalletDeleted" }},
		{"missing event type", func(e *EventEnvelope) { e.EventType = "" }},
		{"missing aggregate id", func(e *EventEnvelope) { e.AggregateID = uuid.Nil }},
		{"missing correlation id", func(e *EventEnvelope) { e.CorrelationID = uuid.Nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envelope := validEventEnvelope()
			tt.alter(&envelope)
			if err := envelope.Validate(); err == nil {
				t.Fatal("expected invalid event envelope")
			}
		})
	}

	envelope := validEventEnvelope()
	if err := envelope.Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
}

func TestEventEnvelopeSerializesContractFields(t *testing.T) {
	causationID := uuid.New()
	envelope := validEventEnvelope()
	envelope.CausationID = &causationID
	envelope.OccurredAt = time.Date(2026, 9, 30, 12, 0, 0, 123000000, time.UTC)

	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("envelope is missing %s: %s", key, body)
		}
	}
	if decoded["occurredAt"] != "2026-09-30T12:00:00.123Z" {
		t.Fatalf("expected RFC 3339 UTC occurredAt, got %v", decoded["occurredAt"])
	}
	if _, err := time.Parse(time.RFC3339, decoded["occurredAt"].(string)); err != nil {
		t.Fatalf("occurredAt is not RFC 3339: %v", err)
	}

	envelope.CausationID = nil
	body, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "causationId") {
		t.Fatalf("optional causationId must be omitted when absent: %s", body)
	}
}
