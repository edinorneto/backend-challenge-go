package messaging

import (
	"encoding/json"
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
