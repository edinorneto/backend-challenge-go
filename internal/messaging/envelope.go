package messaging

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventEnvelope struct {
	EventID       uuid.UUID       `json:"eventId"`
	Type          string          `json:"type"`
	AggregateID   uuid.UUID       `json:"aggregateId"`
	CorrelationID uuid.UUID       `json:"correlationId"`
	CausationID   *uuid.UUID      `json:"causationId,omitempty"`
	Timestamp     time.Time       `json:"timestamp"`
	Version       int             `json:"version"`
	Payload       json.RawMessage `json:"payload"`
}

func (e EventEnvelope) MarshalJSON() ([]byte, error) {
	type alias EventEnvelope
	return json.Marshal(alias(e))
}
