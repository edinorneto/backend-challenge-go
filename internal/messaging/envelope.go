package messaging

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidEventEnvelope = errors.New("invalid event envelope")

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

func (e EventEnvelope) Validate() error {
	if e.EventID == uuid.Nil || e.Type == "" || e.AggregateID == uuid.Nil ||
		e.CorrelationID == uuid.Nil || e.Version < 1 {
		return ErrInvalidEventEnvelope
	}
	return nil
}
