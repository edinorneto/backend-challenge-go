package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidEventEnvelope = errors.New("invalid event envelope")

type EventEnvelope struct {
	EventID       uuid.UUID       `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateID   uuid.UUID       `json:"aggregateId"`
	CorrelationID uuid.UUID       `json:"correlationId"`
	CausationID   *uuid.UUID      `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

func (e EventEnvelope) MarshalJSON() ([]byte, error) {
	type alias EventEnvelope
	return json.Marshal(alias(e))
}

func (e EventEnvelope) Validate() error {
	if e.EventID == uuid.Nil || e.EventType == "" || e.AggregateID == uuid.Nil ||
		e.CorrelationID == uuid.Nil || e.Version < 1 || e.OccurredAt.IsZero() {
		return ErrInvalidEventEnvelope
	}
	if _, offset := e.OccurredAt.Zone(); offset != 0 {
		return ErrInvalidEventEnvelope
	}
	if len(bytes.TrimSpace(e.Data)) == 0 || bytes.Equal(bytes.TrimSpace(e.Data), []byte("null")) || !json.Valid(e.Data) {
		return ErrInvalidEventEnvelope
	}
	return nil
}
