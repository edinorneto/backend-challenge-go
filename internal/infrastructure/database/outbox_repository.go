package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type OutboxRepo struct {
	DB *pgxpool.Pool
}

func NewOutboxRepo(db *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{DB: db}
}

func (r *OutboxRepo) ClaimPending(ctx context.Context, limit int, leaseDuration time.Duration, owner string) ([]ports.OutboxEvent, error) {
	if limit <= 0 {
		limit = 1
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	if owner == "" {
		return nil, fmt.Errorf("outbox claim owner is required")
	}
	leaseExpiredAt := time.Now().UTC().Add(-leaseDuration)

	query := `
		WITH claimed AS (
			SELECT event_id
			FROM outbox_events
			WHERE status = 'PENDING'
			  AND next_attempt_at <= NOW()
			  AND (locked_at IS NULL OR locked_at < $1)
			ORDER BY next_attempt_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events e
		SET locked_at = NOW(), locked_by = $3
		FROM claimed
		WHERE e.event_id = claimed.event_id
		RETURNING e.*
	`

	rows, err := r.DB.Query(ctx, query, leaseExpiredAt, limit, owner)
	if err != nil {
		return nil, fmt.Errorf("claim pending outbox: %w", err)
	}
	defer rows.Close()

	return scanOutboxEvents(rows)
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, eventID uuid.UUID, owner string) error {
	result, err := r.DB.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PUBLISHED',
		    published_at = NOW(),
		    last_error = NULL,
		    locked_at = NULL,
		    locked_by = NULL
		WHERE event_id = $1
		  AND status = 'PENDING'
		  AND locked_by = $2
	`, eventID, owner)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox published: event %s is no longer owned by %q", eventID, owner)
	}
	return nil
}

func (r *OutboxRepo) Reschedule(ctx context.Context, eventID uuid.UUID, attempts int, nextAttemptAt time.Time, lastError string, owner string) error {
	if nextAttemptAt.IsZero() {
		nextAttemptAt = time.Now().UTC()
	}
	result, err := r.DB.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = $1,
		    next_attempt_at = $2,
		    last_error = $3,
		    locked_at = NULL,
		    locked_by = NULL
		WHERE event_id = $4
		  AND status = 'PENDING'
		  AND locked_by = $5
	`, attempts, nextAttemptAt, lastError, eventID, owner)
	if err != nil {
		return fmt.Errorf("reschedule outbox: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("reschedule outbox: event %s is no longer owned by %q", eventID, owner)
	}
	return nil
}

func scanOutboxEvents(rows pgx.Rows) ([]ports.OutboxEvent, error) {
	var events []ports.OutboxEvent
	for rows.Next() {
		var event ports.OutboxEvent
		var payload []byte
		var causationID *uuid.UUID
		var lockedAt, publishedAt *time.Time
		var nextAttemptAt, occurredAt, createdAt time.Time
		var lastError *string

		if err := rows.Scan(
			&event.EventID,
			&event.AggregateType,
			&event.AggregateID,
			&event.EventType,
			&event.CorrelationID,
			&causationID,
			&occurredAt,
			&event.Version,
			&payload,
			&event.Status,
			&event.Attempts,
			&nextAttemptAt,
			&lockedAt,
			&event.LockedBy,
			&publishedAt,
			&createdAt,
			&lastError,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}

		event.CausationID = causationID
		event.OccurredAt = occurredAt
		event.NextAttemptAt = nextAttemptAt
		event.LockedAt = lockedAt
		event.PublishedAt = publishedAt
		event.CreatedAt = createdAt
		event.LastError = lastError
		event.Payload = json.RawMessage(payload)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outbox rows: %w", err)
	}
	return events, nil
}

// PendingBacklog reports the events not yet published, across all publishers.
func (r *OutboxRepo) PendingBacklog(ctx context.Context) (int64, time.Time, error) {
	var count int64
	var oldest *time.Time
	if err := r.DB.QueryRow(ctx, `
		SELECT COUNT(*), MIN(occurred_at) FROM outbox_events WHERE status = 'PENDING'
	`).Scan(&count, &oldest); err != nil {
		return 0, time.Time{}, fmt.Errorf("read outbox backlog: %w", err)
	}
	if oldest == nil {
		return count, time.Time{}, nil
	}
	return count, *oldest, nil
}

var _ ports.OutboxBacklogReader = (*OutboxRepo)(nil)
