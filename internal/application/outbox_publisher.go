package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/messaging"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

var ErrOutboxPublisherNotConfigured = errors.New("outbox publisher is not configured")

type OutboxPublisher struct {
	repo      ports.OutboxRepository
	publisher ports.OutboxMessagePublisher
	cfg       config.Config
	logger    *observability.Logger
	metrics   *observability.Metrics
	owner     string

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewOutboxPublisher(repo ports.OutboxRepository, publisher ports.OutboxMessagePublisher, cfg config.Config, options ...any) *OutboxPublisher {
	result := &OutboxPublisher{
		repo:      repo,
		publisher: publisher,
		cfg:       cfg,
		owner:     newPublisherOwner(),
	}
	for _, option := range options {
		switch value := option.(type) {
		case *observability.Logger:
			result.logger = value
		case *observability.Metrics:
			result.metrics = value
		}
	}
	return result
}

func newPublisherOwner() string {
	return "outbox-publisher-" + uuid.NewString()
}

func (p *OutboxPublisher) Start(ctx context.Context) error {
	if p == nil || p.repo == nil || p.publisher == nil {
		return ErrOutboxPublisherNotConfigured
	}

	p.mu.Lock()
	if p.cancel != nil {
		p.mu.Unlock()
		return nil
	}

	child, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.mu.Unlock()

	p.wg.Add(1)
	go p.loop(child)
	return nil
}

func (p *OutboxPublisher) Stop(ctx context.Context) error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	cancel := p.cancel
	p.cancel = nil
	p.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *OutboxPublisher) loop(ctx context.Context) {
	defer p.wg.Done()

	interval := p.cfg.OutboxPollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := p.publishPending(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("outbox publisher: %v", err)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *OutboxPublisher) publishPending(ctx context.Context) error {
	if p == nil || p.repo == nil || p.publisher == nil {
		return ErrOutboxPublisherNotConfigured
	}

	batchSize := p.cfg.OutboxBatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	lease := p.cfg.OutboxLeaseDuration
	if lease <= 0 {
		lease = 30 * time.Second
	}

	events, err := p.repo.ClaimPending(ctx, batchSize, lease, p.owner)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	if p.metrics != nil {
		p.metrics.Inc("outbox_claimed_total")
	}
	if p.logger != nil {
		for _, event := range events {
			p.logger.Info(ctx, "outbox_event_claimed", map[string]string{"eventId": event.EventID.String(), "aggregateId": event.AggregateID.String(), "attempt": fmt.Sprint(event.Attempts + 1), "owner": p.owner})
		}
	}
	if p.metrics != nil {
		for _, event := range events {
			p.metrics.Observe("outbox_lag", time.Since(event.CreatedAt))
		}
	}

	for _, event := range events {
		if err := p.publishEvent(ctx, event); err != nil {
			if rescheduleErr := p.repo.Reschedule(
				ctx,
				event.EventID,
				event.Attempts+1,
				time.Now().UTC().Add(p.retryDelay(event.Attempts+1)),
				err.Error(),
				p.owner,
			); rescheduleErr != nil {
				return rescheduleErr
			}
			if p.metrics != nil {
				p.metrics.Inc("outbox_reschedules_total")
			}
			if p.logger != nil {
				p.logger.Error(ctx, "outbox_event_rescheduled", err, map[string]string{"eventId": event.EventID.String(), "attempt": fmt.Sprint(event.Attempts + 1)})
			}
		}
	}
	return nil
}

func (p *OutboxPublisher) publishEvent(ctx context.Context, event ports.OutboxEvent) error {
	body, err := marshalEnvelope(event)
	if err != nil {
		return err
	}
	message := ports.OutboxMessage{
		Body:                   string(body),
		MessageGroupID:         event.AggregateID.String(),
		MessageDeduplicationID: event.EventID.String(),
	}
	if err := p.publisher.Publish(ctx, message); err != nil {
		if p.metrics != nil {
			p.metrics.Inc("outbox_publish_failures_total")
			p.metrics.Inc("outbox_retries_total")
		}
		if p.logger != nil {
			p.logger.Error(ctx, "outbox_publish_failed", err, map[string]string{"eventId": event.EventID.String(), "aggregateId": event.AggregateID.String()})
		}
		return err
	}
	err = p.repo.MarkPublished(ctx, event.EventID, p.owner)
	if err == nil {
		if p.metrics != nil {
			p.metrics.Inc("outbox_published_total")
		}
		if p.logger != nil {
			p.logger.Info(ctx, "outbox_event_published", map[string]string{"eventId": event.EventID.String(), "aggregateId": event.AggregateID.String()})
		}
	} else if p.logger != nil {
		p.logger.Error(ctx, "outbox_mark_published_failed", err, map[string]string{"eventId": event.EventID.String()})
	}
	return err
}

func (p *OutboxPublisher) retryDelay(attempt int) time.Duration {
	base := p.cfg.OutboxRetryBaseDelay
	if base <= 0 {
		base = time.Second
	}
	if attempt <= 0 {
		attempt = 1
	}
	return base * time.Duration(1<<min(6, attempt-1))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func marshalEnvelope(event ports.OutboxEvent) ([]byte, error) {
	envelope := messaging.EventEnvelope{
		EventID:       event.EventID,
		EventType:     event.EventType,
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		CausationID:   event.CausationID,
		OccurredAt:    event.OccurredAt.UTC(),
		Version:       event.Version,
		Data:          event.Payload,
	}
	if err := envelope.Validate(); err != nil {
		return nil, fmt.Errorf("validate event envelope: %w", err)
	}
	return json.Marshal(envelope)
}
