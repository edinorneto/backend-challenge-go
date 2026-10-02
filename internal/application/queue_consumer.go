package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/messaging"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type QueueConsumerConfig struct {
	Name                  string
	BatchSize             int
	WaitTimeSeconds       int
	VisibilityTimeoutSecs int
	RetryDelay            time.Duration
	RetryMaxDelay         time.Duration
	MaxReceiveCount       int
}

type QueueConsumer struct {
	receiver  ports.QueueReceiver
	inbox     ports.InboxRepository
	effect    ports.InboxEffect
	wagering  ports.WageringService
	txManager ports.TransactionManager
	cfg       QueueConsumerConfig
	logger    *observability.Logger
	metrics   *observability.Metrics

	mu sync.Mutex
	// cancel stops polling; abort cancels the in-flight work when the stop
	// deadline expires.
	cancel context.CancelFunc
	abort  context.CancelFunc
	wg     sync.WaitGroup
}

func NewFinancialQueueConsumer(receiver ports.QueueReceiver, inbox ports.InboxRepository, wagering ports.WageringService, txManager ports.TransactionManager, cfg QueueConsumerConfig, options ...any) *QueueConsumer {
	return newQueueConsumer(receiver, inbox, wagering, txManager, nil, cfg, options...)
}

func NewQueueConsumer(receiver ports.QueueReceiver, inbox ports.InboxRepository, effect ports.InboxEffect, cfg QueueConsumerConfig, options ...any) *QueueConsumer {
	return newQueueConsumer(receiver, inbox, nil, nil, effect, cfg, options...)
}

func newQueueConsumer(receiver ports.QueueReceiver, inbox ports.InboxRepository, wagering ports.WageringService, txManager ports.TransactionManager, effect ports.InboxEffect, cfg QueueConsumerConfig, options ...any) *QueueConsumer {
	consumer := &QueueConsumer{receiver: receiver, inbox: inbox, wagering: wagering, txManager: txManager, effect: effect, cfg: cfg}
	for _, option := range options {
		switch value := option.(type) {
		case *observability.Logger:
			consumer.logger = value
		case *observability.Metrics:
			consumer.metrics = value
		}
	}
	return consumer
}

func (c *QueueConsumer) Start(ctx context.Context) error {
	if c == nil || c.receiver == nil || c.inbox == nil ||
		(c.wagering == nil && c.effect == nil) ||
		(c.wagering != nil && c.txManager == nil) {
		return errors.New("queue consumer is not configured")
	}
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return nil
	}
	pollCtx, cancel := context.WithCancel(ctx)
	workCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel
	c.abort = abort
	c.mu.Unlock()
	c.wg.Add(1)
	go c.loop(pollCtx, workCtx)
	return nil
}

func (c *QueueConsumer) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	cancel, abort := c.cancel, c.abort
	c.cancel, c.abort = nil, nil
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	// Stop polling first and let the batch already received finish within the
	// deadline. If the deadline expires, abort that work: its transactions roll
	// back and the unfinished messages are released for immediate redelivery.
	cancel()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		abort()
		return nil
	case <-ctx.Done():
		abort()
		select {
		case <-done:
		case <-time.After(releaseTimeout):
		}
		return ctx.Err()
	}
}

func (c *QueueConsumer) loop(ctx, workCtx context.Context) {
	defer c.wg.Done()
	batchSize := c.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	waitTime := c.cfg.WaitTimeSeconds
	if waitTime <= 0 {
		waitTime = 20
	}
	visibility := c.cfg.VisibilityTimeoutSecs
	if visibility <= 0 {
		visibility = 30
	}
	retryDelay := c.cfg.RetryDelay
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	for {
		messages, err := c.receiver.Receive(ctx, batchSize, waitTime, visibility)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if c.logger != nil {
				c.logger.Error(ctx, "consumer_receive_failed", err, nil)
			} else {
				log.Printf("queue consumer receive: %v", err)
			}
			if c.metrics != nil {
				c.metrics.Inc("sqs_consumer_failures_total")
			}
			if !wait(ctx, retryDelay) {
				return
			}
			continue
		}
		if err := c.processBatch(workCtx, messages); err != nil {
			if c.logger != nil {
				c.logger.Error(ctx, "consumer_batch_failed", err, nil)
			} else {
				log.Printf("queue consumer batch: %v", err)
			}
			if c.metrics != nil {
				c.metrics.Inc("sqs_consumer_failures_total")
			}
		}
	}
}

func (c *QueueConsumer) processBatch(ctx context.Context, messages []ports.QueueMessage) error {
	groups := make(map[string][]ports.QueueMessage)
	for _, message := range messages {
		groups[message.MessageGroup] = append(groups[message.MessageGroup], message)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(groups))
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index, message := range group {
				if err := c.processMessage(ctx, message); err != nil {
					if ctx.Err() != nil {
						c.releaseVisibility(group[index:])
						errCh <- err
						return
					}
					c.scheduleRetry(ctx, message, err)
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

func (c *QueueConsumer) scheduleRetry(ctx context.Context, message ports.QueueMessage, cause error) {
	kind := failureKind(cause)
	if c.metrics != nil {
		c.metrics.IncLabeled("sqs_message_failures_total", map[string]string{"kind": kind})
	}
	changer, ok := c.receiver.(ports.QueueVisibilityChanger)
	if !ok {
		return
	}
	delay := retryDelay(message.ReceiveCount, c.cfg.RetryDelay, c.cfg.RetryMaxDelay)
	seconds := int((delay + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	if err := changer.ChangeVisibility(ctx, message.ReceiptHandle, seconds); err != nil {
		if c.logger != nil {
			c.logger.Error(ctx, "consumer_retry_visibility_failed", err, map[string]string{
				"messageId": message.MessageID,
				"attempt":   fmt.Sprint(message.ReceiveCount),
			})
		}
		return
	}
	if c.metrics != nil {
		c.metrics.Inc("sqs_retries_total")
		if message.ReceiveCount > 0 {
			c.metrics.Inc("sqs_messages_retried_total")
		}
		if c.cfg.MaxReceiveCount > 0 && message.ReceiveCount >= c.cfg.MaxReceiveCount {
			c.metrics.Inc("sqs_messages_dlq_eligible_total")
		}
	}
	if c.logger != nil {
		c.logger.Error(ctx, "consumer_message_retry_scheduled", cause, map[string]string{
			"messageId": message.MessageID,
			"attempt":   fmt.Sprint(message.ReceiveCount),
			"duration":  delay.String(),
			"result":    kind,
		})
	}
}

// errInvalidMessage marks a message that cannot be decoded or validated.
var errInvalidMessage = errors.New("invalid message")

// failureKind classifies a failed delivery for metrics and logs. Invalid input
// and conflicts are permanent: redelivery cannot fix them, so the message ends
// in the DLQ. Anything else is treated as an infrastructure failure.
func failureKind(err error) string {
	switch {
	case errors.Is(err, errInvalidMessage),
		errors.Is(err, ErrInvalidWagerRequest),
		errors.Is(err, ErrIdempotencyKeyRequired),
		errors.Is(err, ports.ErrWalletNotFound):
		return "invalid_message"
	case errors.Is(err, ports.ErrIdempotencyConflict),
		errors.Is(err, ports.ErrExternalTransactionConflict):
		return "conflict"
	default:
		return "infrastructure"
	}
}

func retryDelay(receiveCount int, initial, maximum time.Duration) time.Duration {
	if initial <= 0 {
		initial = time.Second
	}
	if maximum <= 0 {
		maximum = 30 * time.Second
	}
	if initial > maximum {
		return maximum
	}
	if receiveCount < 1 {
		receiveCount = 1
	}
	delay := initial
	for i := 1; i < receiveCount; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
		if delay >= maximum {
			return maximum
		}
	}
	return delay
}

func (c *QueueConsumer) processMessage(ctx context.Context, message ports.QueueMessage) error {
	if c.wagering != nil {
		return c.processCommand(ctx, message)
	}
	var envelope messaging.EventEnvelope
	if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
		return fmt.Errorf("decode message %s: %w: %w", message.MessageID, errInvalidMessage, err)
	}
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("validate message %s: %w: invalid event envelope", message.MessageID, errInvalidMessage)
	}
	_, err := c.inbox.Process(ctx, c.cfg.Name, message.MessageID, []byte(message.Body), c.effect)
	if err != nil {
		return err
	}
	return c.receiver.Delete(ctx, message.ReceiptHandle)
}

func (c *QueueConsumer) processCommand(ctx context.Context, message ports.QueueMessage) error {
	var command messaging.WagerTransactionRequested
	if err := json.Unmarshal([]byte(message.Body), &command); err != nil {
		return fmt.Errorf("decode transaction command %s: %w: %w", message.MessageID, errInvalidMessage, err)
	}
	if err := command.Validate(); err != nil {
		return fmt.Errorf("validate transaction command %s: %w: %w", message.MessageID, errInvalidMessage, err)
	}
	// For SQS the envelope messageId is the correlation ID of every log line of
	// this command, as the Correlation-ID header is for HTTP.
	ctx = observability.WithCorrelationID(ctx, command.MessageID)
	if c.logger != nil {
		c.logger.Info(ctx, "consumer_command_validated", map[string]string{
			"messageId": command.MessageID, "walletId": command.Data.WalletID.String(), "providerId": command.Data.ProviderID,
		})
	}
	amount, err := money.ParseExternal(command.Data.Money.Amount, command.Data.Money.Currency)
	if err != nil {
		return fmt.Errorf("parse transaction command money: %w: %w", errInvalidMessage, err)
	}
	txCtx, tx, err := c.txManager.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction command: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	duplicate, err := c.inbox.Process(txCtx, c.cfg.Name, command.MessageID, []byte(message.Body), func(effectCtx context.Context, _ []byte) error {
		result, err := c.wagering.ProcessTransaction(effectCtx, command.Data.IdempotencyKey, ports.WageringRequest{
			ProviderID:                     command.Data.ProviderID,
			ExternalTransactionID:          command.Data.ExternalTransactionID,
			PlayerID:                       command.Data.PlayerID,
			WalletID:                       command.Data.WalletID,
			RoundID:                        command.Data.RoundID,
			GameID:                         command.Data.GameID,
			Kind:                           command.Data.Kind,
			Amount:                         amount,
			ReferenceExternalTransactionID: command.Data.ReferenceExternalTransactionID,
		})
		// Results, replays and rejections are counted by the wagering service for
		// both channels; the consumer only adds what is specific to SQS.
		if err == nil {
			if c.logger != nil {
				fields := map[string]string{
					"messageId": command.MessageID, "walletId": command.Data.WalletID.String(),
					"providerId": command.Data.ProviderID, "transactionId": result.TransactionID.String(),
					"status": result.Status,
				}
				c.logger.Info(effectCtx, "consumer_financial_processing_completed", fields)
			}
		}
		return err
	})
	if err != nil {
		if c.metrics != nil {
			c.metrics.Inc("sqs_consumer_failures_total")
		}
		return err
	}
	if duplicate && c.metrics != nil {
		c.metrics.Inc("inbox_duplicates_total")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction command: %w", err)
	}
	if c.metrics != nil {
		c.metrics.Inc("wager_processing_total")
	}
	if c.logger != nil {
		c.logger.Info(ctx, "consumer_message_deleted", map[string]string{"messageId": command.MessageID, "walletId": command.Data.WalletID.String(), "providerId": command.Data.ProviderID})
	}
	_ = duplicate
	return c.receiver.Delete(ctx, message.ReceiptHandle)
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// releaseTimeout bounds the work done after an aborted stop: waiting for the
// aborted batch to unwind and releasing the visibility of its messages.
const releaseTimeout = 2 * time.Second

// releaseVisibility makes messages that were received but not completed visible
// again at once, so another consumer can take them without waiting for the
// visibility timeout. It runs after the work context was cancelled, so it uses
// its own short deadline.
func (c *QueueConsumer) releaseVisibility(messages []ports.QueueMessage) {
	changer, ok := c.receiver.(ports.QueueVisibilityChanger)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	for _, message := range messages {
		if err := changer.ChangeVisibility(ctx, message.ReceiptHandle, 0); err != nil {
			if c.logger != nil {
				c.logger.Error(ctx, "consumer_release_failed", err, map[string]string{"messageId": message.MessageID})
			}
			continue
		}
		if c.metrics != nil {
			c.metrics.Inc("sqs_messages_released_total")
		}
		if c.logger != nil {
			c.logger.Info(ctx, "consumer_message_released", map[string]string{"messageId": message.MessageID})
		}
	}
}
