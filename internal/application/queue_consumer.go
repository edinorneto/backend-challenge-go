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
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type QueueConsumerConfig struct {
	Name                  string
	BatchSize             int
	WaitTimeSeconds       int
	VisibilityTimeoutSecs int
	RetryDelay            time.Duration
}

type QueueConsumer struct {
	receiver  ports.QueueReceiver
	inbox     ports.InboxRepository
	effect    ports.InboxEffect
	wagering  ports.WageringService
	txManager ports.TransactionManager
	cfg       QueueConsumerConfig

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewFinancialQueueConsumer(receiver ports.QueueReceiver, inbox ports.InboxRepository, wagering ports.WageringService, txManager ports.TransactionManager, cfg QueueConsumerConfig) *QueueConsumer {
	return &QueueConsumer{receiver: receiver, inbox: inbox, wagering: wagering, txManager: txManager, cfg: cfg}
}

func NewQueueConsumer(receiver ports.QueueReceiver, inbox ports.InboxRepository, effect ports.InboxEffect, cfg QueueConsumerConfig) *QueueConsumer {
	return &QueueConsumer{receiver: receiver, inbox: inbox, effect: effect, cfg: cfg}
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
	child, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()
	c.wg.Add(1)
	go c.loop(child)
	return nil
}

func (c *QueueConsumer) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *QueueConsumer) loop(ctx context.Context) {
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
			log.Printf("queue consumer receive: %v", err)
			if !wait(ctx, retryDelay) {
				return
			}
			continue
		}
		if err := c.processBatch(ctx, messages); err != nil {
			log.Printf("queue consumer batch: %v", err)
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
			for _, message := range group {
				if err := c.processMessage(ctx, message); err != nil {
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

func (c *QueueConsumer) processMessage(ctx context.Context, message ports.QueueMessage) error {
	if c.wagering != nil {
		return c.processCommand(ctx, message)
	}
	var envelope messaging.EventEnvelope
	if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
		return fmt.Errorf("decode message %s: %w", message.MessageID, err)
	}
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("validate message %s: invalid event envelope", message.MessageID)
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
		return fmt.Errorf("decode transaction command %s: %w", message.MessageID, err)
	}
	if err := command.Validate(); err != nil {
		return fmt.Errorf("validate transaction command %s: %w", message.MessageID, err)
	}
	amount, err := money.ParseExternal(command.Data.Money.Amount, command.Data.Money.Currency)
	if err != nil {
		return fmt.Errorf("parse transaction command money: %w", err)
	}
	txCtx, tx, err := c.txManager.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction command: %w", err)
	}
	defer tx.Rollback(ctx)
	duplicate, err := c.inbox.Process(txCtx, c.cfg.Name, command.MessageID, []byte(message.Body), func(effectCtx context.Context, _ []byte) error {
		_, err := c.wagering.ProcessTransaction(effectCtx, command.Data.IdempotencyKey, ports.WageringRequest{
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
		return err
	})
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction command: %w", err)
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
