package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
	"github.com/google/uuid"
)

type fakeQueueReceiver struct {
	mu         sync.Mutex
	messages   []ports.QueueMessage
	deleted    []string
	receives   int
	failOnce   bool
	visibility []int
}

func (r *fakeQueueReceiver) ChangeVisibility(_ context.Context, _ string, seconds int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.visibility = append(r.visibility, seconds)
	return nil
}

type fakeTransaction struct {
	committed bool
}

func (t *fakeTransaction) Commit(context.Context) error {
	t.committed = true
	return nil
}

func (t *fakeTransaction) Rollback(context.Context) error { return nil }

type fakeTransactionManager struct {
	tx *fakeTransaction
}

func (m *fakeTransactionManager) Begin(ctx context.Context) (context.Context, ports.Transaction, error) {
	m.tx = &fakeTransaction{}
	return ctx, m.tx, nil
}

type fakeWageringService struct {
	calls int
}

func (s *fakeWageringService) ProcessTransaction(_ context.Context, _ string, _ ports.WageringRequest) (ports.ProcessTransactionResult, error) {
	s.calls++
	balance, _ := money.FromCents(0, "BRL")
	return ports.ProcessTransactionResult{Status: "PROCESSED", Balance: balance}, nil
}

func (r *fakeQueueReceiver) Receive(ctx context.Context, batchSize, waitTimeSeconds, visibilityTimeoutSeconds int) ([]ports.QueueMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receives++
	if r.failOnce {
		r.failOnce = false
		return nil, errors.New("transient receive error")
	}
	messages := append([]ports.QueueMessage(nil), r.messages...)
	r.messages = nil
	return messages, nil
}

func (r *fakeQueueReceiver) Delete(ctx context.Context, receiptHandle string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, receiptHandle)
	return nil
}

type fakeInboxRepository struct {
	mu          sync.Mutex
	processed   map[string]bool
	completed   []string
	processing  int
	maxParallel int
	effectErr   error
}

func (r *fakeInboxRepository) Process(ctx context.Context, consumerName, messageID string, payload []byte, effect ports.InboxEffect) (bool, error) {
	r.mu.Lock()
	if r.processed == nil {
		r.processed = make(map[string]bool)
	}
	if r.processed[consumerName+":"+messageID] {
		r.mu.Unlock()
		return true, nil
	}
	r.processing++
	if r.processing > r.maxParallel {
		r.maxParallel = r.processing
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.processing--
		r.mu.Unlock()
	}()

	if r.effectErr != nil {
		return false, r.effectErr
	}
	if err := effect(ctx, payload); err != nil {
		return false, err
	}
	r.mu.Lock()
	r.processed[consumerName+":"+messageID] = true
	r.completed = append(r.completed, messageID)
	r.mu.Unlock()
	return false, nil
}

func validBody() string {
	return `{"eventId":"11111111-1111-1111-1111-111111111111","eventType":"WalletBalanceChanged","aggregateId":"22222222-2222-2222-2222-222222222222","correlationId":"33333333-3333-3333-3333-333333333333","occurredAt":"2026-09-30T12:00:00Z","version":1,"data":{}}`
}

func validCommandBody() string {
	body, _ := json.Marshal(map[string]any{
		"messageId":  "command-message-1",
		"type":       "WagerTransactionRequested",
		"occurredAt": "2026-09-30T12:00:00Z",
		"data": map[string]any{
			"providerId":            "provider",
			"externalTransactionId": "external",
			"idempotencyKey":        "idem",
			"playerId":              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			"walletId":              uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			"roundId":               "round",
			"gameId":                "game",
			"kind":                  "BET",
			"money":                 map[string]string{"amount": "1.00", "currency": "BRL"},
		},
	})
	return string(body)
}

func TestQueueConsumerProcessesTransactionCommand(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &fakeInboxRepository{}
	wagering := &fakeWageringService{}
	manager := &fakeTransactionManager{}
	consumer := NewFinancialQueueConsumer(receiver, inbox, wagering, manager, QueueConsumerConfig{Name: "transaction-consumer"})

	err := consumer.processMessage(context.Background(), ports.QueueMessage{
		MessageID: "command-1", ReceiptHandle: "receipt-1", Body: validCommandBody(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if wagering.calls != 1 || manager.tx == nil || !manager.tx.committed || len(receiver.deleted) != 1 {
		t.Fatalf("expected command use case, shared commit, and delete: calls=%d tx=%#v deleted=%d", wagering.calls, manager.tx, len(receiver.deleted))
	}
}

func TestQueueConsumerDoesNotInterpretOutputEventAsCommand(t *testing.T) {
	consumer := NewFinancialQueueConsumer(&fakeQueueReceiver{}, &fakeInboxRepository{}, &fakeWageringService{}, &fakeTransactionManager{}, QueueConsumerConfig{Name: "transaction-consumer"})
	err := consumer.processMessage(context.Background(), ports.QueueMessage{
		MessageID: "event-1", ReceiptHandle: "receipt-1", Body: validBody(),
	})
	if err == nil {
		t.Fatal("expected output event to be rejected as an input command")
	}
}

func TestQueueConsumerProcessesAndDeletesAfterInboxCompletion(t *testing.T) {
	receiver := &fakeQueueReceiver{messages: []ports.QueueMessage{{MessageID: "m1", ReceiptHandle: "r1", Body: validBody(), MessageGroup: "g1"}}}
	inbox := &fakeInboxRepository{}
	effectCalled := false
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error {
		effectCalled = true
		return nil
	}, QueueConsumerConfig{Name: "test"})

	if err := consumer.processBatch(context.Background(), receiver.messages); err != nil {
		t.Fatal(err)
	}
	if !effectCalled || len(inbox.completed) != 1 || len(receiver.deleted) != 1 {
		t.Fatalf("expected effect, inbox completion, and delete: effect=%v completed=%d deleted=%d", effectCalled, len(inbox.completed), len(receiver.deleted))
	}
}

func TestQueueConsumerDoesNotDeleteOnProcessingError(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &fakeInboxRepository{effectErr: errors.New("processing failed")}
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error {
		return errors.New("processing failed")
	}, QueueConsumerConfig{Name: "test"})

	err := consumer.processMessage(context.Background(), ports.QueueMessage{MessageID: "m1", ReceiptHandle: "r1", Body: validBody()})
	if err == nil || len(receiver.deleted) != 0 {
		t.Fatalf("expected processing error without delete, err=%v deleted=%d", err, len(receiver.deleted))
	}

}

func TestRetryDelayUsesReceiveCountAndCaps(t *testing.T) {
	initial := 2 * time.Second
	maximum := 10 * time.Second
	tests := []struct {
		receiveCount int
		expected     time.Duration
	}{
		{0, 2 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 10 * time.Second},
		{100, 10 * time.Second},
	}
	for _, test := range tests {
		if got := retryDelay(test.receiveCount, initial, maximum); got != test.expected {
			t.Fatalf("receive count %d: expected %s, got %s", test.receiveCount, test.expected, got)
		}
	}
}

func TestQueueConsumerSchedulesRetryVisibilityWithoutDeleting(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &fakeInboxRepository{effectErr: errors.New("transient processing failure")}
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error {
		return errors.New("transient processing failure")
	}, QueueConsumerConfig{Name: "test", RetryDelay: 2 * time.Second, RetryMaxDelay: 8 * time.Second})

	err := consumer.processBatch(context.Background(), []ports.QueueMessage{
		{MessageID: "m1", ReceiptHandle: "r1", Body: validBody(), ReceiveCount: 2},
	})
	if err == nil {
		t.Fatal("expected processing failure")
	}
	if len(receiver.deleted) != 0 {
		t.Fatalf("expected no delete, got %d", len(receiver.deleted))
	}
	if len(receiver.visibility) != 1 || receiver.visibility[0] != 4 {
		t.Fatalf("expected visibility retry of 4 seconds, got %v", receiver.visibility)
	}
}

func TestQueueConsumerDeduplicatesMessageID(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &fakeInboxRepository{}
	effects := 0
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error {
		effects++
		return nil
	}, QueueConsumerConfig{Name: "test"})
	message := ports.QueueMessage{MessageID: "m1", ReceiptHandle: "r1", Body: validBody()}
	if err := consumer.processMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := consumer.processMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("expected one effect application, got %d", effects)
	}
}

func TestQueueConsumerProcessesDifferentGroupsInParallel(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &fakeInboxRepository{}
	block := make(chan struct{})
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error {
		<-block
		return nil
	}, QueueConsumerConfig{Name: "test"})
	messages := []ports.QueueMessage{
		{MessageID: "m1", ReceiptHandle: "r1", Body: validBody(), MessageGroup: "g1"},
		{MessageID: "m2", ReceiptHandle: "r2", Body: validBody(), MessageGroup: "g2"},
	}
	done := make(chan error, 1)
	go func() { done <- consumer.processBatch(context.Background(), messages) }()
	deadline := time.After(time.Second)
	for {
		inbox.mu.Lock()
		parallel := inbox.maxParallel
		inbox.mu.Unlock()
		if parallel == 2 {
			close(block)
			break
		}
		select {
		case <-deadline:
			t.Fatal("different groups were not processed in parallel")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestQueueConsumerStopsOnlyFailedMessageGroup(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &groupFailureInbox{failedMessageID: "a1"}
	processed := make(chan string, 3)
	consumer := NewQueueConsumer(receiver, inbox, func(_ context.Context, payload []byte) error {
		processed <- string(payload)
		return nil
	}, QueueConsumerConfig{Name: "test"})

	messages := []ports.QueueMessage{
		{MessageID: "a1", ReceiptHandle: "ra1", Body: validBody(), MessageGroup: "group-a"},
		{MessageID: "a2", ReceiptHandle: "ra2", Body: validBody(), MessageGroup: "group-a"},
		{MessageID: "b1", ReceiptHandle: "rb1", Body: validBody(), MessageGroup: "group-b"},
		{MessageID: "b2", ReceiptHandle: "rb2", Body: validBody(), MessageGroup: "group-b"},
	}

	if err := consumer.processBatch(context.Background(), messages); err == nil {
		t.Fatal("expected failed group to return an error")
	}

	inbox.mu.Lock()
	processedIDs := append([]string(nil), inbox.processedIDs...)
	inbox.mu.Unlock()
	if containsString(processedIDs, "a2") {
		t.Fatal("message after failed group-a message was processed")
	}
	if !containsString(processedIDs, "b1") || !containsString(processedIDs, "b2") {
		t.Fatalf("expected group-b messages to continue, processed=%v", processedIDs)
	}
	if containsString(receiver.deleted, "ra1") || containsString(receiver.deleted, "ra2") {
		t.Fatalf("expected group-a messages to remain undeleted, deleted=%v", receiver.deleted)
	}
}

type groupFailureInbox struct {
	mu              sync.Mutex
	failedMessageID string
	processedIDs    []string
}

func (r *groupFailureInbox) Process(ctx context.Context, _ string, messageID string, payload []byte, effect ports.InboxEffect) (bool, error) {
	if messageID == r.failedMessageID {
		return false, errors.New("group message failed")
	}
	if err := effect(ctx, payload); err != nil {
		return false, err
	}
	r.mu.Lock()
	r.processedIDs = append(r.processedIDs, messageID)
	r.mu.Unlock()
	return false, nil
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func TestQueueConsumerShutdownCancelsPolling(t *testing.T) {
	receiver := &fakeQueueReceiver{}
	inbox := &fakeInboxRepository{}
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error { return nil }, QueueConsumerConfig{Name: "test", WaitTimeSeconds: 1})
	ctx, cancel := context.WithCancel(context.Background())
	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueueConsumerInvalidMessageDoesNotStopWorker(t *testing.T) {
	receiver := &fakeQueueReceiver{messages: []ports.QueueMessage{
		{MessageID: "bad", ReceiptHandle: "bad-receipt", Body: "not-json", MessageGroup: "bad-group"},
		{MessageID: "good", ReceiptHandle: "good-receipt", Body: validBody(), MessageGroup: "good-group"},
	}}
	inbox := &fakeInboxRepository{}
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error { return nil }, QueueConsumerConfig{Name: "test"})

	if err := consumer.processBatch(context.Background(), receiver.messages); err == nil {
		t.Fatal("expected invalid message error")
	}
	if len(receiver.deleted) != 1 || receiver.deleted[0] != "good-receipt" {
		t.Fatalf("expected valid message to be deleted while invalid message was retained: %#v", receiver.deleted)
	}
}

func TestQueueConsumerRetriesTransientReceiveError(t *testing.T) {
	receiver := &fakeQueueReceiver{
		messages: []ports.QueueMessage{{MessageID: "m1", ReceiptHandle: "r1", Body: validBody()}},
		failOnce: true,
	}
	inbox := &fakeInboxRepository{}
	consumer := NewQueueConsumer(receiver, inbox, func(context.Context, []byte) error { return nil }, QueueConsumerConfig{
		Name:       "test",
		RetryDelay: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		receiver.mu.Lock()
		deleted := len(receiver.deleted)
		receiver.mu.Unlock()
		if deleted == 1 {
			break
		}
		select {
		case <-deadline:
			cancel()
			_ = consumer.Stop(context.Background())
			t.Fatal("consumer did not recover from transient receive error")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func batchOfThree() []ports.QueueMessage {
	messages := make([]ports.QueueMessage, 3)
	for i := range messages {
		messages[i] = ports.QueueMessage{
			MessageID:     fmt.Sprintf("stop-%d", i),
			ReceiptHandle: fmt.Sprintf("receipt-%d", i),
			MessageGroup:  "wallet-1",
			Body:          validBody(),
		}
	}
	return messages
}

// On SIGTERM the consumer stops polling but completes the batch it already
// received, as long as the stop deadline allows.
func TestQueueConsumerStopCompletesReceivedBatch(t *testing.T) {
	receiver := &fakeQueueReceiver{messages: batchOfThree()}
	started := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	consumer := NewQueueConsumer(receiver, &fakeInboxRepository{}, func(ctx context.Context, _ []byte) error {
		once.Do(func() { close(started) })
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, QueueConsumerConfig{Name: "stop-test", WaitTimeSeconds: 1})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- consumer.Stop(ctx)
	}()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned while a received message was still being processed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(proceed)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop must succeed once the batch completes: %v", err)
	}

	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if len(receiver.deleted) != 3 || len(receiver.visibility) != 0 {
		t.Fatalf("expected the whole received batch to complete and be deleted, deleted=%v visibility=%v", receiver.deleted, receiver.visibility)
	}
}

// If the stop deadline expires, the in-flight work is cancelled and every
// message that did not complete becomes visible again at once.
func TestQueueConsumerStopDeadlineReleasesUnfinishedMessages(t *testing.T) {
	receiver := &fakeQueueReceiver{messages: batchOfThree()}
	started := make(chan struct{})
	var once sync.Once
	consumer := NewQueueConsumer(receiver, &fakeInboxRepository{}, func(ctx context.Context, _ []byte) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}, QueueConsumerConfig{Name: "stop-deadline-test", WaitTimeSeconds: 1})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := consumer.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the stop deadline to be reported, got %v", err)
	}

	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if len(receiver.deleted) != 0 {
		t.Fatalf("aborted messages must not be deleted, got %v", receiver.deleted)
	}
	if len(receiver.visibility) != 3 {
		t.Fatalf("expected the three unfinished messages to be released, got %v", receiver.visibility)
	}
	for _, seconds := range receiver.visibility {
		if seconds != 0 {
			t.Fatalf("released messages must be visible immediately, got visibility %v", receiver.visibility)
		}
	}
}
