package application

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
	sqsInfra "github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/messaging"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

func TestOutboxPublisherRecoversAfterPublishBeforeMarkPublished(t *testing.T) {
	ctx := context.Background()
	pool, cleanupPool := isolatedOutboxPool(t)
	t.Cleanup(cleanupPool)
	walletRepo := database.NewWalletRepo(pool)
	outboxRepo := database.NewOutboxRepo(pool)

	playerID := uuid.New()
	walletID := uuid.New()
	providerID := "provider-outbox-" + walletID.String()
	externalTransactionID := "outbox-" + uuid.New().String()
	idempotencyKey := "outbox-idem-" + uuid.New().String()

	initialBalance, err := money.ParseExternal("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	testWallet, err := wallet.New(walletID, playerID, initialBalance, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	if err := walletRepo.Create(ctx, testWallet); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id = $1 OR correlation_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
	})

	result, err := walletRepo.ProcessTransaction(ctx, ports.ProcessTransactionRequest{
		ProviderID:            providerID,
		ExternalTransactionID: externalTransactionID,
		IdempotencyKey:        idempotencyKey,
		PayloadHash:           "outbox-payload-" + walletID.String(),
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               "outbox-recovery-round",
		GameID:                "outbox-recovery-game",
		Kind:                  "BET",
		Amount:                mustMoney(t, "25.00"),
	})
	if err != nil {
		t.Fatal(err)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM outbox_events
		WHERE correlation_id = $1
	`, result.TransactionID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 2 {
		t.Fatalf("expected two outbox events in the financial commit, got %d", outboxCount)
	}
	var balanceCents int64
	if err := pool.QueryRow(ctx, `SELECT balance_cents FROM wallets WHERE id = $1`, walletID).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}
	if balanceCents != 7500 {
		t.Fatalf("expected committed wallet balance 75.00, got %d cents", balanceCents)
	}
	if result.Status != "PROCESSED" {
		t.Fatalf("expected committed transaction to be PROCESSED, got %s", result.Status)
	}

	var ledgerCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM wallet_ledger_entries
		WHERE transaction_id = $1
	`, result.TransactionID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("expected one ledger entry in the financial commit, got %d", ledgerCount)
	}

	cfg := recoverySQSConfig()
	sqsClient, err := sqsInfra.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueManager := sqsInfra.NewQueueManager(sqsClient, cfg)
	if _, err := queueManager.Resolve(ctx); err != nil {
		t.Skipf("LocalStack/SQS unavailable: %v", err)
	}
	eventQueueURL := createOutboxTestQueue(t, cfg)
	var eventID uuid.UUID

	sqsPublisher, err := sqsInfra.NewPublisher(sqsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := sqsPublisher.ConfigureQueueURL(eventQueueURL); err != nil {
		t.Fatal(err)
	}

	firstPublisher := &publishThenBlockPublisher{
		inner:     sqsPublisher,
		published: make(chan ports.OutboxMessage, 1),
	}
	firstWorker := NewOutboxPublisher(
		outboxRepo,
		firstPublisher,
		config.Config{
			OutboxBatchSize:      1,
			OutboxLeaseDuration:  2 * time.Second,
			OutboxRetryBaseDelay: 100 * time.Millisecond,
		},
	)

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- firstWorker.publishPending(firstCtx)
	}()

	select {
	case message := <-firstPublisher.published:
		var envelope messaging.EventEnvelope
		if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
			t.Fatal(err)
		}
		eventID = envelope.EventID
	case <-time.After(10 * time.Second):
		firstCancel()
		t.Fatal("first publisher did not publish the event")
	}

	var status string
	var lockedBy *string
	if err := pool.QueryRow(ctx, `
		SELECT status, locked_by
		FROM outbox_events
		WHERE event_id = $1
	`, eventID).Scan(&status, &lockedBy); err != nil {
		firstCancel()
		t.Fatal(err)
	}
	if status != "PENDING" || lockedBy == nil || *lockedBy == "" {
		firstCancel()
		t.Fatalf("expected published event to remain leased and pending, status=%s locked_by=%v", status, lockedBy)
	}

	firstCancel()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first publisher did not stop after cancellation")
	}

	time.Sleep(2500 * time.Millisecond)

	secondPublisher, err := sqsInfra.NewPublisher(sqsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondPublisher.ConfigureQueueURL(eventQueueURL); err != nil {
		t.Fatal(err)
	}
	secondRecordingPublisher := &recordingPublisher{inner: secondPublisher}
	secondWorker := NewOutboxPublisher(
		outboxRepo,
		secondRecordingPublisher,
		config.Config{
			OutboxBatchSize:      10,
			OutboxLeaseDuration:  2 * time.Second,
			OutboxRetryBaseDelay: 100 * time.Millisecond,
		},
	)
	if err := secondWorker.publishPending(ctx); err != nil {
		t.Fatal(err)
	}

	if err := pool.QueryRow(ctx, `
		SELECT status, locked_by
		FROM outbox_events
		WHERE event_id = $1
	`, eventID).Scan(&status, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" || lockedBy != nil {
		t.Fatalf("expected recovered event to be published, status=%s locked_by=%v", status, lockedBy)
	}

	if len(secondRecordingPublisher.messages) == 0 {
		t.Fatal("expected recovered publisher to send at least one message")
	}
	var recoveredMessage ports.OutboxMessage
	for _, message := range secondRecordingPublisher.messages {
		var envelope messaging.EventEnvelope
		if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.EventID == eventID {
			recoveredMessage = message
			break
		}
	}
	if recoveredMessage.MessageDeduplicationID != eventID.String() {
		t.Fatalf("expected recovered publish to reuse deduplication ID %s, got %s", eventID, recoveredMessage.MessageDeduplicationID)
	}
}

func TestOutboxRepositoryClaimsConcurrentEventsOnce(t *testing.T) {
	ctx := context.Background()
	pool := integrationRecoveryPool(t)
	walletRepo := database.NewWalletRepo(pool)
	outboxRepo := database.NewOutboxRepo(pool)

	playerID := uuid.New()
	walletID := uuid.New()
	testWallet, err := wallet.New(walletID, playerID, mustMoney(t, "100.00"), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := walletRepo.Create(ctx, testWallet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id = $1 OR correlation_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
	})

	result, err := walletRepo.ProcessTransaction(ctx, ports.ProcessTransactionRequest{
		ProviderID:            "provider-claim-" + walletID.String(),
		ExternalTransactionID: "claim-" + uuid.New().String(),
		IdempotencyKey:        "claim-idem-" + uuid.New().String(),
		PayloadHash:           "claim-payload-" + walletID.String(),
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               "claim-round",
		GameID:                "claim-game",
		Kind:                  "BET",
		Amount:                mustMoney(t, "10.00"),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := pool.Query(ctx, `
		SELECT event_id
		FROM outbox_events
		WHERE aggregate_id = $1
		   OR correlation_id = $2
	`, walletID, result.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var eventID uuid.UUID
		if err := rows.Scan(&eventID); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		expected[eventID] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(expected) < 2 {
		t.Fatalf("expected at least two pending outbox events for the wallet and transaction, got %d", len(expected))
	}

	start := make(chan struct{})
	results := make(chan []ports.OutboxEvent, 2)
	errorsCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"claim-owner-a", "claim-owner-b"} {
		owner := owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			events, err := outboxRepo.ClaimPending(ctx, 1, 30*time.Second, owner)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- events
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errorsCh)

	for err := range errorsCh {
		t.Fatal(err)
	}

	claimed := make(map[uuid.UUID]string)
	for events := range results {
		if len(events) != 1 {
			t.Fatalf("expected each concurrent publisher to claim one event, got %d", len(events))
		}
		eventID := events[0].EventID
		if _, ok := expected[eventID]; !ok {
			t.Fatalf("claimed unrelated outbox event %s", eventID)
		}
		if previous, ok := claimed[eventID]; ok {
			t.Fatalf("event %s was claimed by both %s and %s", eventID, previous, events[0].LockedBy)
		}
		claimed[eventID] = events[0].LockedBy
	}
	if len(claimed) != 2 {
		t.Fatalf("expected two distinct claimed events, got %d", len(claimed))
	}
}

func TestTwoOutboxPublishersPublishConcurrentEventsOnce(t *testing.T) {
	ctx := context.Background()
	pool, cleanupPool := isolatedOutboxPool(t)
	t.Cleanup(cleanupPool)
	walletRepo := database.NewWalletRepo(pool)
	outboxRepo := database.NewOutboxRepo(pool)

	const operationCount = 3
	correlationIDs := make([]uuid.UUID, 0, operationCount)
	for index := 0; index < operationCount; index++ {
		playerID := uuid.New()
		walletID := uuid.New()
		testWallet, err := wallet.New(walletID, playerID, mustMoney(t, "0.00"), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := walletRepo.Create(ctx, testWallet); err != nil {
			t.Fatal(err)
		}

		result, err := walletRepo.ProcessTransaction(ctx, ports.ProcessTransactionRequest{
			ProviderID:            "provider-publishers-" + walletID.String(),
			ExternalTransactionID: "publishers-" + uuid.New().String(),
			IdempotencyKey:        "publishers-idem-" + uuid.New().String(),
			PayloadHash:           "publishers-payload-" + uuid.New().String(),
			PlayerID:              playerID,
			WalletID:              walletID,
			RoundID:               "publishers-round-" + uuid.New().String(),
			GameID:                "publishers-game",
			Kind:                  "WIN",
			Amount:                mustMoney(t, "10.00"),
		})
		if err != nil {
			t.Fatal(err)
		}
		correlationIDs = append(correlationIDs, result.TransactionID)
	}

	expectedEventIDs := make(map[uuid.UUID]struct{})
	for _, correlationID := range correlationIDs {
		rows, err := pool.Query(ctx, `
			SELECT event_id
			FROM outbox_events
			WHERE correlation_id = $1
		`, correlationID)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var eventID uuid.UUID
			if err := rows.Scan(&eventID); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			expectedEventIDs[eventID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}
	if len(expectedEventIDs) != operationCount*2 {
		t.Fatalf("expected %d test outbox events, got %d", operationCount*2, len(expectedEventIDs))
	}

	cfg := recoverySQSConfig()
	sqsClient, err := sqsInfra.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueManager := sqsInfra.NewQueueManager(sqsClient, cfg)
	if _, err := queueManager.Resolve(ctx); err != nil {
		t.Skipf("LocalStack/SQS unavailable: %v", err)
	}
	eventQueueURL := createOutboxTestQueue(t, cfg)

	publisherOne, err := sqsInfra.NewPublisher(sqsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisherOne.ConfigureQueueURL(eventQueueURL); err != nil {
		t.Fatal(err)
	}
	publisherTwo, err := sqsInfra.NewPublisher(sqsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisherTwo.ConfigureQueueURL(eventQueueURL); err != nil {
		t.Fatal(err)
	}

	recordingOne := &recordingPublisher{inner: publisherOne}
	recordingTwo := &recordingPublisher{inner: publisherTwo}
	publisherConfig := config.Config{
		OutboxBatchSize:      operationCount,
		OutboxLeaseDuration:  30 * time.Second,
		OutboxRetryBaseDelay: 100 * time.Millisecond,
	}
	workerOne := NewOutboxPublisher(outboxRepo, recordingOne, publisherConfig)
	workerTwo := NewOutboxPublisher(outboxRepo, recordingTwo, publisherConfig)

	start := make(chan struct{})
	errorsCh := make(chan error, 2)
	var group sync.WaitGroup
	for _, worker := range []*OutboxPublisher{workerOne, workerTwo} {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			errorsCh <- worker.publishPending(ctx)
		}()
	}
	close(start)
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	var pendingCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM outbox_events
		WHERE event_id = ANY($1) AND status = 'PENDING'
	`, uuidSlice(expectedEventIDs)).Scan(&pendingCount); err != nil {
		t.Fatal(err)
	}
	if pendingCount != 0 {
		t.Fatalf("expected no pending test events, got %d", pendingCount)
	}

	var publishedCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM outbox_events
		WHERE event_id = ANY($1) AND status = 'PUBLISHED'
	`, uuidSlice(expectedEventIDs)).Scan(&publishedCount); err != nil {
		t.Fatal(err)
	}
	if publishedCount != len(expectedEventIDs) {
		t.Fatalf("expected all %d test events to be published, got %d", len(expectedEventIDs), publishedCount)
	}

	recorded := make(map[uuid.UUID]ports.OutboxMessage, len(expectedEventIDs))
	for _, message := range append(recordingOne.messages, recordingTwo.messages...) {
		var envelope messaging.EventEnvelope
		if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
			t.Fatal(err)
		}
		if _, ok := expectedEventIDs[envelope.EventID]; !ok {
			t.Fatalf("publisher sent unrelated event %s", envelope.EventID)
		}
		if message.MessageDeduplicationID != envelope.EventID.String() {
			t.Fatalf("event %s used deduplication ID %s", envelope.EventID, message.MessageDeduplicationID)
		}
		if _, exists := recorded[envelope.EventID]; exists {
			t.Fatalf("event %s was published by both publishers", envelope.EventID)
		}
		recorded[envelope.EventID] = message
	}
	if len(recorded) != len(expectedEventIDs) {
		t.Fatalf("expected one recorded publish for each of %d events, got %d", len(expectedEventIDs), len(recorded))
	}

	consumer, err := sqsInfra.NewConsumer(sqsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.ConfigureQueueURL(eventQueueURL); err != nil {
		t.Fatal(err)
	}
	receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	received := make(map[uuid.UUID]struct{}, len(expectedEventIDs))
	for len(received) < len(expectedEventIDs) {
		messages, err := consumer.Receive(receiveCtx, 10, 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range messages {
			var envelope messaging.EventEnvelope
			if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
				t.Fatal(err)
			}
			if _, ok := expectedEventIDs[envelope.EventID]; !ok {
				t.Fatalf("queue received unrelated event %s", envelope.EventID)
			}
			if _, exists := received[envelope.EventID]; exists {
				t.Fatalf("queue delivered event %s more than once", envelope.EventID)
			}
			received[envelope.EventID] = struct{}{}
			if err := consumer.Delete(ctx, message.ReceiptHandle); err != nil {
				t.Fatal(err)
			}
		}
	}
}

type publishThenBlockPublisher struct {
	inner     ports.OutboxMessagePublisher
	published chan ports.OutboxMessage
}

func (p *publishThenBlockPublisher) Publish(ctx context.Context, message ports.OutboxMessage) error {
	if err := p.inner.Publish(ctx, message); err != nil {
		return err
	}
	p.published <- message
	<-ctx.Done()
	return ctx.Err()
}

type recordingPublisher struct {
	inner    ports.OutboxMessagePublisher
	messages []ports.OutboxMessage
}

func (p *recordingPublisher) Publish(ctx context.Context, message ports.OutboxMessage) error {
	if err := p.inner.Publish(ctx, message); err != nil {
		return err
	}
	p.messages = append(p.messages, message)
	return nil
}

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	value, err := money.ParseExternal(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func uuidSlice(values map[uuid.UUID]struct{}) []uuid.UUID {
	result := make([]uuid.UUID, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	return result
}

func createOutboxTestQueue(t *testing.T, cfg config.Config) string {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithBaseEndpoint(cfg.AWSEndpoint),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := awsSQS.NewFromConfig(awsCfg)
	queueName := "outbox-recovery-" + uuid.NewString() + ".fifo"
	output, err := client.CreateQueue(context.Background(), &awsSQS.CreateQueueInput{
		QueueName: &queueName,
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil || output.QueueUrl == nil {
		t.Fatalf("create outbox recovery queue: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.DeleteQueue(ctx, &awsSQS.DeleteQueueInput{QueueUrl: output.QueueUrl}); err != nil {
			t.Logf("delete outbox recovery queue: %v", err)
		}
	})

	return *output.QueueUrl
}

var _ ports.OutboxMessagePublisher = (*publishThenBlockPublisher)(nil)

func isolatedOutboxPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	basePool := integrationRecoveryPool(t)
	ctx := context.Background()
	schema := "outbox_recovery_" + uuid.New().String()[:8]
	if _, err := basePool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		basePool.Close()
		t.Fatal(err)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "******localhost:5432/betting?sslmode=disable"
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
		t.Fatal(err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
		t.Fatal(err)
	}
	if err := migrations.Run(ctx, pool); err != nil {
		pool.Close()
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
		t.Skipf("isolated PostgreSQL schema unavailable: %v", err)
	}

	return pool, func() {
		pool.Close()
		_, _ = basePool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		basePool.Close()
	}
}
