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
	sqsInfra "github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/messaging"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

func TestFinancialQueueConsumerRecoveryAfterCommit(t *testing.T) {
	ctx := context.Background()

	pool := integrationRecoveryPool(t)
	walletRepo := database.NewWalletRepo(pool)
	inboxRepo := database.NewInboxRepo(pool)
	transactionManager := database.NewTransactionManager(pool)
	wageringService := NewWageringService(walletRepo)

	playerID := uuid.New()
	walletID := uuid.New()
	providerID := "provider-recovery-" + walletID.String()
	externalTransactionID := "recovery-" + uuid.New().String()
	idempotencyKey := "recovery-idem-" + uuid.New().String()

	initialBalance, err := money.ParseExternal("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	testWallet, err := wallet.New(
		walletID,
		playerID,
		initialBalance,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := walletRepo.Create(ctx, testWallet); err != nil {
		t.Fatal(err)
	}

	cfg := recoverySQSConfig()

	sqsClient, err := sqsInfra.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	transactionQueueURL := createRecoveryTestQueue(t, cfg)

	message := messaging.WagerTransactionRequested{
		Type: "WagerTransactionRequested",
		Data: messaging.WagerTransactionData{
			ProviderID:            providerID,
			ExternalTransactionID: externalTransactionID,
			IdempotencyKey:        idempotencyKey,
			PlayerID:              playerID,
			WalletID:              walletID,
			RoundID:               "round-recovery-test",
			GameID:                "game-recovery-test",
			Kind:                  "BET",
			Money: messaging.MoneyData{
				Amount:   "25.00",
				Currency: "BRL",
			},
		},
	}

	messageBody, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	messageID := sendRecoveryMessage(
		t,
		cfg,
		transactionQueueURL,
		string(messageBody),
		walletID.String(),
		"recovery-"+uuid.New().String(),
	)

	t.Cleanup(func() {
		cleanupRecoveryMessage(t, transactionQueueURL, messageID)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM inbox_messages
             WHERE consumer_name = $1
               AND message_id = $2`,
			"transaction-consumer",
			messageID,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM outbox_events
             WHERE aggregate_id = $1
                OR correlation_id IN (
                    SELECT id FROM wager_transactions WHERE wallet_id = $1
                )`,
			walletID,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`,
			walletID,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM wager_transactions WHERE wallet_id = $1`,
			walletID,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM wallets WHERE id = $1`,
			walletID,
		)
	})

	sqsConsumer, err := sqsInfra.NewConsumer(sqsClient)
	if err != nil {
		t.Fatal(err)
	}

	if err := sqsConsumer.ConfigureQueueURL(transactionQueueURL); err != nil {
		t.Fatal(err)
	}

	firstReceiver := &blockingDeleteReceiver{
		inner:         sqsConsumer,
		deleteStarted: make(chan struct{}),
	}

	firstConsumer := NewFinancialQueueConsumer(
		firstReceiver,
		inboxRepo,
		wageringService,
		transactionManager,
		QueueConsumerConfig{
			Name:                  "transaction-consumer",
			BatchSize:             1,
			WaitTimeSeconds:       1,
			VisibilityTimeoutSecs: 2,
			RetryDelay:            100 * time.Millisecond,
		},
	)

	firstCtx, firstCancel := context.WithCancel(context.Background())

	if err := firstConsumer.Start(firstCtx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		firstCancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		if err := firstConsumer.Stop(stopCtx); err != nil {
			t.Logf("stop first consumer during cleanup: %v", err)
		}
	})

	select {
	case <-firstReceiver.deleteStarted:
	case <-time.After(10 * time.Second):
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatal("first consumer never reached DeleteMessage")
	}

	var transactionID uuid.UUID
	var status string
	var resultBalance int64

	if err := pool.QueryRow(
		ctx,
		`
        SELECT id, status, result_balance_cents
        FROM wager_transactions
        WHERE provider_id = $1
          AND external_transaction_id = $2
        `,
		providerID,
		externalTransactionID,
	).Scan(&transactionID, &status, &resultBalance); err != nil {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatal(err)
	}

	if status != "PROCESSED" {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatalf("expected committed transaction to be PROCESSED, got %s", status)
	}

	if resultBalance != 7500 {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatalf("expected committed result balance to be 75.00, got %d cents", resultBalance)
	}

	var balanceCents int64
	if err := pool.QueryRow(
		ctx,
		`SELECT balance_cents FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balanceCents); err != nil {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatal(err)
	}

	if balanceCents != 7500 {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatalf("expected wallet balance 75.00 after first commit, got %d cents", balanceCents)
	}

	var ledgerCount int
	if err := pool.QueryRow(
		ctx,
		`
        SELECT COUNT(*)
        FROM wallet_ledger_entries
        WHERE transaction_id = $1
        `,
		transactionID,
	).Scan(&ledgerCount); err != nil {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatal(err)
	}

	if ledgerCount != 1 {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatalf("expected one ledger entry after first commit, got %d", ledgerCount)
	}

	var inboxCompleted bool
	if err := pool.QueryRow(
		ctx,
		`
        SELECT completed_at IS NOT NULL
        FROM inbox_messages
        WHERE consumer_name = $1
          AND message_id = $2
        `,
		"transaction-consumer",
		messageID,
	).Scan(&inboxCompleted); err != nil {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatal(err)
	}

	if !inboxCompleted {
		firstCancel()
		_ = firstConsumer.Stop(context.Background())
		t.Fatal("expected Inbox message to be completed before DeleteMessage")
	}

	firstCancel()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()

	if err := firstConsumer.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}

	secondSQSConsumer, err := sqsInfra.NewConsumer(sqsClient)
	if err != nil {
		t.Fatal(err)
	}

	if err := secondSQSConsumer.ConfigureQueueURL(transactionQueueURL); err != nil {
		t.Fatal(err)
	}

	secondReceiver := &observingDeleteReceiver{
		inner:           secondSQSConsumer,
		deleteSuccess:   make(chan struct{}),
		messageReceived: make(chan struct{}),
	}

	secondConsumer := NewFinancialQueueConsumer(
		secondReceiver,
		inboxRepo,
		wageringService,
		transactionManager,
		QueueConsumerConfig{
			Name:                  "transaction-consumer",
			BatchSize:             1,
			WaitTimeSeconds:       1,
			VisibilityTimeoutSecs: 2,
			RetryDelay:            100 * time.Millisecond,
		},
	)

	secondCtx, secondCancel := context.WithCancel(context.Background())

	if err := secondConsumer.Start(secondCtx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		secondCancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		if err := secondConsumer.Stop(stopCtx); err != nil {
			t.Logf("stop second consumer during cleanup: %v", err)
		}
	})

	select {
	case <-secondReceiver.messageReceived:
	case <-time.After(10 * time.Second):
		t.Fatal("second consumer never received the redelivered message")
	}

	select {
	case <-secondReceiver.deleteSuccess:
	case <-time.After(10 * time.Second):
		t.Fatal("second consumer never deleted the redelivered message")
	}

	firstIDs := firstReceiver.receivedIDs()
	secondIDs := secondReceiver.receivedIDs()

	if len(firstIDs) != 1 {
		t.Fatalf("expected one first delivery, got %d", len(firstIDs))
	}

	if len(secondIDs) != 1 {
		t.Fatalf("expected one redelivery, got %d", len(secondIDs))
	}

	if firstIDs[0] != messageID {
		t.Fatalf(
			"expected first SQS delivery to use message ID %s, got %s",
			messageID,
			firstIDs[0],
		)
	}

	if secondIDs[0] != firstIDs[0] {
		t.Fatalf(
			"expected redelivery to keep the same SQS message ID, first=%s second=%s",
			firstIDs[0],
			secondIDs[0],
		)
	}

	if err := pool.QueryRow(
		ctx,
		`SELECT balance_cents FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balanceCents); err != nil {
		t.Fatal(err)
	}

	if balanceCents != 7500 {
		t.Fatalf(
			"redelivery changed wallet balance: expected 75.00, got %d cents",
			balanceCents,
		)
	}

	var transactionCount int
	if err := pool.QueryRow(
		ctx,
		`
        SELECT COUNT(*)
        FROM wager_transactions
        WHERE provider_id = $1
          AND external_transaction_id = $2
        `,
		providerID,
		externalTransactionID,
	).Scan(&transactionCount); err != nil {
		t.Fatal(err)
	}

	if transactionCount != 1 {
		t.Fatalf(
			"expected one wager transaction after redelivery, got %d",
			transactionCount,
		)
	}

	if err := pool.QueryRow(
		ctx,
		`
        SELECT COUNT(*)
        FROM wallet_ledger_entries
        WHERE wallet_id = $1
          AND transaction_id = $2
        `,
		walletID,
		transactionID,
	).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}

	if ledgerCount != 1 {
		t.Fatalf(
			"expected one ledger entry after redelivery, got %d",
			ledgerCount,
		)
	}

	var inboxCount int
	if err := pool.QueryRow(
		ctx,
		`
        SELECT COUNT(*)
        FROM inbox_messages
        WHERE consumer_name = $1
          AND message_id = $2
        `,
		"transaction-consumer",
		messageID,
	).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}

	if inboxCount != 1 {
		t.Fatalf("expected one Inbox row after redelivery, got %d", inboxCount)
	}

	var outboxCount int
	if err := pool.QueryRow(
		ctx,
		`
        SELECT COUNT(*)
        FROM outbox_events
        WHERE correlation_id = $1
        `,
		transactionID,
	).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}

	if outboxCount != 2 {
		t.Fatalf(
			"expected two outbox events for the original financial transaction, got %d",
			outboxCount,
		)
	}
}

type blockingDeleteReceiver struct {
	inner         ports.QueueReceiver
	deleteStarted chan struct{}
	startOnce     sync.Once

	mu         sync.Mutex
	messageIDs []string
}

func (r *blockingDeleteReceiver) Receive(
	ctx context.Context,
	batchSize int,
	waitTimeSeconds int,
	visibilityTimeoutSeconds int,
) ([]ports.QueueMessage, error) {
	messages, err := r.inner.Receive(
		ctx,
		batchSize,
		waitTimeSeconds,
		visibilityTimeoutSeconds,
	)

	if err == nil {
		r.mu.Lock()
		for _, message := range messages {
			r.messageIDs = append(r.messageIDs, message.MessageID)
		}
		r.mu.Unlock()
	}

	return messages, err
}

func (r *blockingDeleteReceiver) Delete(
	ctx context.Context,
	receiptHandle string,
) error {
	r.startOnce.Do(func() {
		close(r.deleteStarted)
	})

	<-ctx.Done()

	return ctx.Err()
}

func (r *blockingDeleteReceiver) receivedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.messageIDs...)
}

type observingDeleteReceiver struct {
	inner           ports.QueueReceiver
	deleteSuccess   chan struct{}
	messageReceived chan struct{}
	receiveOnce     sync.Once
	deleteOnce      sync.Once

	mu         sync.Mutex
	messageIDs []string
}

func (r *observingDeleteReceiver) Receive(
	ctx context.Context,
	batchSize int,
	waitTimeSeconds int,
	visibilityTimeoutSeconds int,
) ([]ports.QueueMessage, error) {
	messages, err := r.inner.Receive(
		ctx,
		batchSize,
		waitTimeSeconds,
		visibilityTimeoutSeconds,
	)

	if err == nil && len(messages) > 0 {
		r.mu.Lock()
		for _, message := range messages {
			r.messageIDs = append(r.messageIDs, message.MessageID)
		}
		r.mu.Unlock()

		r.receiveOnce.Do(func() {
			close(r.messageReceived)
		})
	}

	return messages, err
}

func (r *observingDeleteReceiver) Delete(
	ctx context.Context,
	receiptHandle string,
) error {
	if err := r.inner.Delete(ctx, receiptHandle); err != nil {
		return err
	}

	r.deleteOnce.Do(func() {
		close(r.deleteSuccess)
	})

	return nil
}

func (r *observingDeleteReceiver) receivedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.messageIDs...)
}

func integrationRecoveryPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("PostgreSQL unavailable: %v", err)
	}

	var exists bool
	if err := pool.QueryRow(
		ctx,
		`
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'wallets'
        )
        `,
	).Scan(&exists); err != nil {
		pool.Close()
		t.Fatal(err)
	}

	if !exists {
		pool.Close()
		t.Skip("database schema is not initialized")
	}

	t.Cleanup(pool.Close)

	return pool
}

func recoverySQSConfig() config.Config {
	return config.Config{
		AWSRegion:          "us-east-1",
		AWSEndpoint:        "http://localhost:4566",
		AWSAccessKeyID:     "test",
		AWSSecretAccessKey: "test",
		TransactionQueue:   "wager-transactions.fifo",
		TransactionDLQ:     "wager-transactions-dlq.fifo",
		EventQueue:         "wager-events.fifo",
	}
}

func createRecoveryTestQueue(t *testing.T, cfg config.Config) string {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithBaseEndpoint(cfg.AWSEndpoint),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AWSAccessKeyID,
				cfg.AWSSecretAccessKey,
				"",
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := awsSQS.NewFromConfig(awsCfg)
	queueName := "consumer-recovery-" + uuid.NewString() + ".fifo"
	output, err := client.CreateQueue(context.Background(), &awsSQS.CreateQueueInput{
		QueueName: &queueName,
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil || output.QueueUrl == nil {
		t.Fatalf("create consumer recovery queue: %v", err)
	}

	queueURL := *output.QueueUrl
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.DeleteQueue(ctx, &awsSQS.DeleteQueueInput{QueueUrl: &queueURL}); err != nil {
			t.Logf("delete consumer recovery queue: %v", err)
		}
	})

	return queueURL
}

func sendRecoveryMessage(
	t *testing.T,
	cfg config.Config,
	queueURL string,
	body string,
	messageGroupID string,
	messageDeduplicationID string,
) string {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithBaseEndpoint(cfg.AWSEndpoint),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AWSAccessKeyID,
				cfg.AWSSecretAccessKey,
				"",
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := awsSQS.NewFromConfig(awsCfg)

	output, err := client.SendMessage(
		context.Background(),
		&awsSQS.SendMessageInput{
			QueueUrl:               &queueURL,
			MessageBody:            &body,
			MessageGroupId:         &messageGroupID,
			MessageDeduplicationId: &messageDeduplicationID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if output.MessageId == nil || *output.MessageId == "" {
		t.Fatal("SQS did not return a message ID")
	}

	return *output.MessageId
}

func cleanupRecoveryMessage(t *testing.T, queueURL, messageID string) {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithBaseEndpoint("http://localhost:4566"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Logf("cleanup: load AWS config: %v", err)
		return
	}

	client := awsSQS.NewFromConfig(awsCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		output, err := client.ReceiveMessage(ctx, &awsSQS.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     0,
			VisibilityTimeout:   0,
		})
		if err != nil {
			if ctx.Err() != nil {
				t.Logf("cleanup: receive message polling stopped: %v", ctx.Err())
				return
			}
			t.Logf("cleanup: receive message: %v", err)
			select {
			case <-ctx.Done():
				t.Logf("cleanup: message %s was not deleted before timeout", messageID)
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		for _, message := range output.Messages {
			if message.MessageId == nil || *message.MessageId != messageID || message.ReceiptHandle == nil {
				continue
			}
			if _, err := client.DeleteMessage(ctx, &awsSQS.DeleteMessageInput{
				QueueUrl:      &queueURL,
				ReceiptHandle: message.ReceiptHandle,
			}); err != nil {
				t.Logf("cleanup: delete message %s: %v", messageID, err)
				continue
			}
			return
		}

		select {
		case <-ctx.Done():
			t.Logf("cleanup: message %s was not found before timeout; it may already have been deleted", messageID)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
