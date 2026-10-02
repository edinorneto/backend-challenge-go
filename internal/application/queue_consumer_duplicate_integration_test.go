package application

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	awsSQSTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	sqsInfra "github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/messaging"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

// The same command is sent twice with different MessageDeduplicationIds, so
// LocalStack delivers both copies. The application must receive both and apply
// the financial effect once: the duplicate is stopped by the Inbox, not by SQS.
func TestFinancialQueueConsumerAppliesRepeatedDeliveryOnce(t *testing.T) {
	pool, cleanup := isolatedOutboxPool(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	cfg := recoverySQSConfig()
	client := integrationSQSClient(t, cfg)
	queueURL := createFIFOQueueForTest(t, client, "consumer-duplicate-"+uuid.NewString()+".fifo", map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
	})
	t.Cleanup(func() { cleanupTestQueue(t, client, queueURL) })

	repo := database.NewWalletRepo(pool)
	playerID, walletID := createWorkerTestWallet(t, repo, "100.00")
	body := commandBody(t, "duplicate-message-"+uuid.NewString(), "duplicate-bet", "provider-a:duplicate-bet", playerID, walletID, "25.00")
	for _, deduplicationID := range []string{"copy-1-" + uuid.NewString(), "copy-2-" + uuid.NewString()} {
		sendCommand(t, client, queueURL, body, walletID.String(), deduplicationID)
	}

	metrics := observability.NewMetrics()
	consumer := startTestConsumer(t, cfg, queueURL, pool, metrics, 5)
	waitForMetric(t, metrics, "wager_processing_total", 2, 15*time.Second)
	stopTestConsumer(t, consumer)

	if got := metrics.Snapshot("inbox_duplicates_total"); got != 1 {
		t.Fatalf("expected the second delivery to be detected by the Inbox, got %d duplicates", got)
	}
	var transactions, debits, balance int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'),
		       (SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'),
		       (SELECT balance_cents FROM wallets WHERE id = $1)
	`, walletID).Scan(&transactions, &debits, &balance); err != nil {
		t.Fatal(err)
	}
	if transactions != 1 || debits != 1 || balance != 7500 {
		t.Fatalf("repeated delivery was applied more than once: transactions=%d debits=%d balance=%d", transactions, debits, balance)
	}
	assertQueueEmpty(t, client, queueURL)
}

// A command reusing an idempotency key with a different payload is a permanent
// error: it has no financial effect and reaches the DLQ after the receive limit.
func TestFinancialQueueConsumerSendsIdempotencyConflictToDLQ(t *testing.T) {
	pool, cleanup := isolatedOutboxPool(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	cfg := recoverySQSConfig()
	client := integrationSQSClient(t, cfg)
	dlqName := "consumer-conflict-dlq-" + uuid.NewString() + ".fifo"
	dlqURL := createFIFOQueueForTest(t, client, dlqName, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"})
	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": fmt.Sprintf("arn:aws:sqs:%s:000000000000:%s", cfg.AWSRegion, dlqName),
		"maxReceiveCount":     "2",
	})
	queueURL := createFIFOQueueForTest(t, client, "consumer-conflict-"+uuid.NewString()+".fifo", map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "RedrivePolicy": string(redrive),
	})
	t.Cleanup(func() {
		cleanupTestQueue(t, client, queueURL)
		cleanupTestQueue(t, client, dlqURL)
	})

	repo := database.NewWalletRepo(pool)
	playerID, walletID := createWorkerTestWallet(t, repo, "100.00")
	original := commandBody(t, "conflict-original-"+uuid.NewString(), "conflict-bet", "provider-a:conflict-bet", playerID, walletID, "25.00")
	conflicting := commandBody(t, "conflict-changed-"+uuid.NewString(), "conflict-bet", "provider-a:conflict-bet", playerID, walletID, "30.00")
	sendCommand(t, client, queueURL, original, walletID.String(), uuid.NewString())
	sendCommand(t, client, queueURL, conflicting, walletID.String(), uuid.NewString())

	metrics := observability.NewMetrics()
	consumer := startTestConsumer(t, cfg, queueURL, pool, metrics, 2)
	dlqMessage := waitForDLQMessage(t, client, dlqURL, 20*time.Second)
	stopTestConsumer(t, consumer)

	if dlqMessage == nil || dlqMessage.Body == nil || *dlqMessage.Body != conflicting {
		t.Fatalf("expected the conflicting command in the DLQ, got %#v", dlqMessage)
	}
	var debits, balance int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'),
		       (SELECT balance_cents FROM wallets WHERE id = $1)
	`, walletID).Scan(&debits, &balance); err != nil {
		t.Fatal(err)
	}
	if debits != 1 || balance != 7500 {
		t.Fatalf("the conflicting command must have no effect: debits=%d balance=%d", debits, balance)
	}
}

func commandBody(t *testing.T, messageID, externalID, idempotencyKey string, playerID, walletID uuid.UUID, amount string) string {
	t.Helper()
	body, err := json.Marshal(messaging.WagerTransactionRequested{
		MessageID:  messageID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC(),
		Data: messaging.WagerTransactionData{
			ProviderID:            "provider-a",
			ExternalTransactionID: externalID,
			IdempotencyKey:        idempotencyKey,
			PlayerID:              playerID,
			WalletID:              walletID,
			RoundID:               "round-sqs",
			GameID:                "game-sqs",
			Kind:                  "BET",
			Money:                 messaging.MoneyData{Amount: amount, Currency: "BRL"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func sendCommand(t *testing.T, client *awsSQS.Client, queueURL, body, groupID, deduplicationID string) {
	t.Helper()
	if _, err := client.SendMessage(context.Background(), &awsSQS.SendMessageInput{
		QueueUrl:               &queueURL,
		MessageBody:            &body,
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &deduplicationID,
	}); err != nil {
		t.Fatal(err)
	}
}

func startTestConsumer(t *testing.T, cfg config.Config, queueURL string, pool *pgxpool.Pool, metrics *observability.Metrics, maxReceiveCount int) *QueueConsumer {
	t.Helper()
	sqsClient, err := sqsInfra.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := sqsInfra.NewConsumer(sqsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.ConfigureQueueURL(queueURL); err != nil {
		t.Fatal(err)
	}
	consumer := NewFinancialQueueConsumer(
		receiver,
		database.NewInboxRepo(pool),
		NewWageringService(database.NewWalletRepo(pool)),
		database.NewTransactionManager(pool),
		QueueConsumerConfig{
			Name:                  "transaction-consumer",
			BatchSize:             10,
			WaitTimeSeconds:       1,
			VisibilityTimeoutSecs: 1,
			RetryDelay:            time.Second,
			RetryMaxDelay:         time.Second,
			MaxReceiveCount:       maxReceiveCount,
		},
		metrics,
	)
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopTestConsumer(t, consumer) })
	return consumer
}

func stopTestConsumer(t *testing.T, consumer *QueueConsumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.Stop(ctx); err != nil {
		t.Errorf("stop consumer: %v", err)
	}
}

func waitForMetric(t *testing.T, metrics *observability.Metrics, name string, expected uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if metrics.Snapshot(name) >= expected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s >= %d, got %d", name, expected, metrics.Snapshot(name))
}

func assertQueueEmpty(t *testing.T, client *awsSQS.Client, queueURL string) {
	t.Helper()
	output, err := client.GetQueueAttributes(context.Background(), &awsSQS.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []awsSQSTypes.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range output.Attributes {
		if value != "0" {
			t.Fatalf("expected every delivery to be deleted after commit, %s=%s", name, value)
		}
	}
}
