package application

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	sqsInfra "github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

func TestFinancialQueueConsumerRetriesPoisonMessageToDLQ(t *testing.T) {
	pool := integrationRecoveryPool(t)
	cfg := recoverySQSConfig()
	client := integrationSQSClient(t, cfg)
	ctx := context.Background()

	dlqName := "consumer-dlq-" + uuid.NewString() + ".fifo"
	queueName := "consumer-poison-" + uuid.NewString() + ".fifo"
	dlqURL := createFIFOQueueForTest(t, client, dlqName, map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
	})
	dlqARN := fmt.Sprintf("arn:aws:sqs:%s:000000000000:%s", cfg.AWSRegion, dlqName)
	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     "2",
	})
	queueURL := createFIFOQueueForTest(t, client, queueName, map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
		"RedrivePolicy":             string(redrive),
	})
	t.Cleanup(func() {
		cleanupTestQueue(t, client, queueURL)
		cleanupTestQueue(t, client, dlqURL)
	})

	poisonID := "poison-" + uuid.NewString()
	groupID := "poison-group"
	if _, err := client.SendMessage(ctx, &awsSQS.SendMessageInput{
		QueueUrl:               &queueURL,
		MessageBody:            awsString("{not-json"),
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &poisonID,
	}); err != nil {
		t.Fatal(err)
	}

	// Build the real adapter so the test exercises ReceiveCount extraction and
	// ChangeMessageVisibility against LocalStack.
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

	inbox := database.NewInboxRepo(pool)
	walletRepo := database.NewWalletRepo(pool)
	metrics := observability.NewMetrics()
	consumer := NewFinancialQueueConsumer(
		receiver,
		inbox,
		NewWageringService(walletRepo),
		database.NewTransactionManager(pool),
		QueueConsumerConfig{
			Name:                  "poison-consumer-" + uuid.NewString(),
			BatchSize:             1,
			WaitTimeSeconds:       1,
			VisibilityTimeoutSecs: 1,
			RetryDelay:            time.Second,
			RetryMaxDelay:         time.Second,
			MaxReceiveCount:       2,
		},
		metrics,
	)

	runCtx, cancel := context.WithCancel(ctx)
	if err := consumer.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	dlqMessage := waitForDLQMessage(t, client, dlqURL, 15*time.Second)
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := consumer.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if dlqMessage == nil || dlqMessage.Body == nil || *dlqMessage.Body != "{not-json" {
		t.Fatalf("expected poison message in DLQ, got %#v", dlqMessage)
	}

	var inboxCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM inbox_messages WHERE consumer_name = $1
	`, consumer.cfg.Name).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != 0 {
		t.Fatalf("expected poison message to have no Inbox effect, got %d rows", inboxCount)
	}
	if metrics.Snapshot("sqs_messages_dlq_eligible_total") == 0 {
		t.Fatal("expected exhausted receive attempt metric")
	}
}

func integrationSQSClient(t *testing.T, cfg config.Config) *awsSQS.Client {
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
		t.Skipf("LocalStack unavailable: %v", err)
	}
	return awsSQS.NewFromConfig(awsCfg)
}

func createFIFOQueueForTest(t *testing.T, client *awsSQS.Client, name string, attributes map[string]string) string {
	t.Helper()
	output, err := client.CreateQueue(context.Background(), &awsSQS.CreateQueueInput{
		QueueName:  &name,
		Attributes: attributes,
	})
	if err != nil || output.QueueUrl == nil {
		t.Fatalf("create queue %s: %v", name, err)
	}
	return *output.QueueUrl
}

func cleanupTestQueue(t *testing.T, client *awsSQS.Client, queueURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.DeleteQueue(ctx, &awsSQS.DeleteQueueInput{QueueUrl: &queueURL}); err != nil {
		t.Logf("delete queue %s: %v", queueURL, err)
	}
}

func waitForDLQMessage(t *testing.T, client *awsSQS.Client, queueURL string, timeout time.Duration) *awsSQSMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		output, err := client.ReceiveMessage(ctx, &awsSQS.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
			VisibilityTimeout:   1,
		})
		cancel()
		if err == nil && len(output.Messages) > 0 {
			message := output.Messages[0]
			return &awsSQSMessage{Body: message.Body}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for DLQ message")
	return nil
}

type awsSQSMessage struct {
	Body *string
}

func awsString(value string) *string {
	return &value
}
