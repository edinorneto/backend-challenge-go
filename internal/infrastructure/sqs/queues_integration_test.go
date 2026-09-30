//go:build integration

package sqs

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/config"
)

func TestLocalStackTransactionQueues(t *testing.T) {
	maxReceiveCount, err := strconv.Atoi(envOr("SQS_MAX_RECEIVE_COUNT", "5"))
	if err != nil {
		t.Fatalf("parse max receive count: %v", err)
	}
	cfg := config.Config{
		AWSRegion:          envOr("AWS_REGION", "us-east-1"),
		AWSEndpoint:        envOr("AWS_ENDPOINT", "http://localhost:4566"),
		AWSAccessKeyID:     envOr("AWS_ACCESS_KEY_ID", "test"),
		AWSSecretAccessKey: envOr("AWS_SECRET_ACCESS_KEY", "test"),
		TransactionQueue:   envOr("SQS_TRANSACTION_QUEUE", "wager-transactions.fifo"),
		TransactionDLQ:     envOr("SQS_TRANSACTION_DLQ", "wager-transactions-dlq.fifo"),
		MaxReceiveCount:    maxReceiveCount,
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewQueueManager(client, cfg)
	urls, err := manager.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for name, queueURL := range map[string]string{
		"transaction": urls.Transaction,
		"DLQ":         urls.TransactionDLQ,
	} {
		output, err := client.GetQueueAttributes(context.Background(), queueURL)
		if err != nil {
			t.Fatalf("get %s attributes: %v", name, err)
		}
		if output["FifoQueue"] != "true" {
			t.Fatalf("%s queue is not FIFO", name)
		}
	}

	output, err := client.GetQueueAttributes(context.Background(), urls.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	var redrive struct {
		DeadLetterTargetARN string `json:"deadLetterTargetArn"`
		MaxReceiveCount     string `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(output["RedrivePolicy"]), &redrive); err != nil {
		t.Fatal(err)
	}
	if redrive.DeadLetterTargetARN == "" || redrive.MaxReceiveCount == "" {
		t.Fatalf("invalid redrive policy: %s", output["RedrivePolicy"])
	}
	if redrive.MaxReceiveCount != strconv.Itoa(cfg.MaxReceiveCount) {
		t.Fatalf("expected max receive count %d, got %s", cfg.MaxReceiveCount, redrive.MaxReceiveCount)
	}
	if !strings.HasSuffix(redrive.DeadLetterTargetARN, cfg.TransactionDLQ) {
		t.Fatalf("expected redrive DLQ ARN to end with %q, got %q", cfg.TransactionDLQ, redrive.DeadLetterTargetARN)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
