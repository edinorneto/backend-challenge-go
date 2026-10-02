//go:build integration

package sqs

import (
	"context"
	"encoding/json"
	"fmt"
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
		EventQueue:         envOr("SQS_EVENT_QUEUE", "wager-events.fifo"),
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

	// Access policies (localstack/policies): who may send to and consume from each
	// queue. LocalStack stores them without enforcing IAM.
	for queueURL, expected := range map[string]map[string]string{
		urls.Transaction: {
			"role/wagering-provider-producer": "sqs:SendMessage",
			"role/backend-application":        "sqs:ReceiveMessage",
		},
		urls.TransactionDLQ: {
			"sqs.amazonaws.com":     "sqs:SendMessage",
			"role/backend-operator": "sqs:StartMessageMoveTask",
		},
		urls.EventQueue: {
			"role/backend-application":   "sqs:SendMessage",
			"role/wager-events-consumer": "sqs:ReceiveMessage",
		},
	} {
		assertQueuePolicy(t, client, queueURL, expected)
	}
}

// assertQueuePolicy checks that each principal is allowed the given action and
// that providers are never allowed to consume.
func assertQueuePolicy(t *testing.T, client *Client, queueURL string, expected map[string]string) {
	t.Helper()
	attributes, err := client.GetQueueAttributes(context.Background(), queueURL)
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Statement []struct {
			Effect    string          `json:"Effect"`
			Principal map[string]any  `json:"Principal"`
			Action    json.RawMessage `json:"Action"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(attributes["Policy"]), &policy); err != nil {
		t.Fatalf("queue %s has no valid access policy: %v (%q)", queueURL, err, attributes["Policy"])
	}
	allowed := map[string]string{}
	for _, statement := range policy.Statement {
		if statement.Effect != "Allow" {
			continue
		}
		for _, principal := range statement.Principal {
			allowed[fmt.Sprint(principal)] += string(statement.Action)
		}
	}
	for principal, action := range expected {
		found := false
		for name, actions := range allowed {
			if strings.HasSuffix(name, principal) && strings.Contains(actions, action) {
				found = true
			}
		}
		if !found {
			t.Fatalf("queue %s policy does not allow %s for %s: %v", queueURL, action, principal, allowed)
		}
	}
	for name, actions := range allowed {
		if strings.HasSuffix(name, "role/wagering-provider-producer") && strings.Contains(actions, "ReceiveMessage") {
			t.Fatalf("providers must not consume from %s", queueURL)
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
