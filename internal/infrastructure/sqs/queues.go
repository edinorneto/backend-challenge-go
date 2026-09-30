package sqs

import (
	"context"
	"fmt"

	"github.com/edinorneto/backend-challenge-go/internal/config"
)

type QueueURLs struct {
	Transaction    string
	TransactionDLQ string
}

type QueueManager struct {
	client *Client
	config config.Config
}

func NewQueueManager(client *Client, cfg config.Config) *QueueManager {
	return &QueueManager{client: client, config: cfg}
}

func (m *QueueManager) Resolve(ctx context.Context) (QueueURLs, error) {
	transaction, err := m.queueURL(ctx, m.config.TransactionQueue)
	if err != nil {
		return QueueURLs{}, err
	}
	dlq, err := m.queueURL(ctx, m.config.TransactionDLQ)
	if err != nil {
		return QueueURLs{}, err
	}
	return QueueURLs{Transaction: transaction, TransactionDLQ: dlq}, nil
}

func (m *QueueManager) Check(ctx context.Context) error {
	_, err := m.Resolve(ctx)
	return err
}

func (m *QueueManager) queueURL(ctx context.Context, name string) (string, error) {
	output, err := m.client.GetQueueURL(ctx, name)
	if err != nil {
		return "", fmt.Errorf("resolve SQS queue %q: %w", name, err)
	}
	return output, nil
}
