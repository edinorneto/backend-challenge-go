package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type Publisher struct {
	client   *Client
	queueURL string
}

func NewPublisher(client *Client) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("sqs client is required")
	}
	return &Publisher{client: client}, nil
}

func (p *Publisher) ConfigureQueueURL(queueURL string) error {
	if p == nil {
		return errors.New("sqs publisher is not configured")
	}
	if queueURL == "" {
		return errors.New("sqs queue URL is empty")
	}
	p.queueURL = queueURL
	return nil
}

func (p *Publisher) Publish(ctx context.Context, message ports.OutboxMessage) error {
	if p == nil || p.client == nil {
		return errors.New("sqs publisher is not configured")
	}
	if p.queueURL == "" {
		return errors.New("sqs queue URL is empty")
	}
	return p.client.Publish(ctx, p.queueURL, message)
}

func (c *Client) Publish(ctx context.Context, queueURL string, message ports.OutboxMessage) error {
	if c == nil || c.api == nil {
		return errors.New("sqs client is not configured")
	}
	if queueURL == "" {
		return errors.New("sqs queue URL is empty")
	}

	input := &sqs.SendMessageInput{
		QueueUrl:       &queueURL,
		MessageBody:    &message.Body,
		MessageGroupId: nil,
	}
	if message.MessageGroupID != "" {
		input.MessageGroupId = &message.MessageGroupID
	}
	if message.MessageDeduplicationID != "" {
		input.MessageDeduplicationId = &message.MessageDeduplicationID
	}
	_, err := c.api.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("publish to SQS queue: %w", err)
	}
	return nil
}

var _ ports.OutboxMessagePublisher = (*Publisher)(nil)
