package sqs

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type Client struct {
	api *sqs.Client
}

func NewClient(cfg config.Config) (*Client, error) {
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AWSRegion),
	}
	if cfg.AWSEndpoint != "" {
		options = append(options, awsconfig.WithBaseEndpoint(cfg.AWSEndpoint))
	}
	if cfg.AWSAccessKeyID != "" || cfg.AWSSecretAccessKey != "" {
		if cfg.AWSAccessKeyID == "" || cfg.AWSSecretAccessKey == "" {
			return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be provided together")
		}
		options = append(options, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return &Client{api: sqs.NewFromConfig(awsCfg)}, nil
}

func (c *Client) GetQueueURL(ctx context.Context, queueName string) (string, error) {
	output, err := c.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		return "", err
	}
	if output.QueueUrl == nil || *output.QueueUrl == "" {
		return "", fmt.Errorf("SQS queue %q returned an empty URL", queueName)
	}
	return *output.QueueUrl, nil
}

type QueueAttributes map[string]string

func (c *Client) GetQueueAttributes(ctx context.Context, queueURL string) (QueueAttributes, error) {
	output, err := c.api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, err
	}

	attributes := make(QueueAttributes, len(output.Attributes))
	for name, value := range output.Attributes {
		attributes[string(name)] = value
	}
	return attributes, nil
}

type Consumer struct {
	client   *Client
	queueURL string
}

func NewConsumer(client *Client) (*Consumer, error) {
	if client == nil {
		return nil, fmt.Errorf("sqs client is required")
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) ConfigureQueueURL(queueURL string) error {
	if c == nil {
		return fmt.Errorf("sqs consumer is not configured")
	}
	if queueURL == "" {
		return fmt.Errorf("sqs queue URL is empty")
	}
	c.queueURL = queueURL
	return nil
}

func (c *Consumer) Receive(ctx context.Context, batchSize int, waitTimeSeconds int, visibilityTimeoutSeconds int) ([]ports.QueueMessage, error) {
	if c == nil || c.client == nil || c.client.api == nil {
		return nil, fmt.Errorf("sqs consumer is not configured")
	}
	if c.queueURL == "" {
		return nil, fmt.Errorf("sqs queue URL is empty")
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > 10 {
		batchSize = 10
	}
	output, err := c.client.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueURL,
		MaxNumberOfMessages: int32(batchSize),
		WaitTimeSeconds:     int32(waitTimeSeconds),
		VisibilityTimeout:   int32(visibilityTimeoutSeconds),
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, fmt.Errorf("receive SQS messages: %w", err)
	}
	messages := make([]ports.QueueMessage, 0, len(output.Messages))
	for _, message := range output.Messages {
		if message.MessageId == nil || message.ReceiptHandle == nil || message.Body == nil {
			continue
		}
		group := ""
		if value, ok := message.Attributes["MessageGroupId"]; ok {
			group = value
		}
		messages = append(messages, ports.QueueMessage{
			MessageID:     *message.MessageId,
			ReceiptHandle: *message.ReceiptHandle,
			Body:          *message.Body,
			MessageGroup:  group,
		})
	}
	return messages, nil
}

func (c *Consumer) Delete(ctx context.Context, receiptHandle string) error {
	if c == nil || c.client == nil || c.client.api == nil {
		return fmt.Errorf("sqs consumer is not configured")
	}
	if c.queueURL == "" || receiptHandle == "" {
		return fmt.Errorf("sqs queue URL and receipt handle are required")
	}
	_, err := c.client.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		return fmt.Errorf("delete SQS message: %w", err)
	}
	return nil
}

var _ ports.QueueReceiver = (*Consumer)(nil)
