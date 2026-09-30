package sqs

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/edinorneto/backend-challenge-go/internal/config"
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
