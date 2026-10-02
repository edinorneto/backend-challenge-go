//go:build integration

package sqs

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/config"
)

// Evidence for ARCHITECTURE section 10 ("Provider identity on SQS"): LocalStack
// fills SenderId with the account derived from the unverified access key, so two
// different producer credentials in one account get the same SenderId. It cannot
// bind data.providerId to the sender locally; the binding stays with the broker
// policies.
func TestLocalStackSenderIdDoesNotIdentifyTheProducer(t *testing.T) {
	ctx := context.Background()
	clientFor := func(accessKey string) *Client {
		client, err := NewClient(config.Config{
			AWSRegion:          envOr("AWS_REGION", "us-east-1"),
			AWSEndpoint:        envOr("AWS_ENDPOINT", "http://localhost:4566"),
			AWSAccessKeyID:     accessKey,
			AWSSecretAccessKey: "test",
		})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	owner := clientFor(envOr("AWS_ACCESS_KEY_ID", "test"))
	created, err := owner.api.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sender-id-probe-" + uuid.NewString()[:8])})
	if err != nil {
		t.Skipf("LocalStack unavailable: %v", err)
	}
	t.Cleanup(func() {
		_, _ = owner.api.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: created.QueueUrl})
	})

	for _, producer := range []string{"provider-a-producer", "provider-b-producer"} {
		if _, err := clientFor(producer).api.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String(producer)}); err != nil {
			t.Fatal(err)
		}
	}

	senders := map[string]string{}
	for attempt := 0; attempt < 5 && len(senders) < 2; attempt++ {
		output, err := owner.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    created.QueueUrl,
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameSenderId},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range output.Messages {
			senders[aws.ToString(message.Body)] = message.Attributes[string(types.MessageSystemAttributeNameSenderId)]
		}
	}
	if len(senders) != 2 {
		t.Fatalf("expected both probe messages, got %v", senders)
	}
	if senders["provider-a-producer"] == "" || senders["provider-a-producer"] != senders["provider-b-producer"] {
		t.Fatalf("LocalStack now tells producers apart by SenderId (%v): the SQS provider binding can be revisited", senders)
	}
}
