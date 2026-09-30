package sqs

import (
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/config"
)

func TestNewClient(t *testing.T) {
	client, err := NewClient(config.Config{
		AWSRegion:          "us-east-1",
		AWSEndpoint:        "http://localhost:4566",
		AWSAccessKeyID:     "test",
		AWSSecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("expected SQS client")
	}
}
