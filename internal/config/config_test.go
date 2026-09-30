package config

import "testing"

func TestLoadReadsSQSConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ENDPOINT", "http://localhost:4566")
	t.Setenv("SQS_TRANSACTION_QUEUE", "transactions.fifo")
	t.Setenv("SQS_TRANSACTION_DLQ", "transactions-dlq.fifo")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AWSRegion != "eu-west-1" || cfg.TransactionQueue != "transactions.fifo" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.MaxReceiveCount != 5 {
		t.Fatalf("expected max receive count 5, got %d", cfg.MaxReceiveCount)
	}
}

func TestLoadRejectsInvalidSQSConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("SQS_TRANSACTION_QUEUE", "same.fifo")
	t.Setenv("SQS_TRANSACTION_DLQ", "same.fifo")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid configuration error")
	}
}

func TestLoadRejectsInvalidMaxReceiveCount(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "0")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid max receive count error")
	}
}
