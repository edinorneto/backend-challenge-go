package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL        string
	HTTPAddr           string
	AWSRegion          string
	AWSEndpoint        string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	TransactionQueue   string
	TransactionDLQ     string
	MaxReceiveCount    int
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/betting"),
		HTTPAddr:           getEnv("HTTP_ADDR", ":8080"),
		AWSRegion:          getEnv("AWS_REGION", "us-east-1"),
		AWSEndpoint:        getEnv("AWS_ENDPOINT", ""),
		AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		TransactionQueue:   getEnv("SQS_TRANSACTION_QUEUE", "wager-transactions.fifo"),
		TransactionDLQ:     getEnv("SQS_TRANSACTION_DLQ", "wager-transactions-dlq.fifo"),
	}
	maxReceiveCount, err := strconv.Atoi(getEnv("SQS_MAX_RECEIVE_COUNT", "5"))
	if err != nil || maxReceiveCount <= 0 {
		return Config{}, errors.New("SQS_MAX_RECEIVE_COUNT must be a positive integer")
	}
	cfg.MaxReceiveCount = maxReceiveCount

	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return Config{}, errors.New("DATABASE_URL must not be empty")
	}
	if strings.TrimSpace(cfg.AWSRegion) == "" {
		return Config{}, errors.New("AWS_REGION must not be empty")
	}
	if strings.TrimSpace(cfg.TransactionQueue) == "" || strings.TrimSpace(cfg.TransactionDLQ) == "" {
		return Config{}, errors.New("SQS queue names must not be empty")
	}
	if cfg.TransactionQueue == cfg.TransactionDLQ {
		return Config{}, errors.New("SQS transaction queue and DLQ must differ")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
