package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL           string
	HTTPAddr              string
	AWSRegion             string
	AWSEndpoint           string
	AWSAccessKeyID        string
	AWSSecretAccessKey    string
	OIDCIssuerURL         string
	OIDCJWKSURL           string
	OIDCAudience          string
	OIDCProviderClaim     string
	TransactionQueue      string
	TransactionDLQ        string
	EventQueue            string
	MaxReceiveCount       int
	OutboxBatchSize       int
	OutboxPollInterval    time.Duration
	OutboxLeaseDuration   time.Duration
	OutboxRetryBaseDelay  time.Duration
	ReferencePollInterval time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/betting"),
		HTTPAddr:              getEnv("HTTP_ADDR", ":8080"),
		AWSRegion:             getEnv("AWS_REGION", "us-east-1"),
		AWSEndpoint:           getEnv("AWS_ENDPOINT", ""),
		AWSAccessKeyID:        os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		OIDCIssuerURL:         getEnv("OIDC_ISSUER_URL", "http://localhost:8081/realms/backend"),
		OIDCJWKSURL:           getEnv("OIDC_JWKS_URL", "http://localhost:8081/realms/backend/protocol/openid-connect/certs"),
		OIDCAudience:          os.Getenv("OIDC_AUDIENCE"),
		OIDCProviderClaim:     getEnv("OIDC_PROVIDER_CLAIM", "provider_id"),
		TransactionQueue:      getEnv("SQS_TRANSACTION_QUEUE", "wager-transactions.fifo"),
		TransactionDLQ:        getEnv("SQS_TRANSACTION_DLQ", "wager-transactions-dlq.fifo"),
		EventQueue:            getEnv("SQS_EVENT_QUEUE", "wager-events.fifo"),
		OutboxBatchSize:       10,
		OutboxPollInterval:    time.Second,
		OutboxLeaseDuration:   30 * time.Second,
		OutboxRetryBaseDelay:  time.Second,
		ReferencePollInterval: time.Second,
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
	if strings.TrimSpace(cfg.OIDCIssuerURL) == "" || strings.TrimSpace(cfg.OIDCJWKSURL) == "" {
		return Config{}, errors.New("OIDC_ISSUER_URL and OIDC_JWKS_URL must not be empty")
	}
	if strings.TrimSpace(cfg.OIDCProviderClaim) == "" {
		return Config{}, errors.New("OIDC_PROVIDER_CLAIM must not be empty")
	}
	if strings.TrimSpace(cfg.TransactionQueue) == "" || strings.TrimSpace(cfg.TransactionDLQ) == "" || strings.TrimSpace(cfg.EventQueue) == "" {
		return Config{}, errors.New("SQS queue names must not be empty")
	}
	if cfg.TransactionQueue == cfg.TransactionDLQ {
		return Config{}, errors.New("SQS transaction queue and DLQ must differ")
	}
	if cfg.EventQueue == cfg.TransactionQueue || cfg.EventQueue == cfg.TransactionDLQ {
		return Config{}, errors.New("SQS event queue must differ from transaction queue and DLQ")
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
