package application

import (
	"context"
	"testing"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

func TestOutboxPublisherFxComposition(t *testing.T) {
	err := fx.ValidateApp(
		fx.Provide(
			func() ports.OutboxRepository {
				return &fakeOutboxRepo{}
			},
			func() ports.OutboxMessagePublisher {
				return &fakePublisher{}
			},
			func() config.Config {
				return config.Config{}
			},
			NewOutboxPublisher,
		),
		fx.Invoke(func(*OutboxPublisher) {}),
	)
	if err != nil {
		t.Fatalf("expected OutboxPublisher to resolve through Fx: %v", err)
	}
}

func TestOutboxPublisherStopsWithoutPendingWork(t *testing.T) {
	worker := NewOutboxPublisher(
		&fakeOutboxRepo{events: map[uuid.UUID]ports.OutboxEvent{}},
		&fakePublisher{},
		config.Config{OutboxPollInterval: time.Millisecond},
	)
	ctx, cancel := context.WithCancel(context.Background())
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
