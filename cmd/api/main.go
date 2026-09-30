package main

import (
	"context"
	"net/http"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/httpapi"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/ports"

	"go.uber.org/fx"
)

func main() {
	fx.New(
		fx.Provide(
			config.Load,
			database.NewPool,
			migrations.NewRunner,
			sqs.NewClient,
			sqs.NewQueueManager,
			database.NewWalletRepo,
			database.NewOutboxRepo,
			func(repo *database.WalletRepo) ports.WalletRepository {
				return repo
			},
			func(repo *database.WalletRepo) ports.WageringRepository {
				return repo
			},
			func(repo *database.OutboxRepo) ports.OutboxRepository {
				return repo
			},
			sqs.NewPublisher,

			application.NewWalletService,
			application.NewWageringService,
			application.NewOutboxPublisher,
			httpapi.NewServer,
		),

		fx.Invoke(
			func(_ *migrations.Runner) {},
			startOutboxPublisher,
			runHTTP,
		),
	).Run()
}

func startOutboxPublisher(
	lc fx.Lifecycle,
	manager *sqs.QueueManager,
	sqsPublisher *sqs.Publisher,
	publisher *application.OutboxPublisher,
) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			urls, err := manager.Resolve(ctx)
			if err != nil {
				return err
			}
			if err := sqsPublisher.ConfigureQueueURL(urls.Transaction); err != nil {
				return err
			}
			return publisher.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return publisher.Stop(ctx)
		},
	})
}

func runHTTP(
	lc fx.Lifecycle,
	cfg config.Config,
	server *httpapi.Server,
) {
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				_ = httpServer.ListenAndServe()
			}()

			return nil
		},

		OnStop: func(ctx context.Context) error {
			return httpServer.Shutdown(ctx)
		},
	})
}
