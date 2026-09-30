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
			sqs.NewConsumer,
			database.NewTransactionManager,
			database.NewWalletRepo,
			database.NewOutboxRepo,
			database.NewInboxRepo,
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
			func(receiver *sqs.Consumer, inbox ports.InboxRepository, wagering *application.WageringService, txManager *database.TransactionManager) *application.QueueConsumer {
				return application.NewFinancialQueueConsumer(receiver, inbox, wagering, txManager, application.QueueConsumerConfig{
					Name:                  "transaction-consumer",
					BatchSize:             10,
					WaitTimeSeconds:       20,
					VisibilityTimeoutSecs: 30,
					RetryDelay:            time.Second,
				})
			},
			httpapi.NewServer,
		),

		fx.Invoke(
			func(_ *migrations.Runner) {},
			startMessaging,
			runHTTP,
		),
	).Run()
}

func startMessaging(
	lc fx.Lifecycle,
	manager *sqs.QueueManager,
	sqsPublisher *sqs.Publisher,
	receiver *sqs.Consumer,
	publisher *application.OutboxPublisher,
	consumer *application.QueueConsumer,
) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			urls, err := manager.Resolve(ctx)
			if err != nil {
				return err
			}
			if err := sqsPublisher.ConfigureQueueURL(urls.EventQueue); err != nil {
				return err
			}
			if err := receiver.ConfigureQueueURL(urls.Transaction); err != nil {
				return err
			}
			if err := publisher.Start(ctx); err != nil {
				return err
			}
			if err := consumer.Start(ctx); err != nil {
				_ = publisher.Stop(ctx)
				return err
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := consumer.Stop(ctx); err != nil {
				return err
			}
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
