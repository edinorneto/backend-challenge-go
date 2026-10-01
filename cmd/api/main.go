package main

import (
	"context"
	"net/http"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/httpapi"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"

	"go.uber.org/fx"
)

func main() {
	fx.New(
		fx.Provide(
			config.Load,
			observability.NewLogger,
			observability.NewMetrics,
			auth.NewVerifier,
			auth.NewMiddleware,
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
			func(repo *database.InboxRepo) ports.InboxRepository {
				return repo
			},
			sqs.NewPublisher,
			func(publisher *sqs.Publisher) ports.OutboxMessagePublisher {
				return publisher
			},
			func(repo ports.WalletRepository, metrics *observability.Metrics) *application.WalletService {
				return application.NewWalletService(repo, metrics)
			},
			application.NewWageringService,
			func(repo ports.OutboxRepository, publisher ports.OutboxMessagePublisher, cfg config.Config, logger *observability.Logger, metrics *observability.Metrics) *application.OutboxPublisher {
				return application.NewOutboxPublisher(repo, publisher, cfg, logger, metrics)
			},
			func(repo *database.WalletRepo, cfg config.Config, logger *observability.Logger, metrics *observability.Metrics) *application.ReferenceWorker {
				return application.NewReferenceWorker(repo, cfg, logger, metrics)
			},
			func(receiver *sqs.Consumer, inbox ports.InboxRepository, wagering *application.WageringService, txManager *database.TransactionManager, cfg config.Config, logger *observability.Logger, metrics *observability.Metrics) *application.QueueConsumer {
				return application.NewFinancialQueueConsumer(receiver, inbox, wagering, txManager, application.QueueConsumerConfig{
					Name:                  "transaction-consumer",
					BatchSize:             10,
					WaitTimeSeconds:       20,
					VisibilityTimeoutSecs: 30,
					RetryDelay:            time.Second,
					RetryMaxDelay:         30 * time.Second,
					MaxReceiveCount:       cfg.MaxReceiveCount,
				}, logger, metrics)
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
	referenceWorker *application.ReferenceWorker,
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
			if err := publisher.Start(context.Background()); err != nil {
				return err
			}
			if err := referenceWorker.Start(context.Background()); err != nil {
				_ = publisher.Stop(ctx)
				return err
			}
			if err := consumer.Start(context.Background()); err != nil {
				_ = referenceWorker.Stop(ctx)
				_ = publisher.Stop(ctx)
				return err
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			var stopErr error
			if err := consumer.Stop(ctx); err != nil {
				stopErr = err
			}
			if err := referenceWorker.Stop(ctx); err != nil {
				if stopErr == nil {
					stopErr = err
				}
			}
			if err := publisher.Stop(ctx); err != nil && stopErr == nil {
				stopErr = err
			}
			return stopErr
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
