package application

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

var ErrReferenceWorkerNotConfigured = errors.New("reference worker is not configured")

type ReferenceWorker struct {
	repo    ports.PendingReferenceRepository
	cfg     config.Config
	logger  *observability.Logger
	metrics *observability.Metrics

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewReferenceWorker(repo ports.PendingReferenceRepository, cfg config.Config, options ...any) *ReferenceWorker {
	worker := &ReferenceWorker{repo: repo, cfg: cfg}
	for _, option := range options {
		switch value := option.(type) {
		case *observability.Logger:
			worker.logger = value
		case *observability.Metrics:
			worker.metrics = value
		}
	}
	return worker
}

func (w *ReferenceWorker) Start(ctx context.Context) error {
	if w == nil || w.repo == nil {
		return ErrReferenceWorkerNotConfigured
	}

	w.mu.Lock()
	if w.cancel != nil {
		w.mu.Unlock()
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.wg.Add(1)
	w.mu.Unlock()

	go w.loop(child)
	return nil
}

func (w *ReferenceWorker) Stop(ctx context.Context) error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *ReferenceWorker) loop(ctx context.Context) {
	defer w.wg.Done()

	interval := w.cfg.ReferencePollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		processed, err := w.repo.ProcessNextPendingReference(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			if w.logger != nil {
				w.logger.Error(ctx, "reference_worker_failed", err, nil)
			} else {
				log.Printf("reference worker: %v", err)
			}
			if w.metrics != nil {
				w.metrics.Inc("reference_worker_failures_total")
			}
		}
		if processed {
			if w.metrics != nil {
				w.metrics.Inc("reference_worker_processed_total")
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
