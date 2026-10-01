package application

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

var ErrReferenceWorkerNotConfigured = errors.New("reference worker is not configured")

type ReferenceWorker struct {
	repo ports.PendingReferenceRepository
	cfg  config.Config

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewReferenceWorker(repo ports.PendingReferenceRepository, cfg config.Config) *ReferenceWorker {
	return &ReferenceWorker{repo: repo, cfg: cfg}
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
			log.Printf("reference worker: %v", err)
		}
		if processed {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
