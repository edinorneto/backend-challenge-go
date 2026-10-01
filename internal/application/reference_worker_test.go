package application

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edinorneto/backend-challenge-go/internal/config"
)

type fakePendingReferenceRepository struct {
	mu        sync.Mutex
	processed int
	ready     chan struct{}
	calls     chan struct{}
}

func (r *fakePendingReferenceRepository) ProcessNextPendingReference(context.Context) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls != nil {
		select {
		case r.calls <- struct{}{}:
		default:
		}
	}
	if r.processed == 0 {
		r.processed++
		close(r.ready)
		return true, nil
	}
	return false, nil
}

func TestReferenceWorkerProcessesPersistedPendingReference(t *testing.T) {
	repo := &fakePendingReferenceRepository{ready: make(chan struct{})}
	worker := NewReferenceWorker(repo, config.Config{ReferencePollInterval: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-repo.ready:
	case <-time.After(time.Second):
		t.Fatal("reference worker did not process the pending reference")
	}
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.processed != 1 {
		t.Fatalf("expected one pending reference processing, got %d", repo.processed)
	}
}

func TestReferenceWorkerStopsOnCancellation(t *testing.T) {
	repo := &fakePendingReferenceRepository{ready: make(chan struct{})}
	worker := NewReferenceWorker(repo, config.Config{ReferencePollInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())

	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReferenceWorkerOutlivesStartupContext(t *testing.T) {
	repo := &fakePendingReferenceRepository{
		ready: make(chan struct{}),
		calls: make(chan struct{}, 2),
	}
	worker := NewReferenceWorker(repo, config.Config{ReferencePollInterval: time.Millisecond})
	startupCtx, cancelStartup := context.WithCancel(context.Background())

	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancelStartup()
	select {
	case <-startupCtx.Done():
	default:
		t.Fatal("startup context was not canceled")
	}

	for i := 0; i < 2; i++ {
		select {
		case <-repo.calls:
		case <-time.After(time.Second):
			_ = worker.Stop(context.Background())
			t.Fatal("reference worker stopped with the startup context")
		}
	}

	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
