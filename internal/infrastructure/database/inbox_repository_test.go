package database_test

import (
	"context"
	"errors"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
)

func TestInboxRepositoryDeduplicatesAndCommitsCompletion(t *testing.T) {
	pool := testPool(t)
	repo := database.NewInboxRepo(pool)
	consumer := "inbox-test"
	messageID := "message-" + t.Name()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID)
	})

	effects := 0
	effect := func(_ context.Context, _ []byte) error {
		effects++
		return nil
	}
	duplicate, err := repo.Process(context.Background(), consumer, messageID, []byte(`{"eventId":"1"}`), effect)
	if err != nil || duplicate {
		t.Fatalf("expected first message to process, duplicate=%v err=%v", duplicate, err)
	}
	duplicate, err = repo.Process(context.Background(), consumer, messageID, []byte(`{"eventId":"1"}`), effect)
	if err != nil || !duplicate {
		t.Fatalf("expected second message to be duplicate, duplicate=%v err=%v", duplicate, err)
	}
	if effects != 1 {
		t.Fatalf("expected one effect, got %d", effects)
	}
}

func TestInboxRepositoryRollsBackOnEffectFailure(t *testing.T) {
	pool := testPool(t)
	repo := database.NewInboxRepo(pool)
	consumer := "inbox-test-rollback"
	messageID := "message-" + t.Name()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID)
	})

	_, err := repo.Process(context.Background(), consumer, messageID, []byte(`{"eventId":"2"}`), func(context.Context, []byte) error {
		return errors.New("transient effect error")
	})
	if err == nil {
		t.Fatal("expected effect failure")
	}
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2
	`, consumer, messageID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected failed processing to rollback Inbox row, got %d rows", count)
	}
}
