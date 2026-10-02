package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type InboxRepo struct {
	DB *pgxpool.Pool
}

func NewInboxRepo(db *pgxpool.Pool) *InboxRepo {
	return &InboxRepo{DB: db}
}

func (r *InboxRepo) Process(
	ctx context.Context,
	consumerName string,
	messageID string,
	payload []byte,
	effect ports.InboxEffect,
) (bool, error) {
	if consumerName == "" || messageID == "" || effect == nil {
		return false, fmt.Errorf("consumer name, message ID, and effect are required")
	}
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])

	tx := txFromContext(ctx)
	var err error
	ownsTx := tx == nil
	if ownsTx {
		tx, err = r.DB.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return false, fmt.Errorf("begin inbox transaction: %w", err)
		}
	}
	if ownsTx {
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var inboxID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO inbox_messages (id, consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (consumer_name, message_id) DO NOTHING
		RETURNING id
	`, uuid.New(), consumerName, messageID, payloadHash, time.Now().UTC()).Scan(&inboxID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("claim inbox message: %w", err)
		}
		var completedAt *time.Time
		var existingHash string
		var receivedAt time.Time
		var existingID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT id, payload_hash, received_at, completed_at
			FROM inbox_messages
			WHERE consumer_name = $1 AND message_id = $2
			FOR UPDATE
		`, consumerName, messageID).Scan(&existingID, &existingHash, &receivedAt, &completedAt); err != nil {
			return false, fmt.Errorf("inspect duplicate inbox message: %w", err)
		}
		if existingHash != payloadHash {
			return false, fmt.Errorf("inbox message %s payload hash mismatch", messageID)
		}
		if completedAt != nil {
			if ownsTx {
				return true, tx.Commit(ctx)
			}
			return true, nil
		}
		if time.Since(receivedAt) < 5*time.Minute {
			return true, fmt.Errorf("inbox message %s is already being processed", messageID)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE inbox_messages
			SET received_at = $1
			WHERE consumer_name = $2 AND message_id = $3
		`, time.Now().UTC(), consumerName, messageID); err != nil {
			return false, fmt.Errorf("reclaim inbox message: %w", err)
		}
		inboxID = existingID
	}

	if err := effect(ctx, payload); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE inbox_messages
		SET completed_at = $1
		WHERE id = $2
	`, time.Now().UTC(), inboxID); err != nil {
		return false, fmt.Errorf("complete inbox message: %w", err)
	}
	if ownsTx {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit inbox transaction: %w", err)
		}
	}
	return false, nil
}
