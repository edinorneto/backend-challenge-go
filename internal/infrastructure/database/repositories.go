package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
)

var ErrWalletAlreadyExists = errors.New("wallet already exists")

type WalletRepo struct {
	DB *pgxpool.Pool
}

func NewWalletRepo(db *pgxpool.Pool) *WalletRepo {
	return &WalletRepo{DB: db}
}

func (r *WalletRepo) Create(ctx context.Context, w *wallet.Wallet) error {
	tx, err := r.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin wallet transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO wallets (
			id,
			player_id,
			currency,
			balance_cents,
			version,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		`,
		w.ID(),
		w.PlayerID(),
		w.Currency(),
		w.Balance().AmountCents(),
		w.Version(),
		w.CreatedAt(),
		w.UpdatedAt(),
	)

	if err != nil {
		var pgErr *pgconn.PgError

		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrWalletAlreadyExists
		}

		return fmt.Errorf("insert wallet: %w", err)
	}

	// Saldo inicial zero não gera OPENING, ledger nem eventos.
	if w.Balance().IsZero() {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit wallet creation: %w", err)
		}

		return nil
	}

	transactionID := uuid.New()
	now := time.Now().UTC()

	// A abertura é uma operação interna.
	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO wager_transactions (
			id,
			source,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			player_id,
			wallet_id,
			round_id,
			game_id,
			kind,
			status,
			amount_cents,
			currency,
			reference_external_transaction_id,
			reference_transaction_id,
			failure_code,
			result_balance_cents,
			result_wallet_version,
			reference_attempts,
			reference_next_attempt_at,
			created_at,
			updated_at,
			processed_at
		)
		VALUES (
			$1,
			'INTERNAL',
			NULL,
			NULL,
			NULL,
			NULL,
			$2,
			$3,
			NULL,
			NULL,
			'OPENING',
			'PROCESSED',
			$4,
			$5,
			NULL,
			NULL,
			NULL,
			$4,
			1,
			0,
			NULL,
			$6,
			$6,
			$6
		)
		`,
		transactionID,
		w.PlayerID(),
		w.ID(),
		w.Balance().AmountCents(),
		w.Currency(),
		now,
	)

	if err != nil {
		return fmt.Errorf("insert opening transaction: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO wallet_ledger_entries (
			id,
			wallet_id,
			transaction_id,
			direction,
			amount_cents,
			currency,
			balance_before_cents,
			balance_after_cents,
			created_at
		)
		VALUES (
			$1,
			$2,
			$3,
			'CREDIT',
			$4,
			$5,
			0,
			$4,
			$6
		)
		`,
		uuid.New(),
		w.ID(),
		transactionID,
		w.Balance().AmountCents(),
		w.Currency(),
		now,
	)

	if err != nil {
		return fmt.Errorf("insert opening ledger: %w", err)
	}

	wagerPayload, err := json.Marshal(map[string]any{
		"transactionId": transactionID,
		"walletId":      w.ID(),
		"playerId":      w.PlayerID(),
		"kind":          "OPENING",
		"status":        "PROCESSED",
		"amount": map[string]string{
			"amount":   w.Balance().String(),
			"currency": w.Currency(),
		},
		"result": map[string]any{
			"balance": w.Balance().String(),
			"version": w.Version(),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal wager event: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			event_id,
			aggregate_type,
			aggregate_id,
			event_type,
			correlation_id,
			causation_id,
			occurred_at,
			version,
			payload,
			status,
			attempts,
			next_attempt_at
		)
		VALUES (
			$1,
			'wallet',
			$2,
			'WagerTransactionProcessed',
			$3,
			NULL,
			$4,
			1,
			$5,
			'PENDING',
			0,
			$4
		)
		`,
		uuid.New(),
		w.ID(),
		transactionID,
		now,
		wagerPayload,
	)

	if err != nil {
		return fmt.Errorf("insert wager outbox event: %w", err)
	}

	walletPayload, err := json.Marshal(map[string]any{
		"walletId": w.ID(),
		"playerId": w.PlayerID(),
		"balance": map[string]string{
			"amount":   w.Balance().String(),
			"currency": w.Currency(),
		},
		"version": w.Version(),
	})
	if err != nil {
		return fmt.Errorf("marshal wallet event: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			event_id,
			aggregate_type,
			aggregate_id,
			event_type,
			correlation_id,
			causation_id,
			occurred_at,
			version,
			payload,
			status,
			attempts,
			next_attempt_at
		)
		VALUES (
			$1,
			'wallet',
			$2,
			'WalletBalanceChanged',
			$3,
			NULL,
			$4,
			1,
			$5,
			'PENDING',
			0,
			$4
		)
		`,
		uuid.New(),
		w.ID(),
		transactionID,
		now,
		walletPayload,
	)

	if err != nil {
		return fmt.Errorf("insert wallet outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit wallet creation: %w", err)
	}

	return nil
}

func (r *WalletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	var (
		playerID     uuid.UUID
		currency     string
		balanceCents int64
		version      int64
		createdAt    time.Time
		updatedAt    time.Time
	)

	err := r.DB.QueryRow(
		ctx,
		`
		SELECT
			player_id,
			currency,
			balance_cents,
			version,
			created_at,
			updated_at
		FROM wallets
		WHERE id = $1
		`,
		id,
	).Scan(
		&playerID,
		&currency,
		&balanceCents,
		&version,
		&createdAt,
		&updatedAt,
	)

	if err != nil {
		return nil, err
	}

	balance, err := money.FromCents(balanceCents, currency)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet money: %w", err)
	}

	w, err := wallet.Rehydrate(
		id,
		playerID,
		currency,
		balance,
		version,
		createdAt,
		updatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet: %w", err)
	}

	return w, nil
}
