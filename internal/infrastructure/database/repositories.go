package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/domain/ledger"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

var (
	ErrWalletAlreadyExists         = errors.New("wallet already exists")
	ErrWalletNotFound              = errors.New("wallet not found")
	ErrIdempotencyConflict         = errors.New("idempotency conflict")
	ErrExternalTransactionConflict = errors.New("external transaction conflict")
)

const (
	failureInsufficientFunds      = "insufficient_funds"
	failureCurrencyMismatch       = "currency_mismatch"
	failureWalletPlayerMismatch   = "wallet_player_mismatch"
	failureWalletCurrencyMismatch = "wallet_currency_mismatch"
	failureUnsupportedOperation   = "unsupported_operation"
	failureIdempotencyConflict    = "idempotency_conflict"
)

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

func (r *WalletRepo) ProcessTransaction(
	ctx context.Context,
	req ports.ProcessTransactionRequest,
) (ports.ProcessTransactionResult, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return ports.ProcessTransactionResult{}, errors.New("idempotency key is required")
	}

	now := time.Now().UTC()
	tx, err := r.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("begin wagering transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	transactionID := uuid.New()
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
			'EXTERNAL',
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			'PENDING',
			$11,
			$12,
			$13,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			0,
			NULL,
			$14,
			$14,
			NULL
		)
		`,
		transactionID,
		req.ProviderID,
		req.ExternalTransactionID,
		req.IdempotencyKey,
		req.PayloadHash,
		req.PlayerID,
		req.WalletID,
		req.RoundID,
		req.GameID,
		req.Kind,
		req.Amount.AmountCents(),
		req.Amount.Currency(),
		now,
	)
	if err != nil {
		if isUniqueViolation(err) {
			result, dupErr := handleDuplicateTransaction(ctx, tx, req)
			if dupErr != nil {
				return ports.ProcessTransactionResult{}, dupErr
			}
			if err := tx.Commit(ctx); err != nil {
				return ports.ProcessTransactionResult{}, fmt.Errorf("commit duplicate transaction: %w", err)
			}
			return result, nil
		}
		return ports.ProcessTransactionResult{}, fmt.Errorf("insert wager transaction: %w", err)
	}

	walletRow := struct {
		playerID     uuid.UUID
		currency     string
		balanceCents int64
		version      int64
		createdAt    time.Time
		updatedAt    time.Time
	}{}

	err = tx.QueryRow(
		ctx,
		`SELECT player_id, currency, balance_cents, version, created_at, updated_at FROM wallets WHERE id = $1 FOR UPDATE`,
		req.WalletID,
	).Scan(
		&walletRow.playerID,
		&walletRow.currency,
		&walletRow.balanceCents,
		&walletRow.version,
		&walletRow.createdAt,
		&walletRow.updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ports.ProcessTransactionResult{}, ErrWalletNotFound
		}
		return ports.ProcessTransactionResult{}, fmt.Errorf("lock wallet: %w", err)
	}

	walletBalance, err := money.FromCents(walletRow.balanceCents, walletRow.currency)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate wallet balance: %w", err)
	}

	w, err := wallet.Rehydrate(
		req.WalletID,
		walletRow.playerID,
		walletRow.currency,
		walletBalance,
		walletRow.version,
		walletRow.createdAt,
		walletRow.updatedAt,
	)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate wallet during processing: %w", err)
	}

	resultBalance := w.Balance()
	resultVersion := w.Version()
	failureCode := ""
	status := "PROCESSED"
	ledgerEntry := (*ledger.WalletLedgerEntry)(nil)

	switch strings.ToUpper(req.Kind) {
	case "BET":
		if req.Amount.AmountCents() <= 0 {
			status = "REJECTED"
			failureCode = failureUnsupportedOperation
			break
		}
		if w.PlayerID() != req.PlayerID {
			status = "REJECTED"
			failureCode = failureWalletPlayerMismatch
			break
		}
		if req.Amount.Currency() != w.Currency() {
			status = "REJECTED"
			failureCode = failureCurrencyMismatch
			break
		}
		if req.Amount.AmountCents() > w.Balance().AmountCents() {
			status = "REJECTED"
			failureCode = failureInsufficientFunds
			break
		}
		if err := w.Debit(req.Amount, now); err != nil {
			if errors.Is(err, wallet.ErrInsufficientFunds) {
				status = "REJECTED"
				failureCode = failureInsufficientFunds
				break
			}
			return ports.ProcessTransactionResult{}, fmt.Errorf("apply BET: %w", err)
		}
		resultBalance = w.Balance()
		resultVersion = w.Version()
		ledgerEntry, err = ledger.NewDebit(
			uuid.New(),
			req.WalletID,
			transactionID,
			req.Amount,
			walletBalance,
			now,
		)
		if err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("create BET ledger entry: %w", err)
		}
	case "WIN":
		if req.Amount.AmountCents() <= 0 {
			status = "REJECTED"
			failureCode = failureUnsupportedOperation
			break
		}
		if w.PlayerID() != req.PlayerID {
			status = "REJECTED"
			failureCode = failureWalletPlayerMismatch
			break
		}
		if req.Amount.Currency() != w.Currency() {
			status = "REJECTED"
			failureCode = failureCurrencyMismatch
			break
		}
		if err := w.Credit(req.Amount, now); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("apply WIN: %w", err)
		}
		resultBalance = w.Balance()
		resultVersion = w.Version()
		ledgerEntry, err = ledger.NewCredit(
			uuid.New(),
			req.WalletID,
			transactionID,
			req.Amount,
			walletBalance,
			now,
		)
		if err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("create WIN ledger entry: %w", err)
		}
	case "LOSS":
		if !req.Amount.IsZero() {
			status = "REJECTED"
			failureCode = failureUnsupportedOperation
			break
		}
		if w.PlayerID() != req.PlayerID {
			status = "REJECTED"
			failureCode = failureWalletPlayerMismatch
			break
		}
		if req.Amount.Currency() != w.Currency() {
			status = "REJECTED"
			failureCode = failureCurrencyMismatch
			break
		}
		resultBalance = w.Balance()
		resultVersion = w.Version()
	case "OPENING", "REFUND", "ROLLBACK":
		status = "REJECTED"
		failureCode = failureUnsupportedOperation
	default:
		status = "REJECTED"
		failureCode = failureUnsupportedOperation
	}

	if status == "PROCESSED" {
		if req.Kind == "BET" || req.Kind == "WIN" {
			if _, err := tx.Exec(
				ctx,
				`UPDATE wallets SET balance_cents = $1, version = $2, updated_at = $3 WHERE id = $4`,
				w.Balance().AmountCents(),
				w.Version(),
				now,
				req.WalletID,
			); err != nil {
				return ports.ProcessTransactionResult{}, fmt.Errorf("update wallet balance: %w", err)
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
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
				`,
				ledgerEntry.ID(),
				req.WalletID,
				transactionID,
				string(ledgerEntry.Direction()),
				ledgerEntry.Amount().AmountCents(),
				ledgerEntry.Currency(),
				ledgerEntry.BalanceBefore().AmountCents(),
				ledgerEntry.BalanceAfter().AmountCents(),
				now,
			)
			if err != nil {
				return ports.ProcessTransactionResult{}, fmt.Errorf("insert wallet ledger entry: %w", err)
			}
		}
		_, err = tx.Exec(
			ctx,
			`
			UPDATE wager_transactions
			SET status = 'PROCESSED',
			    failure_code = NULL,
			    result_balance_cents = $1,
			    result_wallet_version = $2,
			    updated_at = $3,
			    processed_at = $3
			WHERE id = $4
			`,
			resultBalance.AmountCents(),
			resultVersion,
			now,
			transactionID,
		)
		if err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("update processed wager transaction: %w", err)
		}

		if err := insertOutboxEvent(ctx, tx, now, transactionID, req.WalletID, "WagerTransactionProcessed", buildProcessedPayload(transactionID, req, resultBalance, resultVersion)); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("insert processed outbox event: %w", err)
		}
		if req.Kind == "BET" || req.Kind == "WIN" {
			if err := insertOutboxEvent(ctx, tx, now, transactionID, req.WalletID, "WalletBalanceChanged", buildWalletBalanceChangedPayload(req.WalletID, transactionID, req.Kind, req.Amount, walletBalance, resultBalance, w.Version(), resultVersion)); err != nil {
				return ports.ProcessTransactionResult{}, fmt.Errorf("insert wallet balance outbox event: %w", err)
			}
		}
	} else {
		_, err = tx.Exec(
			ctx,
			`
			UPDATE wager_transactions
			SET status = 'REJECTED',
			    failure_code = $1,
			    result_balance_cents = $2,
			    result_wallet_version = $3,
			    updated_at = $4,
			    processed_at = NULL
			WHERE id = $5
			`,
			failureCode,
			walletBalance.AmountCents(),
			w.Version(),
			now,
			transactionID,
		)
		if err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("update rejected wager transaction: %w", err)
		}
		if err := insertOutboxEvent(ctx, tx, now, transactionID, req.WalletID, "WagerTransactionRejected", buildRejectedPayload(transactionID, req, failureCode, walletBalance, w.Version())); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("insert rejected outbox event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("commit wagering transaction: %w", err)
	}

	return ports.ProcessTransactionResult{
		TransactionID:    transactionID,
		Status:           status,
		Balance:          resultBalance,
		IdempotentReplay: false,
		FailureCode:      failureCode,
	}, nil
}

func handleDuplicateTransaction(
	ctx context.Context,
	tx pgx.Tx,
	req ports.ProcessTransactionRequest,
) (ports.ProcessTransactionResult, error) {
	var (
		existingID                  uuid.UUID
		existingStatus              string
		existingPayloadHash         string
		existingResultBalanceCents  int64
		existingResultWalletVersion int64
		existingCurrency            string
	)

	err := tx.QueryRow(
		ctx,
		`
		SELECT id, status, payload_hash, result_balance_cents, result_wallet_version, currency
		FROM wager_transactions
		WHERE provider_id = $1 AND idempotency_key = $2
		LIMIT 1
		`,
		req.ProviderID,
		req.IdempotencyKey,
	).Scan(
		&existingID,
		&existingStatus,
		&existingPayloadHash,
		&existingResultBalanceCents,
		&existingResultWalletVersion,
		&existingCurrency,
	)
	if err == nil {
		if existingPayloadHash == req.PayloadHash {
			balance, err := money.FromCents(existingResultBalanceCents, existingCurrency)
			if err != nil {
				return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate duplicate balance: %w", err)
			}
			return ports.ProcessTransactionResult{
				TransactionID:    existingID,
				Status:           existingStatus,
				Balance:          balance,
				IdempotentReplay: true,
			}, nil
		}
		return ports.ProcessTransactionResult{}, ErrIdempotencyConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ports.ProcessTransactionResult{}, fmt.Errorf("lookup duplicate idempotency key: %w", err)
	}

	err = tx.QueryRow(
		ctx,
		`
		SELECT id, status, payload_hash, result_balance_cents, result_wallet_version, currency
		FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2
		LIMIT 1
		`,
		req.ProviderID,
		req.ExternalTransactionID,
	).Scan(
		&existingID,
		&existingStatus,
		&existingPayloadHash,
		&existingResultBalanceCents,
		&existingResultWalletVersion,
		&existingCurrency,
	)
	if err == nil {
		return ports.ProcessTransactionResult{}, ErrExternalTransactionConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ports.ProcessTransactionResult{}, fmt.Errorf("lookup duplicate external transaction: %w", err)
	}

	return ports.ProcessTransactionResult{}, ErrIdempotencyConflict
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func insertOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	now time.Time,
	transactionID uuid.UUID,
	walletID uuid.UUID,
	eventType string,
	payload map[string]any,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	aggregateType := "wager_transaction"
	aggregateID := transactionID
	if eventType == "WalletBalanceChanged" {
		aggregateType = "wallet"
		aggregateID = walletID
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
		VALUES ($1,$2,$3,$4,$5,NULL,$6,1,$7,'PENDING',0,$6)
		`,
		uuid.New(),
		aggregateType,
		aggregateID,
		eventType,
		transactionID,
		now,
		body,
	)
	if err != nil {
		return err
	}

	return nil
}

func buildProcessedPayload(
	transactionID uuid.UUID,
	req ports.ProcessTransactionRequest,
	resultBalance money.Money,
	resultVersion int64,
) map[string]any {
	return map[string]any{
		"transactionId": transactionID,
		"walletId":      req.WalletID,
		"playerId":      req.PlayerID,
		"kind":          req.Kind,
		"status":        "PROCESSED",
		"amount": map[string]string{
			"amount":   req.Amount.String(),
			"currency": req.Amount.Currency(),
		},
		"result": map[string]any{
			"balance": map[string]string{
				"amount":   resultBalance.String(),
				"currency": resultBalance.Currency(),
			},
			"version": resultVersion,
		},
	}
}

func buildWalletBalanceChangedPayload(
	walletID uuid.UUID,
	transactionID uuid.UUID,
	kind string,
	amount money.Money,
	balanceBefore money.Money,
	balanceAfter money.Money,
	currentVersion int64,
	resultVersion int64,
) map[string]any {
	return map[string]any{
		"walletId":      walletID,
		"transactionId": transactionID,
		"direction":     mapDirection(kind),
		"money": map[string]string{
			"amount":   amount.String(),
			"currency": amount.Currency(),
		},
		"balanceBefore": map[string]string{
			"amount":   balanceBefore.String(),
			"currency": balanceBefore.Currency(),
		},
		"balanceAfter": map[string]string{
			"amount":   balanceAfter.String(),
			"currency": balanceAfter.Currency(),
		},
		"walletVersion":   resultVersion,
		"previousVersion": currentVersion,
	}
}

func buildRejectedPayload(
	transactionID uuid.UUID,
	req ports.ProcessTransactionRequest,
	failureCode string,
	balance money.Money,
	version int64,
) map[string]any {
	return map[string]any{
		"transactionId": transactionID,
		"walletId":      req.WalletID,
		"playerId":      req.PlayerID,
		"kind":          req.Kind,
		"status":        "REJECTED",
		"failureCode":   failureCode,
		"amount": map[string]string{
			"amount":   req.Amount.String(),
			"currency": req.Amount.Currency(),
		},
		"result": map[string]any{
			"balance": map[string]string{
				"amount":   balance.String(),
				"currency": balance.Currency(),
			},
			"version": version,
		},
	}
}

func mapDirection(kind string) string {
	switch strings.ToUpper(kind) {
	case "BET":
		return "DEBIT"
	case "WIN":
		return "CREDIT"
	default:
		return ""
	}
}
