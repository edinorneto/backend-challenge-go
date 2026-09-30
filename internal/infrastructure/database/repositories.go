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
	ErrReferenceNotFound           = errors.New("reference transaction not found")
	ErrReferenceIncompatible       = errors.New("reference transaction incompatible")
	ErrDuplicateReversal           = errors.New("duplicate reversal")
)

const (
	failureInsufficientFunds         = "insufficient_funds"
	failureCurrencyMismatch          = "currency_mismatch"
	failureWalletPlayerMismatch      = "wallet_player_mismatch"
	failureWalletCurrencyMismatch    = "wallet_currency_mismatch"
	failureUnsupportedOperation      = "unsupported_operation"
	failureIdempotencyConflict       = "idempotency_conflict"
	failureReferenceNotFound         = "reference_not_found"
	failureReferencePending          = "reference_pending"
	failureReferenceIncompatible     = "reference_incompatible"
	failureReversalAlreadyProcessed  = "reversal_already_processed"
	failureReversalInsufficientFunds = "reversal_insufficient_funds"
)

const (
	maxReferenceAttempts = 5
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
	insertResult, err := tx.Exec(
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
			0,
			NULL,
			$14,
			$14,
			NULL
		)
		ON CONFLICT DO NOTHING
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
		req.ReferenceExternalTransactionID,
		now,
	)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("insert wager transaction: %w", err)
	}
	if insertResult.RowsAffected() == 0 {
		result, duplicateErr := handleDuplicateTransaction(ctx, tx, req)
		if duplicateErr != nil {
			return ports.ProcessTransactionResult{}, duplicateErr
		}
		if err := tx.Commit(ctx); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("commit duplicate transaction: %w", err)
		}
		return result, nil
	}

	reference, pending, err := resolveReversalReference(ctx, tx, req)
	if err != nil {
		return ports.ProcessTransactionResult{}, err
	}
	if pending {
		var walletExists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wallets WHERE id = $1)`, req.WalletID).Scan(&walletExists); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("check wallet for pending reference: %w", err)
		}
		if !walletExists {
			return ports.ProcessTransactionResult{}, ErrWalletNotFound
		}
		result, err := persistPendingReference(ctx, tx, transactionID, req, now)
		if err != nil {
			return ports.ProcessTransactionResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("commit pending reference: %w", err)
		}
		return result, nil
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
	case "REFUND", "ROLLBACK":
		resultBalance, resultVersion, failureCode, ledgerEntry, err = processReversal(
			ctx, tx, req, reference, w, walletBalance, transactionID, now,
		)
		if err != nil {
			return ports.ProcessTransactionResult{}, err
		}
		if failureCode != "" {
			status = "REJECTED"
		}
	case "OPENING":
		status = "REJECTED"
		failureCode = failureUnsupportedOperation
	default:
		status = "REJECTED"
		failureCode = failureUnsupportedOperation
	}

	if status == "PROCESSED" {
		if ledgerEntry != nil {
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
			        reference_transaction_id = COALESCE(reference_transaction_id, $3),
			        updated_at = $4,
			    processed_at = $4
			WHERE id = $5
			`,
			resultBalance.AmountCents(),
			resultVersion,
			referenceTransactionIDOrNil(req.Kind, reference),
			now,
			transactionID,
		)
		if err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("update processed wager transaction: %w", err)
		}

		if err := insertOutboxEvent(ctx, tx, now, transactionID, req.WalletID, "WagerTransactionProcessed", buildProcessedPayload(transactionID, req, resultBalance, resultVersion)); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("insert processed outbox event: %w", err)
		}
		if ledgerEntry != nil {
			if err := insertOutboxEvent(ctx, tx, now, transactionID, req.WalletID, "WalletBalanceChanged", buildWalletBalanceChangedPayload(req.WalletID, transactionID, string(ledgerEntry.Direction()), req.Amount, walletBalance, resultBalance, w.Version(), resultVersion)); err != nil {
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

// RetryPendingReference advances retry metadata for a pending reversal whose
// reference is still unavailable. It does not touch the wallet or ledger.
func (r *WalletRepo) RetryPendingReference(ctx context.Context, transactionID uuid.UUID) (ports.ProcessTransactionResult, error) {
	tx, err := r.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("begin reference retry: %w", err)
	}
	defer tx.Rollback(ctx)

	var req struct {
		providerID          string
		externalID          string
		idempotencyKey      string
		payloadHash         string
		playerID            uuid.UUID
		walletID            uuid.UUID
		roundID             string
		gameID              string
		kind                string
		amountCents         int64
		currency            string
		referenceExternalID string
		status              string
		attempts            int
		balanceCents        int64
	}
	err = tx.QueryRow(ctx, `
		SELECT provider_id, external_transaction_id, idempotency_key, payload_hash,
		       player_id, wallet_id, round_id, game_id, kind, amount_cents, currency,
		       reference_external_transaction_id, status, reference_attempts,
		       COALESCE(result_balance_cents, 0)
		FROM wager_transactions
		WHERE id = $1
		FOR UPDATE
	`, transactionID).Scan(
		&req.providerID, &req.externalID, &req.idempotencyKey, &req.payloadHash,
		&req.playerID, &req.walletID, &req.roundID, &req.gameID, &req.kind,
		&req.amountCents, &req.currency, &req.referenceExternalID, &req.status,
		&req.attempts, &req.balanceCents,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.ProcessTransactionResult{}, ErrReferenceNotFound
	}
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("lock pending reference: %w", err)
	}
	if req.status != "PENDING_REFERENCE" {
		balance, err := money.FromCents(req.balanceCents, req.currency)
		if err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate retry result: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("commit reference retry result: %w", err)
		}
		return ports.ProcessTransactionResult{TransactionID: transactionID, Status: req.status, Balance: balance}, nil
	}

	amount, err := money.FromCents(req.amountCents, req.currency)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate pending reference amount: %w", err)
	}
	reversalReq := ports.ProcessTransactionRequest{
		ProviderID: req.providerID, ExternalTransactionID: req.externalID,
		IdempotencyKey: req.idempotencyKey, PayloadHash: req.payloadHash,
		PlayerID: req.playerID, WalletID: req.walletID, RoundID: req.roundID,
		GameID: req.gameID, Kind: req.kind, Amount: amount,
		ReferenceExternalTransactionID: req.referenceExternalID,
	}
	reference, pending, err := resolveReversalReference(ctx, tx, reversalReq)
	if err != nil {
		return ports.ProcessTransactionResult{}, err
	}
	if pending {
		result, err := persistReferenceRetry(ctx, tx, transactionID, req.walletID, req.providerID, req.kind, req.referenceExternalID, req.attempts, req.balanceCents, req.currency)
		if err != nil {
			return ports.ProcessTransactionResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("commit reference retry: %w", err)
		}
		return result, nil
	}

	walletRow := struct {
		playerID     uuid.UUID
		currency     string
		balanceCents int64
		version      int64
		createdAt    time.Time
		updatedAt    time.Time
	}{}
	if err := tx.QueryRow(ctx, `
		SELECT player_id, currency, balance_cents, version, created_at, updated_at
		FROM wallets WHERE id = $1 FOR UPDATE
	`, req.walletID).Scan(
		&walletRow.playerID, &walletRow.currency, &walletRow.balanceCents,
		&walletRow.version, &walletRow.createdAt, &walletRow.updatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ports.ProcessTransactionResult{}, ErrWalletNotFound
		}
		return ports.ProcessTransactionResult{}, fmt.Errorf("lock wallet for reference retry: %w", err)
	}
	walletBalance, err := money.FromCents(walletRow.balanceCents, walletRow.currency)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate retry wallet balance: %w", err)
	}
	w, err := wallet.Rehydrate(req.walletID, walletRow.playerID, walletRow.currency, walletBalance, walletRow.version, walletRow.createdAt, walletRow.updatedAt)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate retry wallet: %w", err)
	}
	resultBalance, resultVersion, failureCode, ledgerEntry, err := processReversal(
		ctx, tx, reversalReq, reference, w, walletBalance, transactionID, time.Now().UTC(),
	)
	if err != nil {
		return ports.ProcessTransactionResult{}, err
	}
	status := "PROCESSED"
	if failureCode != "" {
		status = "REJECTED"
	}
	now := time.Now().UTC()
	if status == "PROCESSED" && ledgerEntry != nil {
		if _, err := tx.Exec(ctx, `UPDATE wallets SET balance_cents = $1, version = $2, updated_at = $3 WHERE id = $4`, resultBalance.AmountCents(), resultVersion, now, req.walletID); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("update retry wallet balance: %w", err)
		}
		if err := insertLedgerEntry(ctx, tx, ledgerEntry, now); err != nil {
			return ports.ProcessTransactionResult{}, err
		}
	}
	if status == "PROCESSED" {
		if _, err := tx.Exec(ctx, `
			UPDATE wager_transactions
			SET status = 'PROCESSED', failure_code = NULL,
			    reference_transaction_id = $1, result_balance_cents = $2,
			    result_wallet_version = $3, updated_at = $4, processed_at = $4
			WHERE id = $5
		`, reference.id, resultBalance.AmountCents(), resultVersion, now, transactionID); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("update retried wager transaction: %w", err)
		}
		if err := insertOutboxEvent(ctx, tx, now, transactionID, req.walletID, "WagerTransactionProcessed", buildProcessedPayload(transactionID, reversalReq, resultBalance, resultVersion)); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("insert retried processed event: %w", err)
		}
		if ledgerEntry != nil {
			if err := insertOutboxEvent(ctx, tx, now, transactionID, req.walletID, "WalletBalanceChanged", buildWalletBalanceChangedPayload(req.walletID, transactionID, string(ledgerEntry.Direction()), reversalReq.Amount, walletBalance, resultBalance, walletRow.version, resultVersion)); err != nil {
				return ports.ProcessTransactionResult{}, fmt.Errorf("insert retried balance event: %w", err)
			}
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE wager_transactions
			SET status = 'REJECTED', failure_code = $1,
			    result_balance_cents = $2, result_wallet_version = $3,
			    updated_at = $4, processed_at = NULL
			WHERE id = $5
		`, failureCode, walletBalance.AmountCents(), walletRow.version, now, transactionID); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("update rejected retried wager transaction: %w", err)
		}
		if err := insertOutboxEvent(ctx, tx, now, transactionID, req.walletID, "WagerTransactionRejected", buildRejectedPayload(transactionID, reversalReq, failureCode, walletBalance, walletRow.version)); err != nil {
			return ports.ProcessTransactionResult{}, fmt.Errorf("insert retried rejection event: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("commit resolved reference retry: %w", err)
	}
	return ports.ProcessTransactionResult{TransactionID: transactionID, Status: status, Balance: resultBalance, FailureCode: failureCode}, nil
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
		existingResultWalletVersion *int64
		existingCurrency            string
		existingFailureCode         *string
	)

	err := tx.QueryRow(
		ctx,
		`
		SELECT id, status, payload_hash, result_balance_cents, result_wallet_version, currency, failure_code
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
		&existingFailureCode,
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
				FailureCode:      stringValue(existingFailureCode),
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
		SELECT id, status, payload_hash, result_balance_cents, result_wallet_version, currency, failure_code
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
		&existingFailureCode,
	)
	if err == nil {
		return ports.ProcessTransactionResult{}, ErrExternalTransactionConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ports.ProcessTransactionResult{}, fmt.Errorf("lookup duplicate external transaction: %w", err)
	}

	return ports.ProcessTransactionResult{}, ErrIdempotencyConflict
}

type reversalReference struct {
	id          uuid.UUID
	providerID  string
	externalID  string
	playerID    uuid.UUID
	walletID    uuid.UUID
	roundID     string
	kind        string
	status      string
	amountCents int64
	currency    string
}

func resolveReversalReference(
	ctx context.Context,
	tx pgx.Tx,
	req ports.ProcessTransactionRequest,
) (*reversalReference, bool, error) {
	if req.Kind != "REFUND" && req.Kind != "ROLLBACK" {
		return nil, false, nil
	}

	var reference reversalReference
	err := tx.QueryRow(
		ctx,
		`
		SELECT id, provider_id, external_transaction_id, player_id, wallet_id,
		       round_id, kind, status, amount_cents, currency
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
		ORDER BY created_at ASC
		LIMIT 1
		FOR UPDATE
		`,
		req.ProviderID,
		req.ReferenceExternalTransactionID,
	).Scan(
		&reference.id,
		&reference.providerID,
		&reference.externalID,
		&reference.playerID,
		&reference.walletID,
		&reference.roundID,
		&reference.kind,
		&reference.status,
		&reference.amountCents,
		&reference.currency,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("resolve reversal reference: %w", err)
	}

	if reference.status == "PENDING" || reference.status == "PENDING_REFERENCE" {
		return &reference, true, nil
	}
	if reference.status != "PROCESSED" {
		return &reference, false, nil
	}

	return &reference, false, nil
}

func persistPendingReference(
	ctx context.Context,
	tx pgx.Tx,
	transactionID uuid.UUID,
	req ports.ProcessTransactionRequest,
	now time.Time,
) (ports.ProcessTransactionResult, error) {
	attempts := 1
	nextAttempt := nextReferenceAttempt(now, attempts)
	var status string
	if attempts > maxReferenceAttempts {
		status = "REJECTED"
	} else {
		status = "PENDING_REFERENCE"
	}

	failureCode := failureReferenceNotFound
	eventType := "WagerTransactionPendingReference"
	if req.ReferenceExternalTransactionID != "" {
		failureCode = failureReferencePending
	}
	if status == "REJECTED" {
		failureCode = failureReferenceNotFound
		eventType = "WagerTransactionRejected"
	}

	zero, err := money.Zero(req.Amount.Currency())
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("create pending reference balance: %w", err)
	}
	_, err = tx.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET status = $1,
		    failure_code = $2,
		    reference_attempts = $3,
		    reference_next_attempt_at = $4,
		    result_balance_cents = $5,
		    result_wallet_version = NULL,
		    updated_at = $6
		WHERE id = $7
		`,
		status,
		failureCode,
		attempts,
		nextAttempt,
		zero.AmountCents(),
		now,
		transactionID,
	)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("persist pending reference: %w", err)
	}

	payload := map[string]any{
		"transactionId":                  transactionID,
		"walletId":                       req.WalletID,
		"providerId":                     req.ProviderID,
		"kind":                           req.Kind,
		"status":                         status,
		"failureCode":                    failureCode,
		"referenceExternalTransactionId": req.ReferenceExternalTransactionID,
		"referenceAttempts":              attempts,
		"nextAttemptAt":                  nextAttempt.UTC().Format(time.RFC3339),
	}
	if err := insertOutboxEvent(ctx, tx, now, transactionID, req.WalletID, eventType, payload); err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("insert pending reference outbox event: %w", err)
	}

	return ports.ProcessTransactionResult{
		TransactionID: transactionID,
		Status:        status,
		Balance:       zero,
		FailureCode:   failureCode,
	}, nil
}

func nextReferenceAttempt(now time.Time, attempts int) time.Time {
	if attempts < 1 {
		attempts = 1
	}
	return now.Add(time.Minute << (attempts - 1))
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func validateReversalReference(
	req ports.ProcessTransactionRequest,
	reference *reversalReference,
) error {
	if reference == nil {
		return ErrReferenceNotFound
	}
	if strings.TrimSpace(reference.status) != "PROCESSED" {
		return ErrReferenceIncompatible
	}
	if strings.TrimSpace(reference.providerID) != strings.TrimSpace(req.ProviderID) ||
		reference.playerID != req.PlayerID ||
		reference.walletID != req.WalletID ||
		strings.TrimSpace(reference.roundID) != strings.TrimSpace(req.RoundID) ||
		strings.TrimSpace(reference.currency) != strings.TrimSpace(req.Amount.Currency()) ||
		reference.amountCents != req.Amount.AmountCents() {
		return ErrReferenceIncompatible
	}
	return nil
}

func validateNoDuplicateReversal(
	ctx context.Context,
	tx pgx.Tx,
	req ports.ProcessTransactionRequest,
	reference *reversalReference,
) error {
	if reference == nil {
		return ErrReferenceNotFound
	}

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM wager_transactions
			WHERE provider_id = $1
			  AND reference_external_transaction_id = $2
			  AND status = 'PROCESSED'
			  AND kind = $3
		)`
	kind := req.Kind
	if reference.kind == "BET" {
		query = `
			SELECT EXISTS (
				SELECT 1
				FROM wager_transactions
				WHERE provider_id = $1
				  AND reference_external_transaction_id = $2
				  AND status = 'PROCESSED'
				  AND kind IN ('REFUND', 'ROLLBACK')
			)`
	}

	var exists bool
	var err error
	if reference.kind == "BET" {
		err = tx.QueryRow(ctx, query, req.ProviderID, req.ReferenceExternalTransactionID).Scan(&exists)
	} else {
		err = tx.QueryRow(ctx, query, req.ProviderID, req.ReferenceExternalTransactionID, kind).Scan(&exists)
	}
	if err != nil {
		return fmt.Errorf("check duplicate reversal: %w", err)
	}
	if exists {
		return ErrDuplicateReversal
	}
	return nil
}

func processReversal(
	ctx context.Context,
	tx pgx.Tx,
	req ports.ProcessTransactionRequest,
	reference *reversalReference,
	w *wallet.Wallet,
	walletBalance money.Money,
	transactionID uuid.UUID,
	now time.Time,
) (money.Money, int64, string, *ledger.WalletLedgerEntry, error) {
	if err := validateReversalReference(req, reference); err != nil {
		return w.Balance(), w.Version(), failureCodeForReferenceError(err), nil, nil
	}
	if err := validateNoDuplicateReversal(ctx, tx, req, reference); err != nil {
		return w.Balance(), w.Version(), failureCodeForReferenceError(err), nil, nil
	}
	referenceKind := strings.TrimSpace(reference.kind)
	if req.Kind == "REFUND" && referenceKind != "BET" {
		return w.Balance(), w.Version(), failureReferenceIncompatible, nil, nil
	}
	if req.Kind == "ROLLBACK" &&
		referenceKind != "BET" && referenceKind != "WIN" && referenceKind != "REFUND" {
		return w.Balance(), w.Version(), failureReferenceIncompatible, nil, nil
	}

	var entry *ledger.WalletLedgerEntry
	var err error
	if req.Kind == "ROLLBACK" && referenceKind != "BET" {
		err = w.Debit(req.Amount, now)
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			return w.Balance(), w.Version(), failureReversalInsufficientFunds, nil, nil
		}
		if err == nil {
			entry, err = ledger.NewDebit(uuid.New(), req.WalletID, transactionID, req.Amount, walletBalance, now)
		}
	} else {
		err = w.Credit(req.Amount, now)
		if err == nil {
			entry, err = ledger.NewCredit(uuid.New(), req.WalletID, transactionID, req.Amount, walletBalance, now)
		}
	}
	if err != nil {
		return money.Money{}, 0, "", nil, fmt.Errorf("apply reversal: %w", err)
	}
	return w.Balance(), w.Version(), "", entry, nil
}

func insertLedgerEntry(ctx context.Context, tx pgx.Tx, entry *ledger.WalletLedgerEntry, now time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_cents, currency,
			balance_before_cents, balance_after_cents, created_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, entry.ID(), entry.WalletID(), entry.TransactionID(), string(entry.Direction()),
		entry.Amount().AmountCents(), entry.Currency(), entry.BalanceBefore().AmountCents(),
		entry.BalanceAfter().AmountCents(), now)
	if err != nil {
		return fmt.Errorf("insert ledger entry: %w", err)
	}
	return nil
}

func persistReferenceRetry(
	ctx context.Context,
	tx pgx.Tx,
	transactionID, walletID uuid.UUID,
	providerID, kind, referenceExternalID string,
	currentAttempts int,
	balanceCents int64,
	currency string,
) (ports.ProcessTransactionResult, error) {
	now := time.Now().UTC()
	attempts := currentAttempts + 1
	status := "PENDING_REFERENCE"
	eventType := "WagerTransactionPendingReference"
	failureCode := failureReferencePending
	if attempts > maxReferenceAttempts {
		status = "REJECTED"
		eventType = "WagerTransactionRejected"
		failureCode = failureReferenceNotFound
	}
	nextAttempt := nextReferenceAttempt(now, attempts)
	if _, err := tx.Exec(ctx, `
		UPDATE wager_transactions
		SET status = $1, failure_code = $2, reference_attempts = $3,
		    reference_next_attempt_at = $4, updated_at = $5, processed_at = NULL
		WHERE id = $6
	`, status, failureCode, attempts, nextAttempt, now, transactionID); err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("update reference retry: %w", err)
	}
	payload := map[string]any{
		"transactionId": transactionID, "walletId": walletID, "providerId": providerID,
		"kind": kind, "status": status, "failureCode": failureCode,
		"referenceExternalTransactionId": referenceExternalID,
		"referenceAttempts":              attempts, "nextAttemptAt": nextAttempt.UTC().Format(time.RFC3339),
	}
	if err := insertOutboxEvent(ctx, tx, now, transactionID, walletID, eventType, payload); err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("insert reference retry event: %w", err)
	}
	balance, err := money.FromCents(balanceCents, currency)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("rehydrate reference retry balance: %w", err)
	}
	return ports.ProcessTransactionResult{
		TransactionID: transactionID, Status: status, Balance: balance, FailureCode: failureCode,
	}, nil
}

func failureCodeForReferenceError(err error) string {
	switch {
	case errors.Is(err, ErrReferenceNotFound):
		return failureReferenceNotFound
	case errors.Is(err, ErrDuplicateReversal):
		return failureReversalAlreadyProcessed
	case errors.Is(err, ErrReferenceIncompatible):
		return failureReferenceIncompatible
	default:
		return failureReferenceIncompatible
	}
}

func referenceTransactionIDOrNil(kind string, reference *reversalReference) *uuid.UUID {
	if (kind != "REFUND" && kind != "ROLLBACK") || reference == nil {
		return nil
	}
	return &reference.id
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
	direction string,
	amount money.Money,
	balanceBefore money.Money,
	balanceAfter money.Money,
	currentVersion int64,
	resultVersion int64,
) map[string]any {
	return map[string]any{
		"walletId":      walletID,
		"transactionId": transactionID,
		"direction":     direction,
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
