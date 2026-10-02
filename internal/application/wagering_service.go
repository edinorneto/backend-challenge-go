package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

var (
	ErrIdempotencyKeyRequired = errors.New("idempotency key is required")
	ErrInvalidWagerRequest    = errors.New("invalid wager request")
)

type WageringRequest = ports.WageringRequest

type WageringService struct {
	repo    ports.WageringRepository
	logger  *observability.Logger
	metrics *observability.Metrics
}

var _ ports.WageringService = (*WageringService)(nil)

func NewWageringService(repo ports.WageringRepository, options ...any) *WageringService {
	service := &WageringService{repo: repo}
	for _, option := range options {
		switch value := option.(type) {
		case *observability.Logger:
			service.logger = value
		case *observability.Metrics:
			service.metrics = value
		}
	}
	return service
}

// ProcessTransaction is the single entry point for HTTP and SQS operations, so
// results, latency and conflicts are measured here once for both channels.
func (s *WageringService) ProcessTransaction(
	ctx context.Context,
	idempotencyKey string,
	req ports.WageringRequest,
) (ports.ProcessTransactionResult, error) {
	started := time.Now()
	result, err := s.process(ctx, idempotencyKey, req)
	s.record(ctx, req, result, err, time.Since(started))
	return result, err
}

func (s *WageringService) process(
	ctx context.Context,
	idempotencyKey string,
	req ports.WageringRequest,
) (ports.ProcessTransactionResult, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return ports.ProcessTransactionResult{}, ErrIdempotencyKeyRequired
	}

	if req.ProviderID == "" || req.ExternalTransactionID == "" {
		return ports.ProcessTransactionResult{}, ErrInvalidWagerRequest
	}
	if req.PlayerID == uuid.Nil || req.WalletID == uuid.Nil {
		return ports.ProcessTransactionResult{}, ErrInvalidWagerRequest
	}
	if req.RoundID == "" || req.GameID == "" {
		return ports.ProcessTransactionResult{}, ErrInvalidWagerRequest
	}
	if req.Amount.Currency() == "" {
		return ports.ProcessTransactionResult{}, ErrInvalidWagerRequest
	}

	kind := strings.ToUpper(strings.TrimSpace(req.Kind))
	switch kind {
	case "BET", "WIN", "LOSS", "REFUND", "ROLLBACK":
	default:
		return ports.ProcessTransactionResult{}, ErrInvalidWagerRequest
	}
	if (kind == "REFUND" || kind == "ROLLBACK") &&
		strings.TrimSpace(req.ReferenceExternalTransactionID) == "" {
		return ports.ProcessTransactionResult{}, ErrInvalidWagerRequest
	}

	payloadHash, err := computePayloadHash(
		req.ProviderID,
		req.ExternalTransactionID,
		req.PlayerID,
		req.WalletID,
		req.RoundID,
		req.GameID,
		kind,
		req.Amount,
		req.ReferenceExternalTransactionID,
	)
	if err != nil {
		return ports.ProcessTransactionResult{}, fmt.Errorf("compute payload hash: %w", err)
	}

	return s.repo.ProcessTransaction(ctx, ports.ProcessTransactionRequest{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		IdempotencyKey:                 idempotencyKey,
		PayloadHash:                    payloadHash,
		PlayerID:                       req.PlayerID,
		WalletID:                       req.WalletID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           kind,
		Amount:                         req.Amount,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	})
}

func (s *WageringService) GetTransaction(ctx context.Context, transactionID uuid.UUID) (ports.TransactionView, error) {
	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *WageringService) GetTransactionByExternal(ctx context.Context, providerID, externalTransactionID string) (ports.TransactionView, error) {
	return s.repo.GetTransactionByExternal(ctx, providerID, externalTransactionID)
}

// computePayloadHash returns the SHA-256 (hex) of the canonical JSON of the
// operation's business fields, the same for HTTP and SQS. Canonical means keys
// sorted at every level (encoding/json sorts map keys), no insignificant
// whitespace and no HTML escaping. Values are normalized first: kind is upper
// case and trimmed, money is the normalized two-decimal amount ("25.5" and
// "25.50" hash alike) and the currency code. The idempotency key and transport
// metadata (messageId, occurredAt, headers) are excluded; the provider comes
// from the authenticated identity on HTTP and from data.providerId on SQS.
func computePayloadHash(
	providerID string,
	externalTransactionID string,
	playerID uuid.UUID,
	walletID uuid.UUID,
	roundID string,
	gameID string,
	kind string,
	amount money.Money,
	referenceExternalTransactionID string,
) (string, error) {
	body, err := canonicalPayload(providerID, externalTransactionID, playerID, walletID, roundID, gameID, kind, amount, referenceExternalTransactionID)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalPayload(
	providerID string,
	externalTransactionID string,
	playerID uuid.UUID,
	walletID uuid.UUID,
	roundID string,
	gameID string,
	kind string,
	amount money.Money,
	referenceExternalTransactionID string,
) ([]byte, error) {
	payload := map[string]any{
		"externalTransactionId": externalTransactionID,
		"gameId":                gameID,
		"kind":                  kind,
		"money":                 map[string]string{"amount": amount.String(), "currency": amount.Currency()},
		"playerId":              playerID.String(),
		"providerId":            providerID,
		"roundId":               roundID,
		"walletId":              walletID.String(),
	}
	if referenceExternalTransactionID != "" {
		payload["referenceExternalTransactionId"] = referenceExternalTransactionID
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// record emits the metrics and the log line of one operation. Labels come from
// fixed sets (status, failure code, error kind, SQLSTATE); identifiers go only
// to the log.
func (s *WageringService) record(ctx context.Context, req ports.WageringRequest, result ports.ProcessTransactionResult, err error, duration time.Duration) {
	outcome := operationOutcome(err)
	if s.metrics != nil {
		s.metrics.Observe("wager_processing_duration", duration)
		if err != nil {
			s.metrics.IncLabeled("wager_errors_total", map[string]string{"kind": outcome})
			if class := observability.ErrorClass(err); class == "postgres_40P01" || class == "postgres_40001" {
				s.metrics.IncLabeled("db_concurrency_conflicts_total", map[string]string{"sqlstate": strings.TrimPrefix(class, "postgres_")})
			}
		} else {
			s.metrics.IncLabeled("wager_results_total", map[string]string{"status": result.Status})
			if result.Status == "REJECTED" {
				s.metrics.IncLabeled("wager_rejections_total", map[string]string{"failure_code": result.FailureCode})
			}
			if result.IdempotentReplay {
				s.metrics.Inc("idempotency_replays_total")
			}
		}
	}
	if s.logger == nil {
		return
	}
	fields := map[string]string{
		"providerId": req.ProviderID,
		"walletId":   req.WalletID.String(),
		"operation":  strings.ToUpper(strings.TrimSpace(req.Kind)),
		"duration":   duration.String(),
	}
	if err != nil {
		fields["result"] = outcome
		s.logger.Error(ctx, "wager_transaction_failed", err, fields)
		return
	}
	fields["transactionId"] = result.TransactionID.String()
	fields["status"] = result.Status
	fields["replay"] = fmt.Sprint(result.IdempotentReplay)
	if result.FailureCode != "" {
		fields["failureCode"] = result.FailureCode
	}
	s.logger.Info(ctx, "wager_transaction_completed", fields)
}

func operationOutcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrIdempotencyKeyRequired), errors.Is(err, ErrInvalidWagerRequest):
		return "invalid_request"
	case errors.Is(err, ports.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, ports.ErrExternalTransactionConflict):
		return "external_transaction_conflict"
	case errors.Is(err, ports.ErrWalletNotFound):
		return "wallet_not_found"
	default:
		return "infrastructure"
	}
}
