package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

var (
	ErrIdempotencyKeyRequired = errors.New("idempotency key is required")
	ErrInvalidWagerRequest    = errors.New("invalid wager request")
)

type WageringRequest = ports.WageringRequest

type WageringService struct {
	repo ports.WageringRepository
}

var _ ports.WageringService = (*WageringService)(nil)

func NewWageringService(repo ports.WageringRepository) *WageringService {
	return &WageringService{repo: repo}
}

func (s *WageringService) ProcessTransaction(
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
	if kind == "" {
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
	payload := struct {
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
		PlayerID              string `json:"playerId"`
		WalletID              string `json:"walletId"`
		RoundID               string `json:"roundId"`
		GameID                string `json:"gameId"`
		Kind                  string `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
	}{
		ProviderID:            providerID,
		ExternalTransactionID: externalTransactionID,
		PlayerID:              playerID.String(),
		WalletID:              walletID.String(),
		RoundID:               roundID,
		GameID:                gameID,
		Kind:                  kind,
		Money: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{
			Amount:   amount.String(),
			Currency: amount.Currency(),
		},
		ReferenceExternalTransactionID: referenceExternalTransactionID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
