package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
)

type Server struct {
	wallets  *application.WalletService
	wagering *application.WageringService
}

func NewServer(wallets *application.WalletService, wagering ...*application.WageringService) *Server {
	var service *application.WageringService
	if len(wagering) > 0 {
		service = wagering[0]
	}

	return &Server{
		wallets:  wallets,
		wagering: service,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health/live", s.liveHandler)
	mux.HandleFunc("POST /wallets", s.walletsHandler)
	mux.HandleFunc("GET /wallets/{walletID}", s.getWalletHandler)
	mux.HandleFunc("POST /wagering/transactions", s.wageringHandler)

	return loggingMiddleware(mux)
}

func (s *Server) liveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) walletsHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/wallets" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PlayerID       string `json:"playerId"`
		InitialBalance struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"initialBalance"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	playerID, err := uuid.Parse(req.PlayerID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_player_id",
		})
		return
	}

	initialBalance, err := money.ParseExternal(
		req.InitialBalance.Amount,
		req.InitialBalance.Currency,
	)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_money",
		})
		return
	}

	wallet, err := s.wallets.CreateWallet(
		r.Context(),
		playerID,
		initialBalance,
	)
	if err != nil {
		if errors.Is(err, database.ErrWalletAlreadyExists) {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "wallet_already_exists",
			})
			return
		}

		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "internal_error",
		})
		return
	}

	w.Header().Set("Location", "/wallets/"+wallet.ID().String())

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":       wallet.ID(),
		"playerId": wallet.PlayerID(),
		"balance": map[string]string{
			"amount":   wallet.Balance().String(),
			"currency": wallet.Currency(),
		},
		"version": wallet.Version(),
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}

func trimTrailingSlash(value string) string {
	return strings.TrimRight(value, "/")
}

func (s *Server) getWalletHandler(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(r.PathValue("walletID"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_wallet_id",
		})
		return
	}

	wallet, err := s.wallets.GetWallet(r.Context(), walletID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "wallet_not_found",
			})
			return
		}

		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "internal_error",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":       wallet.ID(),
		"playerId": wallet.PlayerID(),
		"balance": map[string]string{
			"amount":   wallet.Balance().String(),
			"currency": wallet.Currency(),
		},
		"version": wallet.Version(),
	})
}

func (s *Server) wageringHandler(w http.ResponseWriter, r *http.Request) {
	if s.wagering == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "wagering_service_unavailable",
		})
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
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
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "idempotency_key_required",
		})
		return
	}

	playerID, err := uuid.Parse(req.PlayerID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_player_id",
		})
		return
	}

	walletID, err := uuid.Parse(req.WalletID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_wallet_id",
		})
		return
	}

	amount, err := money.ParseExternal(req.Money.Amount, req.Money.Currency)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_money",
		})
		return
	}

	result, err := s.wagering.ProcessTransaction(r.Context(), idempotencyKey, application.WageringRequest{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		Amount:                         amount,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	})
	if err != nil {
		if errors.Is(err, application.ErrIdempotencyKeyRequired) || errors.Is(err, application.ErrInvalidWagerRequest) {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid_request",
			})
			return
		}
		if errors.Is(err, database.ErrWalletNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "wallet_not_found",
			})
			return
		}
		if errors.Is(err, database.ErrIdempotencyConflict) || errors.Is(err, database.ErrExternalTransactionConflict) {
			failureCode := "idempotency_conflict"
			if errors.Is(err, database.ErrExternalTransactionConflict) {
				failureCode = "external_transaction_conflict"
			}
			writeJSON(w, http.StatusConflict, map[string]any{
				"status":           "CONFLICT",
				"failureCode":      failureCode,
				"idempotentReplay": false,
			})
			return
		}

		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "internal_error",
		})
		return
	}

	response := map[string]any{
		"transactionId": result.TransactionID,
		"status":        result.Status,
		"balance": map[string]string{
			"amount":   result.Balance.String(),
			"currency": result.Balance.Currency(),
		},
		"idempotentReplay": result.IdempotentReplay,
	}

	if result.FailureCode != "" {
		response["failureCode"] = result.FailureCode
		if result.Status == "REJECTED" {
			writeJSON(w, http.StatusUnprocessableEntity, response)
			return
		}
	}

	writeJSON(w, http.StatusOK, response)
}
