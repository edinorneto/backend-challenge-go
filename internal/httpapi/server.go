package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/application"
	"github.com/edinorneto/backend-challenge-go/internal/auth"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/sqs"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type postgresHealthChecker interface {
	Ping(context.Context) error
}

type sqsHealthChecker interface {
	Check(context.Context) error
}

type Server struct {
	wallets  *application.WalletService
	wagering *application.WageringService
	auth     *auth.Middleware
	postgres postgresHealthChecker
	sqs      sqsHealthChecker
}

func NewServer(
	wallets *application.WalletService,
	wagering *application.WageringService,
	middleware *auth.Middleware,
	pool *pgxpool.Pool,
	queueManager *sqs.QueueManager,
) *Server {
	return &Server{
		wallets:  wallets,
		wagering: wagering,
		auth:     middleware,
		postgres: pool,
		sqs:      queueManager,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health/live", s.liveHandler)
	mux.HandleFunc("/health/ready", s.readyHandler)
	mux.Handle("POST /wallets", s.auth.Require(http.HandlerFunc(s.walletsHandler), "wallet-internal"))
	mux.Handle("GET /wallets/{walletID}", s.auth.Require(http.HandlerFunc(s.getWalletHandler), "wallet-internal"))
	mux.Handle("GET /wallets/{walletID}/ledger", s.auth.Require(http.HandlerFunc(s.getLedgerHandler), "wallet-internal"))
	mux.Handle("POST /wallets/{walletID}/reconciliation", s.auth.Require(http.HandlerFunc(s.reconciliationHandler), "wallet-internal"))
	mux.Handle("POST /wagering/transactions", s.auth.Require(http.HandlerFunc(s.wageringHandler)))
	mux.Handle("GET /wagering/transactions/{transactionID}", s.auth.Require(http.HandlerFunc(s.getTransactionHandler)))
	mux.Handle("GET /providers/{providerID}/wagering/transactions/{externalTransactionID}", s.auth.Require(http.HandlerFunc(s.getExternalTransactionHandler)))

	return loggingMiddleware(mux)
}

func (s *Server) liveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	postgresStatus := "ok"
	if s.postgres == nil || s.postgres.Ping(ctx) != nil {
		postgresStatus = "error"
	}

	sqsStatus := "ok"
	if s.sqs == nil || s.sqs.Check(ctx) != nil {
		sqsStatus = "error"
	}

	if postgresStatus == "error" || sqsStatus == "error" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "not_ready",
			"checks": map[string]string{
				"postgres": postgresStatus,
				"sqs":      sqsStatus,
			},
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
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
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "wallet_not_found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":       wallet.ID(),
		"playerId": wallet.PlayerID(),
		"balance":  moneyResponse(wallet.Balance()),
		"version":  wallet.Version(),
	})
}

func (s *Server) getLedgerHandler(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(r.PathValue("walletID"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_wallet_id"})
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
			return
		}
	}
	entries, nextCursor, err := s.wallets.GetLedger(r.Context(), walletID, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "wallet_not_found"})
			return
		}
		if strings.Contains(err.Error(), "cursor") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_cursor"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	responseEntries := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		responseEntries = append(responseEntries, map[string]any{
			"id":            entry.ID,
			"transactionId": entry.TransactionID,
			"direction":     entry.Direction,
			"amount":        moneyResponse(entry.Amount),
			"balanceBefore": moneyResponse(entry.BalanceBefore),
			"balanceAfter":  moneyResponse(entry.BalanceAfter),
			"createdAt":     entry.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId":   walletID,
		"entries":    responseEntries,
		"nextCursor": nextCursor,
	})
}

func (s *Server) reconciliationHandler(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(r.PathValue("walletID"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_wallet_id"})
		return
	}
	result, err := s.wallets.Reconcile(r.Context(), walletID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "wallet_not_found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId":          result.WalletID,
		"storedBalance":     moneyResponse(result.StoredBalance),
		"calculatedBalance": moneyResponse(result.CalculatedBalance),
		"difference":        moneyResponse(result.Difference),
		"consistent":        result.Consistent,
		"checkedEntries":    result.CheckedEntries,
	})
}

func (s *Server) getTransactionHandler(w http.ResponseWriter, r *http.Request) {
	transactionID, err := uuid.Parse(r.PathValue("transactionID"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_transaction_id"})
		return
	}
	transaction, err := s.wagering.GetTransaction(r.Context(), transactionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "transaction_not_found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse(transaction))
}

func (s *Server) getExternalTransactionHandler(w http.ResponseWriter, r *http.Request) {
	identity, ok := auth.IdentityFromContext(r.Context())
	if !ok || identity.ProviderID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "provider_identity_required"})
		return
	}
	if identity.ProviderID != r.PathValue("providerID") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "provider_access_denied"})
		return
	}
	transaction, err := s.wagering.GetTransactionByExternal(r.Context(), identity.ProviderID, r.PathValue("externalTransactionID"))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "transaction_not_found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse(transaction))
}

func moneyResponse(value money.Money) map[string]string {
	return map[string]string{"amount": value.String(), "currency": value.Currency()}
}

func transactionResponse(transaction ports.TransactionView) map[string]any {
	response := map[string]any{
		"transactionId":         transaction.ID,
		"providerId":            transaction.ProviderID,
		"externalTransactionId": transaction.ExternalTransactionID,
		"playerId":              transaction.PlayerID,
		"walletId":              transaction.WalletID,
		"kind":                  transaction.Kind,
		"status":                transaction.Status,
		"amount":                moneyResponse(transaction.Amount),
		"createdAt":             transaction.CreatedAt,
		"updatedAt":             transaction.UpdatedAt,
	}
	if transaction.FailureCode != "" {
		response["failureCode"] = transaction.FailureCode
	}
	if transaction.ReferenceExternalID != "" {
		response["referenceExternalTransactionId"] = transaction.ReferenceExternalID
	}
	if transaction.ReferenceTransactionID != uuid.Nil {
		response["referenceTransactionId"] = transaction.ReferenceTransactionID
	}
	if !transaction.ResultBalance.IsZero() || transaction.Status == "PROCESSED" {
		response["result"] = map[string]any{
			"balance": moneyResponse(transaction.ResultBalance),
			"version": transaction.ResultWalletVersion,
		}
	}
	return response
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
	identity, ok := auth.IdentityFromContext(r.Context())
	if !ok || identity.ProviderID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "provider_identity_required"})
		return
	}

	result, err := s.wagering.ProcessTransaction(r.Context(), idempotencyKey, application.WageringRequest{
		ProviderID:                     identity.ProviderID,
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
		if result.Status == "PENDING_REFERENCE" {
			delete(response, "balance")
			writeJSON(w, http.StatusAccepted, response)
			return
		}
	}

	writeJSON(w, http.StatusOK, response)
}
