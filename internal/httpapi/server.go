package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/edinorneto/backend-challenge-go/internal/observability"
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
	logger   *observability.Logger
	metrics  *observability.Metrics
}

func NewServer(
	wallets *application.WalletService,
	wagering *application.WageringService,
	middleware *auth.Middleware,
	pool *pgxpool.Pool,
	queueManager *sqs.QueueManager,
	options ...any,
) *Server {
	var logger *observability.Logger
	var metrics *observability.Metrics
	for _, option := range options {
		switch value := option.(type) {
		case *observability.Logger:
			logger = value
		case *observability.Metrics:
			metrics = value
		}
	}
	if logger == nil {
		logger = observability.NewLogger()
	}
	if metrics == nil {
		metrics = observability.NewMetrics()
	}
	return &Server{
		wallets:  wallets,
		wagering: wagering,
		auth:     middleware,
		postgres: pool,
		sqs:      queueManager,
		logger:   logger,
		metrics:  metrics,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health/live", s.liveHandler)
	mux.HandleFunc("/health/ready", s.readyHandler)
	if s.metrics != nil {
		mux.Handle("/metrics", s.metrics.Handler())
	}
	// Wallet operations are restricted to the internal service.
	mux.Handle("POST /wallets", s.auth.Require(http.HandlerFunc(s.walletsHandler), auth.RoleWalletInternal))
	mux.Handle("GET /wallets/{walletID}", s.auth.Require(http.HandlerFunc(s.getWalletHandler), auth.RoleWalletInternal))
	mux.Handle("GET /wallets/{walletID}/ledger", s.auth.Require(http.HandlerFunc(s.getLedgerHandler), auth.RoleWalletInternal))
	mux.Handle("POST /wallets/{walletID}/reconciliation", s.auth.Require(http.HandlerFunc(s.reconciliationHandler), auth.RoleWalletInternal))
	// Wagering operations are submitted only by providers, for their own provider ID.
	mux.Handle("POST /wagering/transactions", s.auth.Require(http.HandlerFunc(s.wageringHandler), auth.RoleWageringProvider))
	mux.Handle("GET /providers/{providerID}/wagering/transactions/{externalTransactionID}", s.auth.Require(http.HandlerFunc(s.getExternalTransactionHandler), auth.RoleWageringProvider))
	// Transaction reads: providers see their own transactions; the internal service sees all.
	mux.Handle("GET /wagering/transactions/{transactionID}", s.auth.RequireAny(http.HandlerFunc(s.getTransactionHandler), auth.RoleWageringProvider, auth.RoleWalletInternal))

	return loggingMiddleware(mux, s.logger, s.metrics)
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

	// Each dependency gets its own deadline and runs concurrently, so a hanging
	// PostgreSQL cannot make SQS look unavailable (or the reverse).
	postgresDone := make(chan string, 1)
	sqsDone := make(chan string, 1)
	go func() {
		postgresDone <- s.checkDependency(r.Context(), "postgres", func(ctx context.Context) error {
			if s.postgres == nil {
				return errors.New("postgres health checker is not configured")
			}
			return s.postgres.Ping(ctx)
		})
	}()
	go func() {
		sqsDone <- s.checkDependency(r.Context(), "sqs", func(ctx context.Context) error {
			if s.sqs == nil {
				return errors.New("sqs health checker is not configured")
			}
			return s.sqs.Check(ctx)
		})
	}()
	postgresStatus := <-postgresDone
	sqsStatus := <-sqsDone

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

var readinessCheckTimeout = 3 * time.Second

// checkDependency runs one readiness check and logs the failed dependency with a
// coarse reason. The raw error is not logged because it may contain hosts or
// database user names.
func (s *Server) checkDependency(parent context.Context, dependency string, check func(context.Context) error) string {
	ctx, cancel := context.WithTimeout(parent, readinessCheckTimeout)
	defer cancel()
	err := check(ctx)
	if err == nil {
		return "ok"
	}
	reason := "unavailable"
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = "timeout"
	}
	if s.logger != nil {
		s.logger.Error(parent, "readiness_check_failed", err, map[string]string{"dependency": dependency, "reason": reason})
	}
	if s.metrics != nil {
		s.metrics.Inc("readiness_check_failures_total")
	}
	return "error"
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

	if err := decodeSingleJSON(decoder, &req); err != nil {
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

		s.writeServerError(w, r, err)
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

func loggingMiddleware(next http.Handler, logger *observability.Logger, metrics *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := r.Header.Get("Correlation-ID")
		if correlationID == "" {
			correlationID = uuid.NewString()
		}
		r = r.WithContext(observability.WithCorrelationID(r.Context(), correlationID))
		w.Header().Set("Correlation-ID", correlationID)
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		if metrics != nil {
			metrics.Observe("http_request_duration", time.Since(start))
			metrics.Inc("http_requests_total")
			switch {
			case recorder.status >= 500:
				metrics.Inc("http_requests_5xx_total")
			case recorder.status >= 400:
				metrics.Inc("http_requests_4xx_total")
			default:
				metrics.Inc("http_requests_2xx_3xx_total")
			}
		}
		if logger != nil {
			fields := map[string]string{
				"method":   r.Method,
				"route":    r.URL.Path,
				"status":   strconv.Itoa(recorder.status),
				"duration": time.Since(start).String(),
			}
			if identity, ok := auth.IdentityFromContext(r.Context()); ok {
				fields["providerId"] = identity.ProviderID
			}
			logger.Info(r.Context(), "http_request", fields)
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	return r.ResponseWriter.Write(body)
}

func decodeSingleJSON(decoder *json.Decoder, target any) error {
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
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
		s.writeServerError(w, r, err)
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
		if errors.Is(err, database.ErrInvalidLedgerCursor) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_cursor"})
			return
		}
		s.writeServerError(w, r, err)
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
		s.writeServerError(w, r, err)
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
	identity, ok := auth.IdentityFromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "provider_identity_required"})
		return
	}
	transaction, err := s.wagering.GetTransaction(r.Context(), transactionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "transaction_not_found"})
			return
		}
		s.writeServerError(w, r, err)
		return
	}
	if !identity.HasRole(auth.RoleWalletInternal) && identity.ProviderID != transaction.ProviderID {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "provider_access_denied"})
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
		s.writeServerError(w, r, err)
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
	if err := decodeSingleJSON(decoder, &req); err != nil {
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
	// The authenticated identity is the only source of the provider. A body
	// providerId is optional; when present it must match, otherwise the request
	// is refused before any financial processing.
	if bodyProvider := strings.TrimSpace(req.ProviderID); bodyProvider != "" && bodyProvider != identity.ProviderID {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "provider_mismatch"})
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

		s.writeServerError(w, r, err)
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

// writeServerError answers an unexpected failure. A temporary database condition
// becomes 503 service_unavailable with Retry-After, so clients can tell it apart
// from a defect (500 internal_error) and retry with the same Idempotency-Key.
func (s *Server) writeServerError(w http.ResponseWriter, r *http.Request, err error) {
	transient := database.IsTransient(err)
	result := "internal_error"
	if transient {
		result = "service_unavailable"
	}
	if s.logger != nil {
		s.logger.Error(r.Context(), "request_failed", err, map[string]string{"route": r.Pattern, "result": result})
	}
	if transient {
		if s.metrics != nil {
			s.metrics.Inc("http_dependency_unavailable_total")
		}
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "service_unavailable"})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
}
