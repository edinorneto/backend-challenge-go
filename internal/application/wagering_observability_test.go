package application

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type scriptedWageringRepo struct {
	stubWageringRepo
	result ports.ProcessTransactionResult
	err    error
}

func (r *scriptedWageringRepo) ProcessTransaction(context.Context, ports.ProcessTransactionRequest) (ports.ProcessTransactionResult, error) {
	return r.result, r.err
}

func observedBet(t *testing.T) ports.WageringRequest {
	t.Helper()
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return ports.WageringRequest{
		ProviderID: "provider-a", ExternalTransactionID: "bet-1", PlayerID: uuid.New(), WalletID: uuid.New(),
		RoundID: "round", GameID: "game", Kind: "BET", Amount: amount,
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &output
}

// Both channels go through ProcessTransaction, so one place measures results
// by status, rejections by failure code, replays and latency, and logs the
// identifiers needed to trace the operation.
func TestWageringServiceRecordsResultsAndTraceableLog(t *testing.T) {
	logs := captureLogs(t)
	metrics := observability.NewMetrics()
	transactionID := uuid.New()
	balance, _ := money.FromCents(1000, "BRL")
	repo := &scriptedWageringRepo{}
	service := NewWageringService(repo, observability.NewLogger(), metrics)
	request := observedBet(t)
	ctx := observability.WithCorrelationID(context.Background(), "corr-123")

	repo.result = ports.ProcessTransactionResult{TransactionID: transactionID, Status: "PROCESSED", Balance: balance}
	if _, err := service.ProcessTransaction(ctx, "provider-a:bet-1", request); err != nil {
		t.Fatal(err)
	}
	repo.result = ports.ProcessTransactionResult{TransactionID: transactionID, Status: "PROCESSED", Balance: balance, IdempotentReplay: true}
	if _, err := service.ProcessTransaction(ctx, "provider-a:bet-1", request); err != nil {
		t.Fatal(err)
	}
	repo.result = ports.ProcessTransactionResult{TransactionID: uuid.New(), Status: "REJECTED", Balance: balance, FailureCode: "insufficient_funds"}
	if _, err := service.ProcessTransaction(ctx, "provider-a:bet-2", request); err != nil {
		t.Fatal(err)
	}

	for labels, expected := range map[string]uint64{"PROCESSED": 2, "REJECTED": 1} {
		if got := metrics.SnapshotLabeled("wager_results_total", map[string]string{"status": labels}); got != expected {
			t.Fatalf("wager_results_total{status=%q} = %d, want %d", labels, got, expected)
		}
	}
	if got := metrics.SnapshotLabeled("wager_rejections_total", map[string]string{"failure_code": "insufficient_funds"}); got != 1 {
		t.Fatalf("expected one insufficient_funds rejection, got %d", got)
	}
	if got := metrics.Snapshot("idempotency_replays_total"); got != 1 {
		t.Fatalf("expected one replay, got %d", got)
	}
	line := firstLine(logs.String(), `"message":"wager_transaction_completed"`)
	for _, expected := range []string{
		`"correlationId":"corr-123"`,
		`"transactionId":"` + transactionID.String() + `"`,
		`"walletId":"` + request.WalletID.String() + `"`,
		`"providerId":"provider-a"`,
		`"status":"PROCESSED"`,
	} {
		if !strings.Contains(line, expected) {
			t.Fatalf("operation log is missing %s: %s", expected, line)
		}
	}
	if strings.Contains(logs.String(), "25.00") || strings.Contains(logs.String(), request.PlayerID.String()) {
		t.Fatalf("operation logs must not contain amounts or player IDs: %s", logs.String())
	}
}

func TestWageringServiceClassifiesFailures(t *testing.T) {
	captureLogs(t)
	metrics := observability.NewMetrics()
	repo := &scriptedWageringRepo{}
	service := NewWageringService(repo, observability.NewLogger(), metrics)

	repo.err = ports.ErrIdempotencyConflict
	_, _ = service.ProcessTransaction(context.Background(), "provider-a:bet-1", observedBet(t))
	repo.err = fmt.Errorf("lock wallet: %w", &pgconn.PgError{Code: "40P01"})
	_, _ = service.ProcessTransaction(context.Background(), "provider-a:bet-1", observedBet(t))
	_, _ = service.ProcessTransaction(context.Background(), "", observedBet(t))

	for kind, expected := range map[string]uint64{"idempotency_conflict": 1, "infrastructure": 1, "invalid_request": 1} {
		if got := metrics.SnapshotLabeled("wager_errors_total", map[string]string{"kind": kind}); got != expected {
			t.Fatalf("wager_errors_total{kind=%q} = %d, want %d", kind, got, expected)
		}
	}
	if got := metrics.SnapshotLabeled("db_concurrency_conflicts_total", map[string]string{"sqlstate": "40P01"}); got != 1 {
		t.Fatalf("expected the deadlock to be counted as a concurrency conflict, got %d", got)
	}
}

func firstLine(logs, contains string) string {
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, contains) {
			return line
		}
	}
	return ""
}
