package application

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type reconcileOnlyRepo struct {
	view ports.ReconciliationView
}

func (r reconcileOnlyRepo) Create(context.Context, *wallet.Wallet) error { return nil }
func (r reconcileOnlyRepo) Get(context.Context, uuid.UUID) (*wallet.Wallet, error) {
	return nil, nil
}
func (r reconcileOnlyRepo) GetLedger(context.Context, uuid.UUID, string, int) ([]ports.LedgerEntryView, string, error) {
	return nil, "", nil
}
func (r reconcileOnlyRepo) Reconcile(context.Context, uuid.UUID) (ports.ReconciliationView, error) {
	return r.view, nil
}

func reconciliationView(t *testing.T, storedCents, calculatedCents int64) ports.ReconciliationView {
	t.Helper()
	stored, err := money.FromCents(storedCents, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	calculated, err := money.FromCents(calculatedCents, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	difference, err := stored.Subtract(calculated)
	if err != nil {
		t.Fatal(err)
	}
	return ports.ReconciliationView{
		WalletID:          uuid.New(),
		StoredBalance:     stored,
		CalculatedBalance: calculated,
		Difference:        difference,
		Consistent:        difference.IsZero(),
		CheckedEntries:    3,
	}
}

func TestReconcileReportsDivergenceInMetricAndLog(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	metrics := observability.NewMetrics()
	view := reconciliationView(t, 10250, 10000)
	service := NewWalletService(reconcileOnlyRepo{view: view}, observability.NewLogger(), metrics)

	result, err := service.Reconcile(context.Background(), view.WalletID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Consistent || result.Difference.String() != "2.50" {
		t.Fatalf("the response must carry the divergence, got %+v", result)
	}
	if metrics.Snapshot("reconciliation_divergences_total") != 1 || metrics.Snapshot("reconciliation_total") != 1 {
		t.Fatalf("expected one reconciliation and one divergence in the metrics")
	}
	logs := output.String()
	for _, expected := range []string{
		`"message":"reconciliation_divergence"`,
		`"walletId":"` + view.WalletID.String() + `"`,
		`"difference":"2.50"`,
		`"checkedEntries":"3"`,
	} {
		if !strings.Contains(logs, expected) {
			t.Fatalf("divergence log is missing %s: %s", expected, logs)
		}
	}
}

func TestReconcileConsistentWalletIsNotReported(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	metrics := observability.NewMetrics()
	view := reconciliationView(t, 10000, 10000)
	service := NewWalletService(reconcileOnlyRepo{view: view}, observability.NewLogger(), metrics)
	if _, err := service.Reconcile(context.Background(), view.WalletID); err != nil {
		t.Fatal(err)
	}
	if metrics.Snapshot("reconciliation_divergences_total") != 0 || strings.Contains(output.String(), "reconciliation_divergence") {
		t.Fatalf("a consistent wallet must not be reported as divergent: %s", output.String())
	}
}
