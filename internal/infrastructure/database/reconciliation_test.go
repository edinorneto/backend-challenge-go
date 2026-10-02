package database_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
)

func TestReconcileIncludesOpeningAndMatchesLedger(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "1000.00")
	if _, err := repo.ProcessTransaction(context.Background(), testRequest(playerID, walletID, "reconcile-bet", "reconcile-bet-key", "reconcile-bet-hash", testMoney(t, "25.00"))); err != nil {
		t.Fatal(err)
	}

	result, err := repo.Reconcile(context.Background(), walletID)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Consistent || result.CheckedEntries != 2 ||
		result.StoredBalance.String() != "975.00" || result.CalculatedBalance.String() != "975.00" ||
		result.Difference.String() != "0.00" || result.Difference.Currency() != "BRL" {
		t.Fatalf("expected the README example (opening + bet, 975.00, 2 entries), got %+v", result)
	}
}

// A stored balance that no longer matches the ledger is reported, with
// difference = stored - calculated, and reconciliation never repairs it.
func TestReconcileReportsDivergenceWithoutChangingTheWallet(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	_, walletID := createTestWallet(t, repo, pool, "100.00")
	// Simulate corruption outside the application: the ledger still says 100.00.
	if _, err := pool.Exec(context.Background(), `UPDATE wallets SET balance_cents = 10250 WHERE id = $1`, walletID); err != nil {
		t.Fatal(err)
	}

	result, err := repo.Reconcile(context.Background(), walletID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Consistent || result.StoredBalance.String() != "102.50" ||
		result.CalculatedBalance.String() != "100.00" || result.Difference.String() != "2.50" {
		t.Fatalf("expected a 2.50 divergence (stored - calculated), got %+v", result)
	}
	var balance, version int64
	if err := pool.QueryRow(context.Background(), `SELECT balance_cents, version FROM wallets WHERE id = $1`, walletID).Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	if balance != 10250 || version != 1 {
		t.Fatalf("reconciliation must not change the wallet, got balance=%d version=%d", balance, version)
	}
}

// Balance and ledger must be read from one snapshot. Otherwise an operation that
// commits between the two reads makes a healthy wallet look divergent.
func TestReconcileUsesOneSnapshotUnderConcurrentWrites(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	playerID, walletID := createTestWallet(t, repo, pool, "1000.00")

	var stop atomic.Bool
	var writers sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			for n := 0; !stop.Load() && n < 2000; n++ {
				id := fmt.Sprintf("snapshot-%d-%d", writer, n)
				if _, err := repo.ProcessTransaction(context.Background(), testRequest(playerID, walletID, id, id+"-key", id+"-hash", testMoney(t, "0.01"))); err != nil {
					t.Error(err)
					return
				}
			}
		}(writer)
	}

	divergent := 0
	for i := 0; i < 300; i++ {
		result, err := repo.Reconcile(context.Background(), walletID)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Consistent {
			divergent++
		}
	}
	stop.Store(true)
	writers.Wait()
	if divergent != 0 {
		t.Fatalf("%d of 300 reconciliations reported a divergence on a healthy wallet", divergent)
	}
}
