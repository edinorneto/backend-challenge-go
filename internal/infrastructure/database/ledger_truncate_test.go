package database_test

import (
	"context"
	"strings"
	"testing"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database"
)

// TRUNCATE skips row-level triggers; the statement-level trigger of 000007 keeps
// the ledger append-only against it too.
func TestLedgerCannotBeTruncated(t *testing.T) {
	pool := testPool(t)
	repo := database.NewWalletRepo(pool)
	_, walletID := createTestWallet(t, repo, pool, "100.00")

	for _, statement := range []string{
		`TRUNCATE wallet_ledger_entries`,
		`TRUNCATE wallet_ledger_entries CASCADE`,
	} {
		_, err := pool.Exec(context.Background(), statement)
		if err == nil || !strings.Contains(err.Error(), "wallet ledger is append-only") {
			t.Fatalf("%s: expected the ledger trigger to refuse it, got %v", statement, err)
		}
	}
	var entries int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 1 {
		t.Fatalf("ledger entries changed: %d", entries)
	}
}
