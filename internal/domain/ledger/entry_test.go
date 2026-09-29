package ledger

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

func TestLedgerDebitValidatesBalanceTransition(t *testing.T) {
	before, err := money.ParseExternal("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	entry, err := NewDebit(uuid.New(), uuid.New(), uuid.New(), amount, before, time.Now().UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.BalanceAfter().String() != "75.00" {
		t.Fatalf("expected 75.00, got %s", entry.BalanceAfter().String())
	}
}

func TestLedgerRejectsNegativeBalance(t *testing.T) {
	before, err := money.ParseExternal("10.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := NewDebit(uuid.New(), uuid.New(), uuid.New(), amount, before, time.Now().UTC()); err == nil {
		t.Fatal("expected invalid ledger entry for negative balance")
	}
}
