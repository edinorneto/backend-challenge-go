package wallet

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

func walletMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	value, err := money.ParseExternal(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestWalletNewAndRehydrate(t *testing.T) {
	now := time.Now().UTC()
	id := uuid.New()
	playerID := uuid.New()
	w, err := New(id, playerID, walletMoney(t, "100.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.ID() != id || w.PlayerID() != playerID || w.Version() != 1 || w.Balance().String() != "100.00" {
		t.Fatalf("unexpected wallet: %+v", w)
	}

	rehydrated, err := Rehydrate(id, playerID, "BRL", walletMoney(t, "75.00"), 3, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if rehydrated.Version() != 3 || rehydrated.Balance().String() != "75.00" {
		t.Fatalf("unexpected rehydrated wallet: %+v", rehydrated)
	}
}

func TestWalletCreditAndDebitAdvanceVersion(t *testing.T) {
	now := time.Now().UTC()
	w, err := New(uuid.New(), uuid.New(), walletMoney(t, "100.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Debit(walletMoney(t, "25.00"), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if w.Balance().String() != "75.00" || w.Version() != 2 {
		t.Fatalf("unexpected debit result: %s v%d", w.Balance(), w.Version())
	}
	if err := w.Credit(walletMoney(t, "10.00"), now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if w.Balance().String() != "85.00" || w.Version() != 3 {
		t.Fatalf("unexpected credit result: %s v%d", w.Balance(), w.Version())
	}
}

func TestWalletRejectsInvalidMovements(t *testing.T) {
	w, err := New(uuid.New(), uuid.New(), walletMoney(t, "100.00"), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	zero := walletMoney(t, "0.00")
	negative, err := money.New(-100, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	for name, amount := range map[string]money.Money{"zero": zero, "negative": negative} {
		if err := w.Debit(amount, time.Now().UTC()); err == nil {
			t.Fatalf("expected %s debit to fail", name)
		}
		if err := w.Credit(amount, time.Now().UTC()); err == nil {
			t.Fatalf("expected %s credit to fail", name)
		}
	}
	if err := w.Debit(walletMoney(t, "101.00"), time.Now().UTC()); err != ErrInsufficientFunds {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	usd, err := money.ParseExternal("1.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Credit(usd, time.Now().UTC()); err != money.ErrCurrencyMismatch {
		t.Fatalf("expected currency mismatch, got %v", err)
	}
}

func TestWalletRejectsNegativeAndInvalidRehydration(t *testing.T) {
	now := time.Now().UTC()
	negative, err := money.New(-1, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(uuid.New(), uuid.New(), negative, now); err != ErrInvalidWallet {
		t.Fatalf("expected negative opening balance to fail, got %v", err)
	}
	if _, err := Rehydrate(uuid.New(), uuid.New(), "BRL", walletMoney(t, "1.00"), 0, now, now); err != ErrInvalidWallet {
		t.Fatalf("expected invalid version to fail, got %v", err)
	}
	if _, err := Rehydrate(uuid.New(), uuid.New(), "USD", walletMoney(t, "1.00"), 1, now, now); err != ErrInvalidWallet {
		t.Fatalf("expected currency mismatch to fail, got %v", err)
	}
}
