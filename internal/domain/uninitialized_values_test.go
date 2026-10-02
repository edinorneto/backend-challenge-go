package domain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/ledger"
	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/edinorneto/backend-challenge-go/internal/domain/wallet"
)

// A zero-value Money (no currency) must never be accepted by the domain.
func TestDomainRejectsUninitializedMoney(t *testing.T) {
	var uninitialized money.Money
	now := time.Now().UTC()
	valid, err := money.ParseExternal("10.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := uninitialized.Add(valid); err == nil {
		t.Error("Add accepted an uninitialized Money")
	}
	if _, err := valid.Subtract(uninitialized); err == nil {
		t.Error("Subtract accepted an uninitialized Money")
	}
	if _, err := wallet.New(uuid.New(), uuid.New(), uninitialized, now); err == nil {
		t.Error("wallet.New accepted an uninitialized initial balance")
	}
	w, err := wallet.New(uuid.New(), uuid.New(), valid, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Credit(uninitialized, now); err == nil {
		t.Error("Credit accepted an uninitialized Money")
	}
	if err := w.Debit(uninitialized, now); err == nil {
		t.Error("Debit accepted an uninitialized Money")
	}
	if w.Balance() != valid || w.Version() != 1 {
		t.Errorf("rejected movements changed the wallet: %s v%d", w.Balance(), w.Version())
	}
	if _, err := ledger.NewDebit(uuid.New(), uuid.New(), uuid.New(), uninitialized, valid, now); err == nil {
		t.Error("ledger.NewDebit accepted an uninitialized amount")
	}
	if _, err := ledger.NewCredit(uuid.New(), uuid.New(), uuid.New(), valid, uninitialized, now); err == nil {
		t.Error("ledger.NewCredit accepted an uninitialized balance")
	}
	if _, err := wagertransaction.NewExternal(uuid.New(), "provider-a", "tx-1", "provider-a:tx-1", "hash", uuid.New(), uuid.New(),
		"round", "game", wagertransaction.Kind("BET"), uninitialized, now); err == nil {
		t.Error("NewExternal accepted an uninitialized amount")
	}
	if _, err := wagertransaction.NewOpening(uuid.New(), uuid.New(), uuid.New(), uninitialized, now); err == nil {
		t.Error("NewOpening accepted an uninitialized amount")
	}
}
