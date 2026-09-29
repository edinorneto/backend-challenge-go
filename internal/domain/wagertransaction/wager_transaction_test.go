package wagertransaction

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

func TestNewExternalBetStartsPending(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := NewExternal(
		uuid.New(),
		"provider-a",
		"external-123",
		"provider-a:external-123",
		"hash",
		uuid.New(),
		uuid.New(),
		"round-1",
		"game-1",
		KindBet,
		amount,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if tx.Status() != StatusPending {
		t.Fatalf("expected PENDING, got %s", tx.Status())
	}

	if tx.Kind() != KindBet {
		t.Fatalf("expected BET, got %s", tx.Kind())
	}
}

func TestLossMustHaveZeroAmount(t *testing.T) {
	amount, err := money.ParseExternal("1.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewExternal(
		uuid.New(),
		"provider-a",
		"external-123",
		"provider-a:external-123",
		"hash",
		uuid.New(),
		uuid.New(),
		"round-1",
		"game-1",
		KindLoss,
		amount,
		time.Now().UTC(),
	)

	if err == nil {
		t.Fatal("expected LOSS with non-zero amount to fail")
	}
}

func TestOpeningCannotBeExternal(t *testing.T) {
	amount, err := money.ParseExternal("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewExternal(
		uuid.New(),
		"provider-a",
		"external-123",
		"provider-a:external-123",
		"hash",
		uuid.New(),
		uuid.New(),
		"round-1",
		"game-1",
		KindOpening,
		amount,
		time.Now().UTC(),
	)

	if err != ErrInvalidTransactionKind {
		t.Fatalf("expected ErrInvalidTransactionKind, got %v", err)
	}
}

func TestTerminalTransactionCannotTransitionAgain(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := NewExternal(
		uuid.New(),
		"provider-a",
		"external-123",
		"provider-a:external-123",
		"hash",
		uuid.New(),
		uuid.New(),
		"round-1",
		"game-1",
		KindBet,
		amount,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}

	balance, err := money.ParseExternal("75.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.MarkProcessed(balance, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	if err := tx.Reject("some_error", time.Now().UTC()); err != ErrTerminalTransaction {
		t.Fatalf("expected ErrTerminalTransaction, got %v", err)
	}
}

func TestRefundRequiresReference(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := NewExternal(
		uuid.New(),
		"provider-a",
		"refund-123",
		"provider-a:refund-123",
		"hash",
		uuid.New(),
		uuid.New(),
		"round-1",
		"game-1",
		KindRefund,
		amount,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.SetReference(""); err != ErrInvalidReference {
		t.Fatalf("expected ErrInvalidReference, got %v", err)
	}

	if err := tx.SetReference("bet-123"); err != nil {
		t.Fatal(err)
	}

	if tx.ReferenceExternalTransactionID() != "bet-123" {
		t.Fatalf(
			"expected reference bet-123, got %s",
			tx.ReferenceExternalTransactionID(),
		)
	}
}

func TestPendingReferenceCanResolveAndProcess(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := NewExternal(
		uuid.New(),
		"provider-a",
		"refund-456",
		"provider-a:refund-456",
		"hash",
		uuid.New(),
		uuid.New(),
		"round-1",
		"game-1",
		KindRefund,
		amount,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetReference("bet-456"); err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkPendingReference(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	referenceID := uuid.New()
	if err := tx.AssociateReference(referenceID); err != nil {
		t.Fatal(err)
	}

	balance, err := money.ParseExternal("125.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkProcessed(balance, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if tx.ReferenceTransactionID() != referenceID {
		t.Fatalf("expected reference %s, got %s", referenceID, tx.ReferenceTransactionID())
	}
}

func TestTerminalTransactionCannotAssociateReference(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewExternal(
		uuid.New(), "provider-a", "refund-789", "key", "hash",
		uuid.New(), uuid.New(), "round-1", "game-1", KindRefund, amount, time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkProcessed(amount, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := tx.AssociateReference(uuid.New()); err != ErrTerminalTransaction {
		t.Fatalf("expected terminal error, got %v", err)
	}
}
