package ledger

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

var ErrInvalidLedgerEntry = errors.New("invalid ledger entry")

type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

type WalletLedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	currency      string
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

func NewDebit(
	id uuid.UUID,
	walletID uuid.UUID,
	transactionID uuid.UUID,
	amount money.Money,
	balanceBefore money.Money,
	now time.Time,
) (*WalletLedgerEntry, error) {
	return newEntry(
		id,
		walletID,
		transactionID,
		DirectionDebit,
		amount,
		balanceBefore,
		now,
	)
}

func NewCredit(
	id uuid.UUID,
	walletID uuid.UUID,
	transactionID uuid.UUID,
	amount money.Money,
	balanceBefore money.Money,
	now time.Time,
) (*WalletLedgerEntry, error) {
	return newEntry(
		id,
		walletID,
		transactionID,
		DirectionCredit,
		amount,
		balanceBefore,
		now,
	)
}

func newEntry(
	id uuid.UUID,
	walletID uuid.UUID,
	transactionID uuid.UUID,
	direction Direction,
	amount money.Money,
	balanceBefore money.Money,
	now time.Time,
) (*WalletLedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return nil, ErrInvalidLedgerEntry
	}
	if now.IsZero() {
		return nil, ErrInvalidLedgerEntry
	}
	if amount.IsZero() || amount.IsNegative() {
		return nil, ErrInvalidLedgerEntry
	}
	if balanceBefore.IsNegative() {
		return nil, ErrInvalidLedgerEntry
	}
	if amount.Currency() != balanceBefore.Currency() {
		return nil, ErrInvalidLedgerEntry
	}

	var balanceAfter money.Money
	var err error

	switch direction {
	case DirectionDebit:
		balanceAfter, err = balanceBefore.Subtract(amount)
	case DirectionCredit:
		balanceAfter, err = balanceBefore.Add(amount)
	default:
		return nil, ErrInvalidLedgerEntry
	}
	if err != nil {
		return nil, err
	}
	if balanceAfter.IsNegative() {
		return nil, ErrInvalidLedgerEntry
	}

	return &WalletLedgerEntry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		currency:      amount.Currency(),
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     now,
	}, nil
}

func (e *WalletLedgerEntry) ID() uuid.UUID {
	return e.id
}

func (e *WalletLedgerEntry) WalletID() uuid.UUID {
	return e.walletID
}

func (e *WalletLedgerEntry) TransactionID() uuid.UUID {
	return e.transactionID
}

func (e *WalletLedgerEntry) Direction() Direction {
	return e.direction
}

func (e *WalletLedgerEntry) Amount() money.Money {
	return e.amount
}

func (e *WalletLedgerEntry) Currency() string {
	return e.currency
}

func (e *WalletLedgerEntry) BalanceBefore() money.Money {
	return e.balanceBefore
}

func (e *WalletLedgerEntry) BalanceAfter() money.Money {
	return e.balanceAfter
}

func (e *WalletLedgerEntry) CreatedAt() time.Time {
	return e.createdAt
}
