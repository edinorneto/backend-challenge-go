package wallet

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

var (
	ErrInvalidWallet      = errors.New("invalid wallet")
	ErrInsufficientFunds  = errors.New("insufficient funds")
)

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  string
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func New(
	id uuid.UUID,
	playerID uuid.UUID,
	initialBalance money.Money,
	now time.Time,
) (*Wallet, error) {
	if id == uuid.Nil {
		return nil, ErrInvalidWallet
	}

	if playerID == uuid.Nil {
		return nil, ErrInvalidWallet
	}

	if initialBalance.IsNegative() {
		return nil, ErrInvalidWallet
	}

	if now.IsZero() {
		return nil, ErrInvalidWallet
	}

	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  initialBalance.Currency(),
		balance:   initialBalance,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// Rehydrate reconstrói a entidade a partir do banco,
// sem reaplicar nenhuma movimentação ou evento.
func Rehydrate(
	id uuid.UUID,
	playerID uuid.UUID,
	currency string,
	balance money.Money,
	version int64,
	createdAt time.Time,
	updatedAt time.Time,
) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, ErrInvalidWallet
	}

	if version < 1 {
		return nil, ErrInvalidWallet
	}

	if balance.IsNegative() {
		return nil, ErrInvalidWallet
	}

	if balance.Currency() != currency {
		return nil, ErrInvalidWallet
	}

	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  currency,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}, nil
}

func (w *Wallet) Debit(amount money.Money, now time.Time) error {
	if amount.IsZero() || amount.IsNegative() {
		return ErrInvalidWallet
	}

	if amount.Currency() != w.currency {
		return money.ErrCurrencyMismatch
	}

	canDebit, err := w.balance.GreaterThan(amount)
	if err != nil {
		return err
	}

	if !canDebit && !w.balance.Equal(amount) {
		return ErrInsufficientFunds
	}

	newBalance, err := w.balance.Subtract(amount)
	if err != nil {
		return err
	}

	if newBalance.IsNegative() {
		return ErrInsufficientFunds
	}

	w.balance = newBalance
	w.version++
	w.updatedAt = now

	return nil
}

func (w *Wallet) Credit(amount money.Money, now time.Time) error {
	if amount.IsZero() || amount.IsNegative() {
		return ErrInvalidWallet
	}

	if amount.Currency() != w.currency {
		return money.ErrCurrencyMismatch
	}

	newBalance, err := w.balance.Add(amount)
	if err != nil {
		return err
	}

	w.balance = newBalance
	w.version++
	w.updatedAt = now

	return nil
}

func (w *Wallet) ID() uuid.UUID {
	return w.id
}

func (w *Wallet) PlayerID() uuid.UUID {
	return w.playerID
}

func (w *Wallet) Currency() string {
	return w.currency
}

func (w *Wallet) Balance() money.Money {
	return w.balance
}

func (w *Wallet) Version() int64 {
	return w.version
}

func (w *Wallet) CreatedAt() time.Time {
	return w.createdAt
}

func (w *Wallet) UpdatedAt() time.Time {
	return w.updatedAt
}