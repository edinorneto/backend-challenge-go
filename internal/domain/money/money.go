package money

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidAmount    = errors.New("invalid monetary amount")
	ErrInvalidCurrency  = errors.New("invalid currency")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrOverflow         = errors.New("monetary overflow")
)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
var decimalPattern = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]{1,2})?$`)

type Money struct {
	amountCents int64
	currency    string
}

func New(amountCents int64, currency string) (Money, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))

	if !currencyPattern.MatchString(currency) {
		return Money{}, ErrInvalidCurrency
	}

	return Money{
		amountCents: amountCents,
		currency:    currency,
	}, nil
}

func FromCents(amountCents int64, currency string) (Money, error) {
	return New(amountCents, currency)
}

func Zero(currency string) (Money, error) {
	return New(0, currency)
}

func ParseExternal(amount string, currency string) (Money, error) {
	amount = strings.TrimSpace(amount)

	if amount == "" {
		return Money{}, ErrInvalidAmount
	}

	if strings.ContainsAny(amount, "eE") {
		return Money{}, ErrInvalidAmount
	}

	if !decimalPattern.MatchString(amount) {
		return Money{}, ErrInvalidAmount
	}

	if strings.HasPrefix(amount, "-") {
		return Money{}, ErrInvalidAmount
	}

	parts := strings.SplitN(amount, ".", 2)

	integerPart := parts[0]
	decimalPart := "00"

	if len(parts) == 2 {
		decimalPart = parts[1]

		if len(decimalPart) == 1 {
			decimalPart += "0"
		}
	}

	integerValue, err := strconv.ParseInt(integerPart, 10, 64)
	if err != nil {
		return Money{}, ErrInvalidAmount
	}

	decimalValue, err := strconv.ParseInt(decimalPart, 10, 64)
	if err != nil {
		return Money{}, ErrInvalidAmount
	}

	if integerValue > (math.MaxInt64-decimalValue)/100 {
		return Money{}, ErrOverflow
	}

	return New(integerValue*100+decimalValue, currency)
}

func (m Money) AmountCents() int64 {
	return m.amountCents
}

func (m Money) Currency() string {
	return m.currency
}

func (m Money) IsZero() bool {
	return m.amountCents == 0
}

func (m Money) IsNegative() bool {
	return m.amountCents < 0
}

func (m Money) Equal(other Money) bool {
	return m.currency == other.currency &&
		m.amountCents == other.amountCents
}

func (m Money) GreaterThan(other Money) (bool, error) {
	if err := m.ensureSameCurrency(other); err != nil {
		return false, err
	}

	return m.amountCents > other.amountCents, nil
}

func (m Money) Add(other Money) (Money, error) {
	if err := m.ensureSameCurrency(other); err != nil {
		return Money{}, err
	}

	if other.amountCents > 0 &&
		m.amountCents > math.MaxInt64-other.amountCents {
		return Money{}, ErrOverflow
	}

	if other.amountCents < 0 &&
		m.amountCents < math.MinInt64-other.amountCents {
		return Money{}, ErrOverflow
	}

	return Money{
		amountCents: m.amountCents + other.amountCents,
		currency:    m.currency,
	}, nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if err := m.ensureSameCurrency(other); err != nil {
		return Money{}, err
	}

	if other.amountCents == math.MinInt64 {
		return Money{}, ErrOverflow
	}

	return m.Add(Money{
		amountCents: -other.amountCents,
		currency:    other.currency,
	})
}

func (m Money) String() string {
	sign := ""

	cents := m.amountCents

	if cents < 0 {
		sign = "-"
		cents = -cents
	}

	return fmt.Sprintf(
		"%s%d.%02d",
		sign,
		cents/100,
		cents%100,
	)
}

func (m Money) ensureSameCurrency(other Money) error {
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}

	return nil
}
