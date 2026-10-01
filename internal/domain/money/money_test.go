package money

import (
	"math"
	"testing"
)

func TestParseExternalValidValues(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		cents  int64
		output string
	}{
		{name: "integer", input: "12", cents: 1200, output: "12.00"},
		{name: "one decimal", input: "12.3", cents: 1230, output: "12.30"},
		{name: "two decimals", input: "12.34", cents: 1234, output: "12.34"},
		{name: "leading and trailing spaces", input: " 12.34 ", cents: 1234, output: "12.34"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := ParseExternal(test.input, "brl")
			if err != nil {
				t.Fatal(err)
			}
			if value.AmountCents() != test.cents || value.String() != test.output || value.Currency() != "BRL" {
				t.Fatalf("unexpected money: %+v", value)
			}
		})
	}
}

func TestParseExternalRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{"", "-1", "1.234", "1e2", "NaN", "Infinity", "1.", ".25"} {
		if _, err := ParseExternal(input, "BRL"); err == nil {
			t.Fatalf("expected %q to be rejected", input)
		}
	}
}

func TestMoneyOperations(t *testing.T) {
	left, err := New(1000, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(250, "BRL")
	if err != nil {
		t.Fatal(err)
	}

	sum, err := left.Add(right)
	if err != nil || sum.AmountCents() != 1250 {
		t.Fatalf("unexpected sum: %v, %v", sum, err)
	}
	difference, err := left.Subtract(right)
	if err != nil || difference.AmountCents() != 750 {
		t.Fatalf("unexpected difference: %v, %v", difference, err)
	}
	negative, err := left.Negate()
	if err != nil || negative.AmountCents() != -1000 {
		t.Fatalf("unexpected negation: %v, %v", negative, err)
	}
	less, err := right.LessThan(left)
	if err != nil || !less {
		t.Fatalf("expected right to be less than left")
	}
	greater, err := left.GreaterThan(right)
	if err != nil || !greater {
		t.Fatalf("expected left to be greater than right")
	}
}

func TestMoneyRejectsCurrencyMismatchAndInvalidCurrency(t *testing.T) {
	brl, err := New(100, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	usd, err := New(100, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brl.Add(usd); err != ErrCurrencyMismatch {
		t.Fatalf("expected currency mismatch, got %v", err)
	}
	if _, err := New(0, "BR"); err != ErrInvalidCurrency {
		t.Fatalf("expected invalid currency, got %v", err)
	}
	if _, err := New(0, "brl"); err != nil {
		t.Fatalf("expected lowercase currency to normalize: %v", err)
	}
}

func TestMoneyOverflowBoundaries(t *testing.T) {
	max, err := New(math.MaxInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	one, err := New(1, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := max.Add(one); err != ErrOverflow {
		t.Fatalf("expected addition overflow, got %v", err)
	}
	min, err := New(math.MinInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := min.Negate(); err != ErrOverflow {
		t.Fatalf("expected negation overflow, got %v", err)
	}
	if _, err := max.Subtract(min); err != ErrOverflow {
		t.Fatalf("expected subtraction overflow, got %v", err)
	}
	if _, err := ParseExternal("92233720368547758.08", "BRL"); err != ErrOverflow {
		t.Fatalf("expected parsing overflow, got %v", err)
	}
}

func TestMoneyZeroAndJSON(t *testing.T) {
	zero, err := Zero("BRL")
	if err != nil {
		t.Fatal(err)
	}
	if !zero.IsZero() || zero.IsNegative() || zero.String() != "0.00" {
		t.Fatalf("unexpected zero money: %v", zero)
	}
}
