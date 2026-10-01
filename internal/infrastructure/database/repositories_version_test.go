package database

import (
	"testing"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
)

func TestBuildWalletBalanceChangedPayloadUsesBeforeAndAfterVersions(t *testing.T) {
	amount, err := money.FromCents(100, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	before, err := money.FromCents(1000, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	after, err := money.FromCents(900, "BRL")
	if err != nil {
		t.Fatal(err)
	}

	payload := buildWalletBalanceChangedPayload(
		uuid.New(),
		uuid.New(),
		"DEBIT",
		amount,
		before,
		after,
		1,
		2,
	)
	if payload.PreviousVersion != 1 || payload.WalletVersion != 2 {
		t.Fatalf("expected previous version 1 and wallet version 2, got previous=%d wallet=%d", payload.PreviousVersion, payload.WalletVersion)
	}
}
