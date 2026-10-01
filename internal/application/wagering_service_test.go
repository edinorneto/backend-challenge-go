package application

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/edinorneto/backend-challenge-go/internal/domain/money"
	"github.com/edinorneto/backend-challenge-go/internal/ports"
)

type stubWageringRepo struct {
	calls int
}

func (s *stubWageringRepo) ProcessTransaction(
	ctx context.Context,
	req ports.ProcessTransactionRequest,
) (ports.ProcessTransactionResult, error) {
	s.calls++
	return ports.ProcessTransactionResult{
		TransactionID: uuid.New(),
		Status:        "PROCESSED",
		Balance:       req.Amount,
	}, nil
}

func (s *stubWageringRepo) RetryPendingReference(
	ctx context.Context,
	transactionID uuid.UUID,
) (ports.ProcessTransactionResult, error) {
	return ports.ProcessTransactionResult{}, nil
}

func (s *stubWageringRepo) GetTransaction(context.Context, uuid.UUID) (ports.TransactionView, error) {
	return ports.TransactionView{}, nil
}

func (s *stubWageringRepo) GetTransactionByExternal(context.Context, string, string) (ports.TransactionView, error) {
	return ports.TransactionView{}, nil
}

func TestWageringPayloadHashIsDeterministic(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	playerID := uuid.New()
	walletID := uuid.New()
	first, err := computePayloadHash(
		"provider-a",
		"transaction-123",
		playerID,
		walletID,
		"round-987",
		"fortune-chimp",
		"BET",
		amount,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}

	second, err := computePayloadHash(
		"provider-a",
		"transaction-123",
		playerID,
		walletID,
		"round-987",
		"fortune-chimp",
		"BET",
		amount,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("expected deterministic hash")
	}
}

func TestWageringServiceProcessesRequest(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	repo := &stubWageringRepo{}
	service := NewWageringService(repo)
	result, err := service.ProcessTransaction(context.Background(), "provider-a:transaction-123", WageringRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              uuid.New(),
		WalletID:              uuid.New(),
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                amount,
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "PROCESSED" {
		t.Fatalf("expected PROCESSED, got %s", result.Status)
	}

	if repo.calls != 1 {
		t.Fatalf("expected one repo call, got %d", repo.calls)
	}
	if result.TransactionID == uuid.Nil {
		t.Fatal("expected transaction id")
	}
}

func TestWageringServiceRejectsOpeningAndUnknownKinds(t *testing.T) {
	repo := &stubWageringRepo{}
	service := NewWageringService(repo)
	amount, err := money.ParseExternal("1.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	base := ports.WageringRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "external-1",
		PlayerID:              uuid.New(),
		WalletID:              uuid.New(),
		RoundID:               "round",
		GameID:                "game",
		Amount:                amount,
	}

	for _, kind := range []string{"OPENING", "INVALID"} {
		request := base
		request.Kind = kind
		if _, err := service.ProcessTransaction(context.Background(), "idem-"+kind, request); !errors.Is(err, ErrInvalidWagerRequest) {
			t.Fatalf("kind %q: expected invalid wager request, got %v", kind, err)
		}
	}
	if repo.calls != 0 {
		t.Fatalf("repository must not be called for invalid external kind, got %d calls", repo.calls)
	}
}

func TestWageringServiceRequiresReferenceForReversals(t *testing.T) {
	amount, err := money.ParseExternal("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	service := NewWageringService(&stubWageringRepo{})
	for _, kind := range []string{"REFUND", "ROLLBACK"} {
		_, err := service.ProcessTransaction(context.Background(), "key-"+kind, WageringRequest{
			ProviderID: "provider-a", ExternalTransactionID: "transaction-" + kind,
			PlayerID: uuid.New(), WalletID: uuid.New(), RoundID: "round",
			GameID: "game", Kind: kind, Amount: amount,
		})
		if err != ErrInvalidWagerRequest {
			t.Fatalf("expected invalid request for %s without reference, got %v", kind, err)
		}
	}
}
