package database

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// Every malformed cursor is ErrInvalidLedgerCursor (400 invalid_cursor), also
// when it is not valid base64. The cursor is decoded before any query.
func TestGetLedgerRejectsMalformedCursorAsInvalidCursor(t *testing.T) {
	repo := &WalletRepo{}
	for _, cursor := range []string{
		"!!!",
		"not base64 at all",
		base64.RawURLEncoding.EncodeToString([]byte("no-separator")),
		base64.RawURLEncoding.EncodeToString([]byte("not-a-time|" + uuid.NewString())),
		base64.RawURLEncoding.EncodeToString([]byte("2026-09-30T12:00:00Z|not-a-uuid")),
	} {
		_, _, err := repo.GetLedger(context.Background(), uuid.New(), cursor, 10)
		if !errors.Is(err, ErrInvalidLedgerCursor) {
			t.Fatalf("cursor %q: expected ErrInvalidLedgerCursor, got %v", cursor, err)
		}
	}
}
