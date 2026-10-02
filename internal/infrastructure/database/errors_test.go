package database

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{"nil", nil, false},
		{"deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), true},
		{"connection exception", &pgconn.PgError{Code: "08006"}, true},
		{"serialization failure", fmt.Errorf("commit: %w", &pgconn.PgError{Code: "40001"}), true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"dropped connection", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"check violation", &pgconn.PgError{Code: "23514"}, false},
		{"business error", ErrWalletAlreadyExists, false},
		{"plain error", errors.New("boom"), false},
		{"client canceled", context.Canceled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransient(tt.err); got != tt.transient {
				t.Fatalf("IsTransient(%v) = %v, want %v", tt.err, got, tt.transient)
			}
		})
	}
}
