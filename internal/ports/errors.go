package ports

import "errors"

// Errors returned by the wagering repository that callers above the
// infrastructure layer need to tell apart. The database package exposes the
// same values, so errors.Is works with either name.
var (
	ErrWalletNotFound              = errors.New("wallet not found")
	ErrIdempotencyConflict         = errors.New("idempotency conflict")
	ErrExternalTransactionConflict = errors.New("external transaction conflict")
)
