package delegation

import "errors"

var (
	ErrNotFound            = errors.New("delegation record not found")
	ErrAlreadyExists       = errors.New("delegation record already exists")
	ErrRevisionConflict    = errors.New("delegation task revision changed concurrently")
	ErrIdempotencyConflict = errors.New("delegation idempotency key reused with different input")
	ErrInvalidTransition   = errors.New("invalid delegation task transition")
	ErrInvalidRewind       = errors.New("invalid delegation rewind target")
	ErrInvalidInput        = errors.New("invalid delegation input")
)
