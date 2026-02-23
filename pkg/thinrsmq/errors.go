package thinrsmq

import (
	"errors"
	"fmt"
)

var (
	ErrUnknownVersion = errors.New("thinrsmq: unknown envelope version")
	ErrMessageTrimmed = errors.New("thinrsmq: message has been trimmed (nil fields)")
	ErrMissingField   = errors.New("thinrsmq: missing required field")
)

// MissingFieldError wraps ErrMissingField with the field name.
type MissingFieldError struct {
	Field string
}

func (e *MissingFieldError) Error() string {
	return fmt.Sprintf("thinrsmq: missing required field '%s'", e.Field)
}

func (e *MissingFieldError) Unwrap() error {
	return ErrMissingField
}
