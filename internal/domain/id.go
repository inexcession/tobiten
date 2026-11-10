package domain

import (
	"github.com/google/uuid"
)

// NewID generates an application-level RFC 9562 UUIDv7 identifier in canonical
// lowercase hyphenated representation (ADR-0001 §6, S-01).
func NewID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// MustNewID generates a UUIDv7 identifier or panics if generation fails.
func MustNewID() string {
	id, err := NewID()
	if err != nil {
		panic(err)
	}
	return id
}
