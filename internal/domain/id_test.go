package domain_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/inexcession/tobiten/internal/domain"
)

func TestNewID(t *testing.T) {
	id, err := domain.NewID()
	if err != nil {
		t.Fatalf("unexpected error generating ID: %v", err)
	}

	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("failed to parse generated ID %q: %v", id, err)
	}

	if parsed.Version() != 7 {
		t.Errorf("expected UUID version 7, got version %d", parsed.Version())
	}

	if parsed.Variant() != uuid.RFC4122 {
		t.Errorf("expected RFC4122 variant, got %v", parsed.Variant())
	}

	mustID := domain.MustNewID()
	if len(mustID) != 36 {
		t.Errorf("expected UUID length 36, got %d", len(mustID))
	}
}
