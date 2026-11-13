package app

import (
	"errors"
)

// Common application-service sentinel errors.
var (
	ErrNotFound       = errors.New("not found")
	ErrUnauthorized   = errors.New("unauthorized")
	ErrForbidden      = errors.New("forbidden")
	ErrConflict       = errors.New("conflict")
	ErrInvalidInput   = errors.New("invalid input")
	ErrSetupSealed    = errors.New("setup is sealed: admin account already exists")
	ErrNotImplemented = errors.New("not implemented")
)

// Services groups all core application service interfaces.
// Protocol adapters (Komga, OPDS, native REST, MCP) call these services exclusively (ADR-0018 §1).
type Services struct {
	Setup     SetupService
	BasicAuth BasicAuthenticator
	PATAuth   PATAuthenticator
	Library   LibraryService
	Reading   ReadingService
	Curation  CurationService
	Admin     AdminService
}
