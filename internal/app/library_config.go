package app

import (
	"context"

	"github.com/inexcession/tobiten/internal/domain"
)

// CreateLibraryRequest defines parameters for creating a library and optional initial roots
// with canonical root/intake overlap rejection (ADR-0002, ADR-0019).
type CreateLibraryRequest struct {
	Name                  string
	Mode                  domain.LibraryMode
	IntakePath            []byte
	Roots                 [][]byte
	AutoMetadataWriteback bool
	ResolverChain         []domain.ResolverName
	MetadataPrecedence    domain.MetadataPrecedence
	DefaultCoverPolicy    domain.CoverPolicy
}

// CreateLibraryResult contains the newly persisted library and its initial roots.
type CreateLibraryResult struct {
	Library domain.Library       `json:"library"`
	Roots   []domain.LibraryRoot `json:"roots"`
}

// AddLibraryRootRequest defines parameters for adding a root directory to an existing library.
type AddLibraryRootRequest struct {
	LibraryID string
	Path      []byte
}

// SetManagedIntakePathRequest defines parameters for setting or updating a managed library's intake path.
type SetManagedIntakePathRequest struct {
	LibraryID  string
	IntakePath []byte
}

// RelocateRootRequest defines parameters for relocating an existing library root to a new path.
type RelocateRootRequest struct {
	RootID string
	Path   []byte
}

// RemoveLibraryRootRequest defines parameters for removing a root directory from a library.
type RemoveLibraryRootRequest struct {
	LibraryID string
	RootID    string
}

// LibraryConfigService defines administrative use-cases for configuring libraries, roots,
// and managed intake paths.
type LibraryConfigService interface {
	CreateLibrary(ctx context.Context, principal *domain.Principal, req CreateLibraryRequest) (*CreateLibraryResult, error)
	AddRoot(ctx context.Context, principal *domain.Principal, req AddLibraryRootRequest) (*domain.LibraryRoot, error)
	RemoveRoot(ctx context.Context, principal *domain.Principal, req RemoveLibraryRootRequest) error
	SetManagedIntakePath(ctx context.Context, principal *domain.Principal, req SetManagedIntakePathRequest) (*domain.Library, error)
	RelocateRoot(ctx context.Context, principal *domain.Principal, req RelocateRootRequest) (string, error)
}
