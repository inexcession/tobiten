package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

const (
	// DefaultLibraryPageLimit is the default page size when library paging limit is zero.
	DefaultLibraryPageLimit = 50
	// MaxLibraryPageLimit is the hard maximum page size enforced on library reads (ADR-0020 §7).
	MaxLibraryPageLimit = 200
)

// LibraryPageResult holds one server-paginated slice of configured libraries together
// with the total count captured in the same statement snapshot (ADR-0020 §7).
type LibraryPageResult struct {
	Items      []domain.Library
	TotalCount int64
	Limit      int
	Offset     int
}

// LibraryRepository defines persistence operations for libraries and library roots (ADR-0002, ADR-0019).
type LibraryRepository interface {
	// Libraries
	CreateLibrary(ctx context.Context, library *domain.Library) error
	GetLibraryByID(ctx context.Context, id string) (*domain.Library, error)
	GetLibraryByName(ctx context.Context, name string) (*domain.Library, error)
	ListLibraries(ctx context.Context) ([]domain.Library, error)
	ListLibrariesPaged(ctx context.Context, limit, offset int) (*LibraryPageResult, error)
	UpdateLibrary(ctx context.Context, library *domain.Library) error
	DeleteLibrary(ctx context.Context, id string) error

	// Library roots
	CreateLibraryRoot(ctx context.Context, root *domain.LibraryRoot) error
	GetLibraryRootByID(ctx context.Context, id string) (*domain.LibraryRoot, error)
	ListLibraryRoots(ctx context.Context, libraryID string) ([]domain.LibraryRoot, error)
	ListLibraryRootsByLibraryIDs(ctx context.Context, libraryIDs []string) ([]domain.LibraryRoot, error)
	ListLibraryRootsForNoKeyUpdate(ctx context.Context, libraryID string) ([]domain.LibraryRoot, error)
	ListAllLibraryRoots(ctx context.Context) ([]domain.LibraryRoot, error)
	UpdateLibraryRoot(ctx context.Context, root *domain.LibraryRoot) error
	// UpdateLibraryRootStatus returns ErrNotFound if the root's identity changed since it was checked.
	UpdateLibraryRootStatus(ctx context.Context, root domain.LibraryRoot, state domain.LibraryRootState, checkedAt time.Time, lastError *string) error
	DeleteLibraryRoot(ctx context.Context, id string) error

	// Configuration serialization
	LockConfiguration(ctx context.Context) error
	LockLibrary(ctx context.Context, id string) error
	LockLibraryForKeyShare(ctx context.Context, id string) error
}
