package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// LibraryRepository defines persistence operations for libraries and library roots (ADR-0002, ADR-0019).
type LibraryRepository interface {
	// Libraries
	CreateLibrary(ctx context.Context, library *domain.Library) error
	GetLibraryByID(ctx context.Context, id string) (*domain.Library, error)
	GetLibraryByName(ctx context.Context, name string) (*domain.Library, error)
	ListLibraries(ctx context.Context) ([]domain.Library, error)
	UpdateLibrary(ctx context.Context, library *domain.Library) error
	DeleteLibrary(ctx context.Context, id string) error

	// Library roots
	CreateLibraryRoot(ctx context.Context, root *domain.LibraryRoot) error
	GetLibraryRootByID(ctx context.Context, id string) (*domain.LibraryRoot, error)
	ListLibraryRoots(ctx context.Context, libraryID string) ([]domain.LibraryRoot, error)
	ListAllLibraryRoots(ctx context.Context) ([]domain.LibraryRoot, error)
	UpdateLibraryRoot(ctx context.Context, root *domain.LibraryRoot) error
	UpdateLibraryRootStatus(ctx context.Context, id string, state domain.LibraryRootState, checkedAt time.Time, lastError *string) error
	DeleteLibraryRoot(ctx context.Context, id string) error
}
