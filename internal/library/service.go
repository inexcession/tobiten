// Package library implements library catalogue, multi-root status, and folder-browsing use-cases.
package library

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
)

var _ app.LibraryService = (*Service)(nil)

// ServiceOptions configures optional runtime dependencies on the library Service.
type ServiceOptions struct {
	ScanTrigger scanner.JobTrigger
}

// Service implements app.LibraryService over PostgreSQL repositories inside repeatable-read snapshots.
type Service struct {
	readTx      storage.ReadTransactor
	scanTrigger scanner.JobTrigger
}

// NewService constructs a LibraryService backed by readTx and opts.
func NewService(readTx storage.ReadTransactor, opts ServiceOptions) (*Service, error) {
	if readTx == nil {
		return nil, errors.New("library service read transactor cannot be nil")
	}
	return &Service{
		readTx:      readTx,
		scanTrigger: opts.ScanTrigger,
	}, nil
}

// ListLibraries returns a server-paginated list of libraries with root metadata redacted for non-admins.
func (s *Service) ListLibraries(ctx context.Context, principal *domain.Principal, filter app.LibraryFilter) (*app.LibraryPage, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return nil, err
	}
	limit, offset, err := normalizePagination(filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}

	var out *app.LibraryPage
	err = s.withReadTx(ctx, func(tx storage.ReadTx) error {
		libRepo, err := requireLibraryRepo(tx)
		if err != nil {
			return err
		}
		page, err := libRepo.ListLibrariesPaged(ctx, limit, offset)
		if err != nil {
			return mapServiceError(err)
		}

		libIDs := make([]string, len(page.Items))
		for i := range page.Items {
			libIDs[i] = page.Items[i].ID
		}
		roots, err := libRepo.ListLibraryRootsByLibraryIDs(ctx, libIDs)
		if err != nil {
			return mapServiceError(err)
		}

		rootsByLib := make(map[string][]domain.LibraryRoot, len(page.Items))
		for _, r := range roots {
			rootsByLib[r.LibraryID] = append(rootsByLib[r.LibraryID], r)
		}

		items := make([]app.Library, len(page.Items))
		for i, lib := range page.Items {
			items[i] = app.LibraryFromDomain(lib, rootsByLib[lib.ID], principal)
		}

		out = &app.LibraryPage{
			Items:      items,
			TotalCount: page.TotalCount,
			Limit:      page.Limit,
			Offset:     page.Offset,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetLibrary returns one configured library and its roots, redacting host filesystem paths unless principal holds library:admin.
func (s *Service) GetLibrary(ctx context.Context, principal *domain.Principal, id string) (*app.Library, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("%w: library id is required", app.ErrInvalidInput)
	}

	var out *app.Library
	err := s.withReadTx(ctx, func(tx storage.ReadTx) error {
		libRepo, err := requireLibraryRepo(tx)
		if err != nil {
			return err
		}
		lib, err := libRepo.GetLibraryByID(ctx, id)
		if err != nil {
			return mapServiceError(err)
		}
		roots, err := libRepo.ListLibraryRoots(ctx, id)
		if err != nil {
			return mapServiceError(err)
		}
		dto := app.LibraryFromDomain(*lib, roots, principal)
		out = &dto
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BrowseFolder returns one server-paginated slice of immediate child folders and files under
// filter.FolderPathBytes across all roots of filter.LibraryID, enforcing label restrictions.
func (s *Service) BrowseFolder(ctx context.Context, principal *domain.Principal, filter app.FolderBrowseFilter) (*app.FolderBrowseResult, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return nil, err
	}
	if strings.TrimSpace(filter.LibraryID) == "" {
		return nil, fmt.Errorf("%w: library id is required", app.ErrInvalidInput)
	}
	limit, offset, err := normalizePagination(filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	if len(filter.FolderPathBytes) > 0 {
		if err := domain.ValidateRelativePathBytes(filter.FolderPathBytes); err != nil {
			return nil, mapServiceError(err)
		}
	}

	var out *app.FolderBrowseResult
	err = s.withReadTx(ctx, func(tx storage.ReadTx) error {
		libRepo, err := requireLibraryRepo(tx)
		if err != nil {
			return err
		}
		if _, err := libRepo.GetLibraryByID(ctx, filter.LibraryID); err != nil {
			return mapServiceError(err)
		}
		catRepo, err := requireCatalogueRepo(tx)
		if err != nil {
			return err
		}
		raw, err := catRepo.BrowseFolder(ctx, catalogueReadScope(principal), storage.FolderBrowseFilter{
			LibraryID:       filter.LibraryID,
			FolderPathBytes: filter.FolderPathBytes,
			Limit:           limit,
			Offset:          offset,
		})
		if err != nil {
			return mapServiceError(err)
		}
		out = mapFolderBrowseResult(raw)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListSeries returns a server-paginated page of visible series matching filter under ADR-0006 §5 label rules.
func (s *Service) ListSeries(ctx context.Context, principal *domain.Principal, filter app.SeriesFilter) (*app.SeriesPage, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return nil, err
	}
	limit, offset, err := normalizePagination(filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}

	var out *app.SeriesPage
	err = s.withReadTx(ctx, func(tx storage.ReadTx) error {
		catRepo, err := requireCatalogueRepo(tx)
		if err != nil {
			return err
		}
		page, err := catRepo.ListSeriesPaged(ctx, catalogueReadScope(principal), storage.SeriesReadFilter{
			LibraryID: strings.TrimSpace(filter.LibraryID),
			Search:    filter.Search,
			Limit:     limit,
			Offset:    offset,
		})
		if err != nil {
			return mapServiceError(err)
		}

		items := make([]app.Series, len(page.Items))
		for i, ser := range page.Items {
			items[i] = app.SeriesFromDomain(ser)
		}
		out = &app.SeriesPage{
			Items:      items,
			TotalCount: page.TotalCount,
			Limit:      page.Limit,
			Offset:     page.Offset,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetSeries retrieves one visible series by ID, returning app.ErrNotFound when missing, restricted, trashed, or unallowed.
func (s *Service) GetSeries(ctx context.Context, principal *domain.Principal, id string) (*app.Series, error) {
	return getCatalogueEntity(
		ctx, s, principal, id, "series id",
		func(repo storage.CatalogueRepository, scope storage.CatalogueReadScope, targetID string) (*domain.Series, error) {
			return repo.GetSeriesByID(ctx, scope, targetID)
		},
		app.SeriesFromDomain,
	)
}

// ListVolumes returns a server-paginated page of visible volumes matching filter under ADR-0006 §5 label rules.
func (s *Service) ListVolumes(ctx context.Context, principal *domain.Principal, filter app.VolumeFilter) (*app.VolumePage, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return nil, err
	}
	limit, offset, err := normalizePagination(filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}

	var out *app.VolumePage
	err = s.withReadTx(ctx, func(tx storage.ReadTx) error {
		catRepo, err := requireCatalogueRepo(tx)
		if err != nil {
			return err
		}
		page, err := catRepo.ListVolumesPaged(ctx, catalogueReadScope(principal), storage.VolumeReadFilter{
			LibraryID: strings.TrimSpace(filter.LibraryID),
			SeriesID:  strings.TrimSpace(filter.SeriesID),
			Search:    filter.Search,
			Limit:     limit,
			Offset:    offset,
		})
		if err != nil {
			return mapServiceError(err)
		}

		items := make([]app.Volume, len(page.Items))
		for i, vol := range page.Items {
			items[i] = app.VolumeFromDomain(vol)
		}
		out = &app.VolumePage{
			Items:      items,
			TotalCount: page.TotalCount,
			Limit:      page.Limit,
			Offset:     page.Offset,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetVolume retrieves one visible volume by ID, returning app.ErrNotFound when missing, restricted, trashed, or unallowed.
func (s *Service) GetVolume(ctx context.Context, principal *domain.Principal, id string) (*app.Volume, error) {
	return getCatalogueEntity(
		ctx, s, principal, id, "volume id",
		func(repo storage.CatalogueRepository, scope storage.CatalogueReadScope, targetID string) (*domain.Volume, error) {
			return repo.GetVolumeByID(ctx, scope, targetID)
		},
		app.VolumeFromDomain,
	)
}

func getCatalogueEntity[DomainT any, DTO any](
	ctx context.Context,
	s *Service,
	principal *domain.Principal,
	id string,
	idLabel string,
	fetch func(storage.CatalogueRepository, storage.CatalogueReadScope, string) (*DomainT, error),
	project func(DomainT) DTO,
) (*DTO, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("%w: %s is required", app.ErrInvalidInput, idLabel)
	}

	var out *DTO
	err := s.withReadTx(ctx, func(tx storage.ReadTx) error {
		catRepo, err := requireCatalogueRepo(tx)
		if err != nil {
			return err
		}
		item, err := fetch(catRepo, catalogueReadScope(principal), id)
		if err != nil {
			return mapServiceError(err)
		}
		dto := project(*item)
		out = &dto
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ScanLibrary verifies library:admin permission and library existence, then enqueues a
// deduplicated durable SCAN job created by principal.User.ID (ADR-0017 §§1–2).
func (s *Service) ScanLibrary(ctx context.Context, principal *domain.Principal, libraryID string) (string, error) {
	if err := validateReadPrincipal(principal); err != nil {
		return "", err
	}
	if err := app.RequirePermission(principal, domain.PermissionLibraryAdmin); err != nil {
		return "", err
	}
	if _, err := scanner.ScanDedupeKey(libraryID); err != nil {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	if s == nil || s.scanTrigger == nil {
		return "", fmt.Errorf("%w: scan job trigger is unavailable", app.ErrServiceBusy)
	}

	err := s.withReadTx(ctx, func(tx storage.ReadTx) error {
		libRepo, err := requireLibraryRepo(tx)
		if err != nil {
			return err
		}
		if _, err := libRepo.GetLibraryByID(ctx, libraryID); err != nil {
			return mapServiceError(err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	createdBy := principal.User.ID
	return scanner.TriggerScan(ctx, s.scanTrigger, libraryID, scanner.TriggerScanOptions{
		CreatedBy: &createdBy,
	})
}

func (s *Service) withReadTx(ctx context.Context, fn func(tx storage.ReadTx) error) error {
	if s == nil || s.readTx == nil {
		return errors.New("library service read transactor is not initialized")
	}
	return s.readTx.WithRepeatableReadTx(ctx, fn)
}

func requireLibraryRepo(tx storage.ReadTx) (storage.LibraryRepository, error) {
	repo := tx.Libraries()
	if repo == nil {
		return nil, errors.New("read transaction missing library repository")
	}
	return repo, nil
}

func requireCatalogueRepo(tx storage.ReadTx) (storage.CatalogueRepository, error) {
	repo := tx.Catalogue()
	if repo == nil {
		return nil, errors.New("read transaction missing catalogue repository")
	}
	return repo, nil
}

func validateReadPrincipal(principal *domain.Principal) error {
	if principal == nil || strings.TrimSpace(principal.User.ID) == "" {
		return app.ErrUnauthorized
	}
	return nil
}

func normalizePagination(limit, offset int) (int, int, error) {
	if limit < 0 || offset < 0 || offset > math.MaxInt32 {
		return 0, 0, fmt.Errorf("%w: invalid pagination limit=%d offset=%d", app.ErrInvalidInput, limit, offset)
	}
	if limit == 0 {
		limit = app.DefaultLibraryPageLimit
	} else if limit > app.MaxLibraryPageLimit {
		limit = app.MaxLibraryPageLimit
	}
	return limit, offset, nil
}

func catalogueReadScope(principal *domain.Principal) storage.CatalogueReadScope {
	return storage.CatalogueReadScope{
		Principal:      principal,
		IncludeTrashed: false,
	}
}

func mapFolderBrowseResult(raw *storage.FolderBrowseResult) *app.FolderBrowseResult {
	entries := make([]app.FolderEntry, len(raw.Entries))
	for i, e := range raw.Entries {
		entry := app.FolderEntry{
			Kind:           string(e.Kind),
			NameDisplay:    e.Name.Display,
			NameHex:        hex.EncodeToString(e.Name.Bytes),
			RelPathDisplay: e.RelPath.Display,
			RelPathHex:     hex.EncodeToString(e.RelPath.Bytes),
			InvalidUTF8:    e.Name.InvalidUTF8 || e.RelPath.InvalidUTF8,
		}
		if e.Kind == storage.FolderEntryKindFile {
			assetID := e.AssetID
			volumeID := e.VolumeID
			rootID := e.LibraryRootID
			format := string(e.Format)
			fileSize := e.FileSize
			pageCount := e.PageCount
			entry.AssetID = &assetID
			entry.VolumeID = &volumeID
			entry.LibraryRootID = &rootID
			entry.Format = &format
			entry.FileSize = &fileSize
			entry.PageCount = &pageCount
		}
		entries[i] = entry
	}
	return &app.FolderBrowseResult{
		LibraryID:         raw.LibraryID,
		FolderPathDisplay: raw.FolderPath.Display,
		FolderPathHex:     hex.EncodeToString(raw.FolderPath.Bytes),
		InvalidUTF8:       raw.FolderPath.InvalidUTF8,
		Entries:           entries,
		TotalCount:        raw.TotalCount,
		Limit:             raw.Limit,
		Offset:            raw.Offset,
	}
}

func mapServiceError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return app.ErrNotFound
	case errors.Is(err, domain.ErrEmptyLibraryID),
		errors.Is(err, domain.ErrEmptySeriesID),
		errors.Is(err, domain.ErrEmptyVolumeID),
		errors.Is(err, domain.ErrInvalidAssetPath):
		return fmt.Errorf("%w: %v", app.ErrInvalidInput, err)
	case errors.Is(err, domain.ErrEmptyUserID):
		return app.ErrUnauthorized
	default:
		return err
	}
}
