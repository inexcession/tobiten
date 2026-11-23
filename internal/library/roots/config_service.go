package roots

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/storage"
)

// ConfigServiceOptions configures the concrete library configuration service.
type ConfigServiceOptions struct {
	DefaultMode    domain.LibraryMode
	Now            func() time.Time
	Notifier       jobs.JobNotifier
	OnRootsChanged func()
}

// ConfigService implements app.LibraryConfigService with PostgreSQL advisory-lock
// serialization and canonical root/intake overlap rejection.
type ConfigService struct {
	tx             storage.Transactor
	defaultMode    domain.LibraryMode
	now            func() time.Time
	notifier       jobs.JobNotifier
	onRootsChanged func()
}

var _ app.LibraryConfigService = (*ConfigService)(nil)

// NewConfigService constructs a ConfigService backed by tx.
func NewConfigService(tx storage.Transactor, opts ConfigServiceOptions) (*ConfigService, error) {
	if tx == nil {
		return nil, errors.New("transactor is required")
	}
	defaultMode := opts.DefaultMode
	if defaultMode == "" {
		defaultMode = domain.LibraryModeVirtual
	}
	if !defaultMode.IsValid() {
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidLibraryMode, defaultMode)
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &ConfigService{
		tx:             tx,
		defaultMode:    defaultMode,
		now:            nowFn,
		notifier:       opts.Notifier,
		onRootsChanged: opts.OnRootsChanged,
	}, nil
}

func (s *ConfigService) notifyRootsChanged() {
	if s != nil && s.onRootsChanged != nil {
		s.onRootsChanged()
	}
}

// CreateLibrary validates mode/intake/root invariants, checks canonical overlap against
// all existing roots and intakes (and among the new library's own paths), and persists
// the library and its initial roots atomically.
func (s *ConfigService) CreateLibrary(
	ctx context.Context,
	principal *domain.Principal,
	req app.CreateLibraryRequest,
) (*app.CreateLibraryResult, error) {
	if err := app.RequirePermission(principal, domain.PermissionLibraryAdmin); err != nil {
		return nil, err
	}
	if s == nil || s.tx == nil {
		return nil, fmt.Errorf("%w: library configuration service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryName)
	}

	mode, err := s.resolveCreateMode(req.Mode, req.IntakePath)
	if err != nil {
		return nil, err
	}
	if err := validateCreateLibraryOptions(req); err != nil {
		return nil, err
	}

	var result *app.CreateLibraryResult
	err = s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		if err := repo.LockConfiguration(ctx); err != nil {
			return err
		}

		intakeIdentity, canonRoots, err := resolveNewLibraryPaths(ctx, repo, mode, req.IntakePath, req.Roots)
		if err != nil {
			return err
		}

		result, err = persistNewLibraryAndRoots(ctx, repo, mode, req, intakeIdentity, canonRoots, s.now().UTC())
		return err
	})
	if err != nil {
		return nil, err
	}
	s.notifyRootsChanged()
	return result, nil
}

func resolveNewLibraryPaths(
	ctx context.Context,
	repo storage.LibraryRepository,
	mode domain.LibraryMode,
	rawIntake []byte,
	rawRoots [][]byte,
) (*domain.PathIdentity, [][]byte, error) {
	existing, err := CollectExistingPaths(ctx, repo, CollectOptions{})
	if err != nil {
		return nil, nil, err
	}

	var intakeIdentity *domain.PathIdentity
	if mode == domain.LibraryModeManaged {
		canonIntake, err := CheckPathOverlap(rawIntake, existing)
		if err != nil {
			return nil, nil, err
		}
		id, err := domain.NewRootPathIdentity(canonIntake)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}
		intakeIdentity = &id
		existing = append(existing, ExistingPath{
			Kind: PathKindIntake,
			Path: canonIntake,
		})
	}

	canonRoots := make([][]byte, 0, len(rawRoots))
	for _, rawRoot := range rawRoots {
		canonRoot, err := CheckPathOverlap(rawRoot, existing)
		if err != nil {
			return nil, nil, err
		}
		canonRoots = append(canonRoots, canonRoot)
		existing = append(existing, ExistingPath{
			Kind: PathKindRoot,
			Path: canonRoot,
		})
	}

	return intakeIdentity, canonRoots, nil
}

func persistNewLibraryAndRoots(
	ctx context.Context,
	repo storage.LibraryRepository,
	mode domain.LibraryMode,
	req app.CreateLibraryRequest,
	intakeIdentity *domain.PathIdentity,
	canonRoots [][]byte,
	createdAt time.Time,
) (*app.CreateLibraryResult, error) {
	lib := domain.Library{
		Name:                  req.Name,
		Mode:                  mode,
		IntakePath:            intakeIdentity,
		AutoMetadataWriteback: req.AutoMetadataWriteback,
		ResolverChain:         req.ResolverChain,
		MetadataPrecedence:    req.MetadataPrecedence,
		DefaultCoverPolicy:    req.DefaultCoverPolicy,
		CreatedAt:             createdAt,
	}
	if err := repo.CreateLibrary(ctx, &lib); err != nil {
		return nil, mapLibraryStorageError(err)
	}

	roots := make([]domain.LibraryRoot, 0, len(canonRoots))
	for _, canonRoot := range canonRoots {
		pathID, err := domain.NewRootPathIdentity(canonRoot)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}
		root := domain.LibraryRoot{
			LibraryID: lib.ID,
			Path:      pathID,
			State:     domain.LibraryRootStateOnline,
		}
		if err := repo.CreateLibraryRoot(ctx, &root); err != nil {
			return nil, mapLibraryStorageError(err)
		}
		roots = append(roots, root)
	}

	return &app.CreateLibraryResult{
		Library: lib,
		Roots:   roots,
	}, nil
}

// AddRoot adds a root directory to an existing library after verifying it does not
// overlap any existing root or intake of any library (including the target library).
func (s *ConfigService) AddRoot(
	ctx context.Context,
	principal *domain.Principal,
	req app.AddLibraryRootRequest,
) (*domain.LibraryRoot, error) {
	if err := app.RequirePermission(principal, domain.PermissionLibraryAdmin); err != nil {
		return nil, err
	}
	if s == nil || s.tx == nil {
		return nil, fmt.Errorf("%w: library configuration service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(req.LibraryID) == "" {
		return nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryID)
	}

	var created *domain.LibraryRoot
	err := s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		if err := repo.LockConfiguration(ctx); err != nil {
			return err
		}

		if err := repo.LockLibrary(ctx, req.LibraryID); err != nil {
			return mapLibraryStorageError(err)
		}

		existing, err := CollectExistingPaths(ctx, repo, CollectOptions{})
		if err != nil {
			return err
		}

		canonRoot, err := CheckPathOverlap(req.Path, existing)
		if err != nil {
			return err
		}

		pathID, err := domain.NewRootPathIdentity(canonRoot)
		if err != nil {
			return fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}
		root := domain.LibraryRoot{
			LibraryID: req.LibraryID,
			Path:      pathID,
			State:     domain.LibraryRootStateOnline,
		}
		if err := repo.CreateLibraryRoot(ctx, &root); err != nil {
			return mapLibraryStorageError(err)
		}
		created = &root
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.notifyRootsChanged()
	return created, nil
}

// RemoveRoot deletes an empty library root from its owning library when at least one
// sibling root remains. Removing a library's final root (ADR-0019 §3) or a root that
// still owns any media asset, including missing or retired ones (ADR-0019 §7), fails
// with app.ErrConflict and preserves all root and catalogue records.
func (s *ConfigService) RemoveRoot(
	ctx context.Context,
	principal *domain.Principal,
	req app.RemoveLibraryRootRequest,
) error {
	if err := app.RequirePermission(principal, domain.PermissionLibraryAdmin); err != nil {
		return err
	}
	if s == nil || s.tx == nil {
		return fmt.Errorf("%w: library configuration service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(req.LibraryID) == "" {
		return fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryID)
	}
	if strings.TrimSpace(req.RootID) == "" {
		return fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryRootID)
	}

	err := s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		if err := repo.LockConfiguration(ctx); err != nil {
			return err
		}

		root, err := repo.GetLibraryRootByID(ctx, req.RootID)
		if err != nil {
			return mapLibraryStorageError(err)
		}
		if root.LibraryID != req.LibraryID {
			return fmt.Errorf("%w: library root %q does not belong to library %q", app.ErrNotFound, req.RootID, req.LibraryID)
		}
		if err := repo.LockLibrary(ctx, req.LibraryID); err != nil {
			return mapLibraryStorageError(err)
		}

		roots, err := repo.ListLibraryRoots(ctx, req.LibraryID)
		if err != nil {
			return mapLibraryStorageError(err)
		}
		if len(roots) <= 1 {
			return fmt.Errorf("%w: cannot remove the final root of library %q", app.ErrConflict, req.LibraryID)
		}

		if err := repo.DeleteLibraryRoot(ctx, req.RootID); err != nil {
			return mapLibraryStorageError(err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.notifyRootsChanged()
	return nil
}

// SetManagedIntakePath updates a managed library's intake path after verifying it does
// not overlap any existing root (including the target library's own roots) or any other
// library's intake path.
func (s *ConfigService) SetManagedIntakePath(
	ctx context.Context,
	principal *domain.Principal,
	req app.SetManagedIntakePathRequest,
) (*domain.Library, error) {
	if err := app.RequirePermission(principal, domain.PermissionLibraryAdmin); err != nil {
		return nil, err
	}
	if s == nil || s.tx == nil {
		return nil, fmt.Errorf("%w: library configuration service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(req.LibraryID) == "" {
		return nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryID)
	}

	var updated *domain.Library
	err := s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		if err := repo.LockConfiguration(ctx); err != nil {
			return err
		}

		lib, err := repo.GetLibraryByID(ctx, req.LibraryID)
		if err != nil {
			return mapLibraryStorageError(err)
		}
		if lib.Mode != domain.LibraryModeManaged {
			return fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrVirtualLibraryWithIntake)
		}

		existing, err := CollectExistingPaths(ctx, repo, CollectOptions{
			ExcludeIntakeLibraryID: req.LibraryID,
		})
		if err != nil {
			return err
		}

		canonIntake, err := CheckPathOverlap(req.IntakePath, existing)
		if err != nil {
			return err
		}

		pathID, err := domain.NewRootPathIdentity(canonIntake)
		if err != nil {
			return fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}
		lib.IntakePath = &pathID
		if err := repo.UpdateLibrary(ctx, lib); err != nil {
			return mapLibraryStorageError(err)
		}
		updated = lib
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// RelocateRoot validates the target directory against canonical root/intake overlap rules
// (excluding the target root itself) and atomically checks for conflicting active SCAN or
// RELOCATE_ROOT jobs on the owning library before enqueueing a durable RELOCATE_ROOT job
// (ADR-0017 §§1–2, ADR-0019 §7, M1B-D7).
func (s *ConfigService) RelocateRoot(
	ctx context.Context,
	principal *domain.Principal,
	req app.RelocateRootRequest,
) (string, error) {
	if err := app.RequirePermission(principal, domain.PermissionLibraryAdmin); err != nil {
		return "", err
	}
	if s == nil || s.tx == nil {
		return "", fmt.Errorf("%w: library configuration service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(req.RootID) == "" {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryRootID)
	}
	if _, err := ResolveDirectoryPath(req.Path); err != nil {
		return "", err
	}

	var createdBy *string
	if principal != nil && strings.TrimSpace(principal.User.ID) != "" {
		uid := principal.User.ID
		createdBy = &uid
	}

	var jobID string
	err := s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		libRepo := tx.Libraries()
		jobRepo := tx.Jobs()
		if libRepo == nil || jobRepo == nil {
			return fmt.Errorf("%w: library configuration repositories are unavailable", app.ErrServiceBusy)
		}
		if err := libRepo.LockConfiguration(ctx); err != nil {
			return err
		}

		root, err := libRepo.GetLibraryRootByID(ctx, req.RootID)
		if err != nil {
			return mapLibraryStorageError(err)
		}
		if err := libRepo.LockLibrary(ctx, root.LibraryID); err != nil {
			return mapLibraryStorageError(err)
		}

		existing, err := CollectExistingPaths(ctx, libRepo, CollectOptions{
			ExcludeRootID: root.ID,
		})
		if err != nil {
			return err
		}
		canonTarget, err := CheckPathOverlap(req.Path, existing)
		if err != nil {
			return err
		}
		if _, err := domain.NewRootPathIdentity(canonTarget); err != nil {
			return fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}

		reqJob, err := buildRelocateTriggerRequest(root.LibraryID, root.ID, canonTarget, TriggerRelocateOptions{
			CreatedBy: createdBy,
		})
		if err != nil {
			return err
		}
		jobID, err = jobs.TriggerJob(ctx, jobRepo, nil, reqJob, s.now())
		return err
	})
	if err != nil {
		return "", err
	}

	if s.notifier != nil {
		s.notifier.Notify(domain.JobKindRelocateRoot)
	}
	return jobID, nil
}

func (s *ConfigService) resolveCreateMode(mode domain.LibraryMode, intakePath []byte) (domain.LibraryMode, error) {
	if mode == "" {
		mode = s.defaultMode
	}
	if !mode.IsValid() {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrInvalidLibraryMode)
	}
	switch mode {
	case domain.LibraryModeVirtual:
		if len(intakePath) > 0 {
			return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrVirtualLibraryWithIntake)
		}
	case domain.LibraryModeManaged:
		if len(intakePath) == 0 {
			return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrManagedLibraryRequiresIntake)
		}
	}
	return mode, nil
}

func validateCreateLibraryOptions(req app.CreateLibraryRequest) error {
	if req.ResolverChain != nil {
		if err := domain.ValidateResolverChain(req.ResolverChain); err != nil {
			return fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}
	}
	if req.MetadataPrecedence != "" && !req.MetadataPrecedence.IsValid() {
		return fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrInvalidMetadataPrecedence)
	}
	if req.DefaultCoverPolicy != "" && !req.DefaultCoverPolicy.IsValid() {
		return fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrInvalidCoverPolicy)
	}
	return nil
}

func mapLibraryStorageError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrNotFound):
		return fmt.Errorf("%w: %w", app.ErrNotFound, err)
	case errors.Is(err, storage.ErrAlreadyExists),
		errors.Is(err, storage.ErrLibraryRootInUse),
		errors.Is(err, storage.ErrLibraryInUse):
		return fmt.Errorf("%w: %w", app.ErrConflict, err)
	case errors.Is(err, domain.ErrEmptyLibraryID),
		errors.Is(err, domain.ErrEmptyLibraryRootID),
		errors.Is(err, domain.ErrEmptyLibraryName),
		errors.Is(err, domain.ErrInvalidLibraryMode),
		errors.Is(err, domain.ErrVirtualLibraryWithIntake),
		errors.Is(err, domain.ErrManagedLibraryRequiresIntake),
		errors.Is(err, domain.ErrInvalidPathIdentity),
		errors.Is(err, domain.ErrInvalidRootPath),
		errors.Is(err, domain.ErrEmptyResolverChain),
		errors.Is(err, domain.ErrInvalidResolverName),
		errors.Is(err, domain.ErrDuplicateResolverName),
		errors.Is(err, domain.ErrInvalidMetadataPrecedence),
		errors.Is(err, domain.ErrInvalidCoverPolicy),
		errors.Is(err, domain.ErrInvalidLibraryRootState):
		return fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	default:
		return err
	}
}
