package app

import (
	"context"

	"github.com/inexcession/tobiten/internal/domain"
)

// NewAuthorizingServices wraps non-nil application services in permission-enforcing
// decorators so adapters never bypass permission checks (ADR-0006 §4, ADR-0018 §1).
func NewAuthorizingServices(next *Services) *Services {
	if next == nil {
		return nil
	}
	out := &Services{
		Setup:       next.Setup,
		BasicAuth:   next.BasicAuth,
		PATAuth:     next.PATAuth,
		Session:     next.Session,
		SessionAuth: next.SessionAuth,
	}
	if next.Library != nil {
		out.Library = NewAuthorizingLibraryService(next.Library)
	}
	if next.Reading != nil {
		out.Reading = NewAuthorizingReadingService(next.Reading)
	}
	if next.Curation != nil {
		out.Curation = NewAuthorizingCurationService(next.Curation)
	}
	if next.Admin != nil {
		out.Admin = NewAuthorizingAdminService(next.Admin)
	}
	return out
}

type authorizingLibraryService struct {
	next LibraryService
}

// NewAuthorizingLibraryService wraps next with permission checks on every LibraryService method.
func NewAuthorizingLibraryService(next LibraryService) LibraryService {
	return &authorizingLibraryService{next: next}
}

func (s *authorizingLibraryService) ListLibraries(ctx context.Context, principal *domain.Principal) ([]Library, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() ([]Library, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.ListLibraries(ctx, principal)
	})
}

func (s *authorizingLibraryService) GetLibrary(ctx context.Context, principal *domain.Principal, id string) (*Library, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() (*Library, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.GetLibrary(ctx, principal, id)
	})
}

func (s *authorizingLibraryService) ListSeries(ctx context.Context, principal *domain.Principal, filter SeriesFilter) ([]Series, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() ([]Series, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.ListSeries(ctx, principal, filter)
	})
}

func (s *authorizingLibraryService) GetSeries(ctx context.Context, principal *domain.Principal, id string) (*Series, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() (*Series, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.GetSeries(ctx, principal, id)
	})
}

func (s *authorizingLibraryService) ListVolumes(ctx context.Context, principal *domain.Principal, filter VolumeFilter) ([]Volume, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() ([]Volume, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.ListVolumes(ctx, principal, filter)
	})
}

func (s *authorizingLibraryService) GetVolume(ctx context.Context, principal *domain.Principal, id string) (*Volume, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() (*Volume, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.GetVolume(ctx, principal, id)
	})
}

func (s *authorizingLibraryService) ScanLibrary(ctx context.Context, principal *domain.Principal, libraryID string) (string, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryAdmin, func() (string, error) {
		if s.next == nil {
			return "", ErrNotImplemented
		}
		return s.next.ScanLibrary(ctx, principal, libraryID)
	})
}

type authorizingReadingService struct {
	next ReadingService
}

// NewAuthorizingReadingService wraps next with permission checks on every ReadingService method.
func NewAuthorizingReadingService(next ReadingService) ReadingService {
	return &authorizingReadingService{next: next}
}

func (s *authorizingReadingService) GetProgress(ctx context.Context, principal *domain.Principal, volumeID string) (*ReadingProgress, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() (*ReadingProgress, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.GetProgress(ctx, principal, volumeID)
	})
}

func (s *authorizingReadingService) UpdateProgress(ctx context.Context, principal *domain.Principal, req UpdateProgressRequest) (*ReadingProgress, error) {
	return AuthorizeValue(principal, domain.PermissionProgressWrite, func() (*ReadingProgress, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.UpdateProgress(ctx, principal, req)
	})
}

func (s *authorizingReadingService) MarkAsRead(ctx context.Context, principal *domain.Principal, volumeID string) (*ReadingProgress, error) {
	return AuthorizeValue(principal, domain.PermissionProgressWrite, func() (*ReadingProgress, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.MarkAsRead(ctx, principal, volumeID)
	})
}

func (s *authorizingReadingService) MarkAsUnread(ctx context.Context, principal *domain.Principal, volumeID string) error {
	return Authorize(principal, domain.PermissionProgressWrite, func() error {
		if s.next == nil {
			return ErrNotImplemented
		}
		return s.next.MarkAsUnread(ctx, principal, volumeID)
	})
}

type authorizingCurationService struct {
	next CurationService
}

var (
	trashStageRestorePermissions = []domain.Permission{domain.PermissionMetadataEdit, domain.PermissionLibraryAdmin}
	trashListPermissions         = []domain.Permission{domain.PermissionMetadataEdit, domain.PermissionLibraryAdmin, domain.PermissionTrashPurge}
)

// NewAuthorizingCurationService wraps next with permission checks on every CurationService method.
func NewAuthorizingCurationService(next CurationService) CurationService {
	return &authorizingCurationService{next: next}
}

func (s *authorizingCurationService) GetCover(ctx context.Context, principal *domain.Principal, targetID string) (*CoverRendition, error) {
	return AuthorizeValue(principal, domain.PermissionLibraryRead, func() (*CoverRendition, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.GetCover(ctx, principal, targetID)
	})
}

func (s *authorizingCurationService) HidePage(ctx context.Context, principal *domain.Principal, req HidePageRequest) error {
	return Authorize(principal, domain.PermissionMetadataEdit, func() error {
		if s.next == nil {
			return ErrNotImplemented
		}
		return s.next.HidePage(ctx, principal, req)
	})
}

func (s *authorizingCurationService) StageTrash(ctx context.Context, principal *domain.Principal, req StageTrashRequest) error {
	return AuthorizeAny(principal, trashStageRestorePermissions, func() error {
		if s.next == nil {
			return ErrNotImplemented
		}
		return s.next.StageTrash(ctx, principal, req)
	})
}

func (s *authorizingCurationService) ListTrash(ctx context.Context, principal *domain.Principal) ([]TrashItem, error) {
	return AuthorizeAnyValue(principal, trashListPermissions, func() ([]TrashItem, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.ListTrash(ctx, principal)
	})
}

func (s *authorizingCurationService) RestoreTrash(ctx context.Context, principal *domain.Principal, itemID string) error {
	return AuthorizeAny(principal, trashStageRestorePermissions, func() error {
		if s.next == nil {
			return ErrNotImplemented
		}
		return s.next.RestoreTrash(ctx, principal, itemID)
	})
}

func (s *authorizingCurationService) PurgeTrash(ctx context.Context, principal *domain.Principal, req PurgeTrashRequest) (string, error) {
	return AuthorizeValue(principal, domain.PermissionTrashPurge, func() (string, error) {
		if s.next == nil {
			return "", ErrNotImplemented
		}
		return s.next.PurgeTrash(ctx, principal, req)
	})
}

type authorizingAdminService struct {
	next AdminService
}

// NewAuthorizingAdminService wraps next with permission checks on AdminService operations.
func NewAuthorizingAdminService(next AdminService) AdminService {
	return &authorizingAdminService{next: next}
}

func (s *authorizingAdminService) IsSetupSealed(ctx context.Context) (bool, error) {
	if s.next == nil {
		return false, ErrNotImplemented
	}
	return s.next.IsSetupSealed(ctx)
}

func (s *authorizingAdminService) SetupInitialAdmin(ctx context.Context, req SetupAdminRequest) (*domain.User, error) {
	if s.next == nil {
		return nil, ErrNotImplemented
	}
	return s.next.SetupInitialAdmin(ctx, req)
}

func (s *authorizingAdminService) AuthenticateUser(ctx context.Context, username, password string) (*domain.Principal, error) {
	if s.next == nil {
		return nil, ErrNotImplemented
	}
	return s.next.AuthenticateUser(ctx, username, password)
}

func (s *authorizingAdminService) ListUsers(ctx context.Context, principal *domain.Principal) ([]domain.User, error) {
	return AuthorizeValue(principal, domain.PermissionUsersAdmin, func() ([]domain.User, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.ListUsers(ctx, principal)
	})
}

func (s *authorizingAdminService) GetUser(ctx context.Context, principal *domain.Principal, id string) (*domain.User, error) {
	return AuthorizeValue(principal, domain.PermissionUsersAdmin, func() (*domain.User, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.GetUser(ctx, principal, id)
	})
}

func (s *authorizingAdminService) CreateUser(ctx context.Context, principal *domain.Principal, req CreateUserRequest) (*domain.User, error) {
	return AuthorizeValue(principal, domain.PermissionUsersAdmin, func() (*domain.User, error) {
		if s.next == nil {
			return nil, ErrNotImplemented
		}
		return s.next.CreateUser(ctx, principal, req)
	})
}

func (s *authorizingAdminService) DeleteUser(ctx context.Context, principal *domain.Principal, id string) error {
	return Authorize(principal, domain.PermissionUsersAdmin, func() error {
		if s.next == nil {
			return ErrNotImplemented
		}
		return s.next.DeleteUser(ctx, principal, id)
	})
}

func (s *authorizingAdminService) CreatePAT(ctx context.Context, principal *domain.Principal, req CreatePATRequest) (*PATCreationResult, error) {
	if err := RequirePATScopes(principal, req.Scopes); err != nil {
		return nil, err
	}
	if s.next == nil {
		return nil, ErrNotImplemented
	}
	return s.next.CreatePAT(ctx, principal, req)
}

func (s *authorizingAdminService) RevokeCredential(ctx context.Context, principal *domain.Principal, credID string) error {
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	if s.next == nil {
		return ErrNotImplemented
	}
	return s.next.RevokeCredential(ctx, principal, credID)
}

func (s *authorizingAdminService) ListJobs(ctx context.Context, principal *domain.Principal) ([]JobInfo, error) {
	if err := requirePrincipal(principal); err != nil {
		return nil, err
	}
	if s.next == nil {
		return nil, ErrNotImplemented
	}
	return s.next.ListJobs(ctx, principal)
}

func (s *authorizingAdminService) GetJob(ctx context.Context, principal *domain.Principal, jobID string) (*JobInfo, error) {
	if err := requirePrincipal(principal); err != nil {
		return nil, err
	}
	if s.next == nil {
		return nil, ErrNotImplemented
	}
	return s.next.GetJob(ctx, principal, jobID)
}
