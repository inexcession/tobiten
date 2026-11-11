package app

import (
	"context"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// NewStubServices constructs a full Services set backed by stub implementations.
func NewStubServices() *Services {
	return &Services{
		Library:  NewStubLibraryService(),
		Reading:  NewStubReadingService(),
		Curation: NewStubCurationService(),
		Admin:    NewStubAdminService(),
	}
}

// StubLibraryService implements LibraryService for testing and bootstrap composition.
type StubLibraryService struct {
	mu        sync.RWMutex
	Libraries []Library
	Series    []Series
	Volumes   []Volume
}

// NewStubLibraryService creates a new StubLibraryService with sensible defaults.
func NewStubLibraryService() *StubLibraryService {
	return &StubLibraryService{
		Libraries: []Library{
			{
				ID:        "lib-stub-1",
				Name:      "Default Manga",
				Path:      "/media/manga",
				Mode:      "VIRTUAL",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
		},
		Series: []Series{
			{
				ID:        "ser-stub-1",
				LibraryID: "lib-stub-1",
				Title:     "Sample Series",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
		},
		Volumes: []Volume{
			{
				ID:         "vol-stub-1",
				LibraryID:  "lib-stub-1",
				Title:      "Volume 1",
				NumberSort: 1.0,
				AssetCount: 1,
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			},
		},
	}
}

func (s *StubLibraryService) ListLibraries(_ context.Context, _ *domain.Principal) ([]Library, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Library, len(s.Libraries))
	copy(out, s.Libraries)
	return out, nil
}

func (s *StubLibraryService) GetLibrary(_ context.Context, _ *domain.Principal, id string) (*Library, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, l := range s.Libraries {
		if l.ID == id {
			return &l, nil
		}
	}
	return nil, ErrNotFound
}

func (s *StubLibraryService) ListSeries(_ context.Context, _ *domain.Principal, _ SeriesFilter) ([]Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Series, len(s.Series))
	copy(out, s.Series)
	return out, nil
}

func (s *StubLibraryService) GetSeries(_ context.Context, _ *domain.Principal, id string) (*Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ser := range s.Series {
		if ser.ID == id {
			return &ser, nil
		}
	}
	return nil, ErrNotFound
}

func (s *StubLibraryService) ListVolumes(_ context.Context, _ *domain.Principal, _ VolumeFilter) ([]Volume, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Volume, len(s.Volumes))
	copy(out, s.Volumes)
	return out, nil
}

func (s *StubLibraryService) GetVolume(_ context.Context, _ *domain.Principal, id string) (*Volume, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.Volumes {
		if v.ID == id {
			return &v, nil
		}
	}
	return nil, ErrNotFound
}

func (s *StubLibraryService) ScanLibrary(_ context.Context, _ *domain.Principal, libraryID string) (string, error) {
	return "job-scan-" + libraryID, nil
}

// StubReadingService implements ReadingService for testing.
type StubReadingService struct {
	mu       sync.RWMutex
	Progress map[string]ReadingProgress // key: volumeID
}

// NewStubReadingService creates a new StubReadingService.
func NewStubReadingService() *StubReadingService {
	return &StubReadingService{
		Progress: make(map[string]ReadingProgress),
	}
}

func (s *StubReadingService) GetProgress(_ context.Context, p *domain.Principal, volumeID string) (*ReadingProgress, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if prog, ok := s.Progress[volumeID]; ok {
		return &prog, nil
	}
	userID := "user-stub"
	if p != nil {
		userID = p.User.ID
	}
	return &ReadingProgress{
		UserID:           userID,
		VolumeID:         volumeID,
		Completed:        false,
		TotalProgression: 0,
		Locator: Locator{
			Type: "page",
			Page: 1,
		},
		UpdatedAt: time.Now().UTC(),
	}, nil
}

func (s *StubReadingService) UpdateProgress(_ context.Context, p *domain.Principal, req UpdateProgressRequest) (*ReadingProgress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	userID := "user-stub"
	if p != nil {
		userID = p.User.ID
	}
	completed := false
	if req.Completed != nil {
		completed = *req.Completed
	}
	prog := ReadingProgress{
		UserID:           userID,
		VolumeID:         req.VolumeID,
		Completed:        completed,
		TotalProgression: req.TotalProgression,
		Locator:          req.Locator,
		Device:           req.Device,
		Source:           req.Source,
		UpdatedAt:        time.Now().UTC(),
	}
	s.Progress[req.VolumeID] = prog
	return &prog, nil
}

func (s *StubReadingService) MarkAsRead(ctx context.Context, p *domain.Principal, volumeID string) (*ReadingProgress, error) {
	completed := true
	return s.UpdateProgress(ctx, p, UpdateProgressRequest{
		VolumeID:         volumeID,
		Completed:        &completed,
		TotalProgression: 1.0,
		Locator:          Locator{Type: "page", Page: 1},
	})
}

func (s *StubReadingService) MarkAsUnread(ctx context.Context, p *domain.Principal, volumeID string) error {
	completed := false
	_, err := s.UpdateProgress(ctx, p, UpdateProgressRequest{
		VolumeID:         volumeID,
		Completed:        &completed,
		TotalProgression: 0.0,
		Locator:          Locator{Type: "page", Page: 1},
	})
	return err
}

// StubCurationService implements CurationService for testing.
type StubCurationService struct {
	mu    sync.RWMutex
	Trash []TrashItem
}

// NewStubCurationService creates a new StubCurationService.
func NewStubCurationService() *StubCurationService {
	return &StubCurationService{
		Trash: make([]TrashItem, 0),
	}
}

func (s *StubCurationService) GetCover(_ context.Context, _ *domain.Principal, targetID string) (*CoverRendition, error) {
	return &CoverRendition{
		TargetID:    targetID,
		ContentType: "image/jpeg",
		Data:        []byte("fake-jpeg-cover-data"),
		ETag:        "\"etag-stub-1\"",
	}, nil
}

func (s *StubCurationService) HidePage(_ context.Context, _ *domain.Principal, _ HidePageRequest) error {
	return nil
}

func (s *StubCurationService) StageTrash(_ context.Context, _ *domain.Principal, req StageTrashRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Trash = append(s.Trash, TrashItem{
		ID:       req.ItemID,
		ItemType: req.ItemType,
		Title:    "Trashed " + req.ItemID,
		TaggedAt: time.Now().UTC(),
	})
	return nil
}

func (s *StubCurationService) ListTrash(_ context.Context, _ *domain.Principal) ([]TrashItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TrashItem, len(s.Trash))
	copy(out, s.Trash)
	return out, nil
}

func (s *StubCurationService) RestoreTrash(_ context.Context, _ *domain.Principal, itemID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := s.Trash[:0]
	for _, item := range s.Trash {
		if item.ID != itemID {
			filtered = append(filtered, item)
		}
	}
	s.Trash = filtered
	return nil
}

func (s *StubCurationService) PurgeTrash(_ context.Context, _ *domain.Principal, req PurgeTrashRequest) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removeSet := make(map[string]bool)
	for _, id := range req.ItemIDs {
		removeSet[id] = true
	}
	filtered := s.Trash[:0]
	for _, item := range s.Trash {
		if !removeSet[item.ID] {
			filtered = append(filtered, item)
		}
	}
	s.Trash = filtered
	return "job-purge-trash", nil
}

// StubAdminService implements AdminService for testing and first-run bootstrap.
type StubAdminService struct {
	mu     sync.RWMutex
	Sealed bool
	Users  []domain.User
	Jobs   []JobInfo
}

// NewStubAdminService creates a new StubAdminService.
func NewStubAdminService() *StubAdminService {
	return &StubAdminService{
		Sealed: false,
		Users: []domain.User{
			{
				ID:        domain.RoleAdminID,
				Username:  "admin",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
		},
		Jobs: []JobInfo{
			{
				ID:          "job-stub-1",
				Kind:        "scanner",
				State:       "FINISHED",
				ProgressPct: 100,
				CreatedAt:   time.Now().UTC(),
			},
		},
	}
}

func (s *StubAdminService) IsSetupSealed(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Sealed, nil
}

func (s *StubAdminService) SetSealed(sealed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Sealed = sealed
}

func (s *StubAdminService) SetupInitialAdmin(_ context.Context, req SetupAdminRequest) (*domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Sealed {
		return nil, ErrSetupSealed
	}
	if req.Username == "" || req.Password == "" {
		return nil, ErrInvalidInput
	}
	adminUser := domain.User{
		ID:        domain.RoleAdminID,
		Username:  req.Username,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	s.Users = append(s.Users, adminUser)
	s.Sealed = true
	return &adminUser, nil
}

func (s *StubAdminService) AuthenticateUser(_ context.Context, username, password string) (*domain.Principal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if username == "" || password == "" {
		return nil, ErrUnauthorized
	}
	// Stub accepts non-empty credentials for demo/bootstrap
	user := domain.User{
		ID:        "user-auth-stub",
		Username:  username,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	roles := domain.BuiltInRoles()
	return domain.NewPrincipal(user, roles, domain.CredentialKindPassword, "cred-auth-stub", nil), nil
}

func (s *StubAdminService) ListUsers(_ context.Context, _ *domain.Principal) ([]domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.User, len(s.Users))
	copy(out, s.Users)
	return out, nil
}

func (s *StubAdminService) GetUser(_ context.Context, _ *domain.Principal, id string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.Users {
		if u.ID == id {
			return &u, nil
		}
	}
	return nil, ErrNotFound
}

func (s *StubAdminService) CreateUser(_ context.Context, _ *domain.Principal, req CreateUserRequest) (*domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Username == "" || req.Password == "" {
		return nil, ErrInvalidInput
	}
	u := domain.User{
		ID:        "user-" + req.Username,
		Username:  req.Username,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	s.Users = append(s.Users, u)
	return &u, nil
}

func (s *StubAdminService) DeleteUser(_ context.Context, _ *domain.Principal, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := s.Users[:0]
	for _, u := range s.Users {
		if u.ID != id {
			filtered = append(filtered, u)
		}
	}
	s.Users = filtered
	return nil
}

func (s *StubAdminService) CreatePAT(_ context.Context, p *domain.Principal, req CreatePATRequest) (*PATCreationResult, error) {
	userID := "user-stub"
	if p != nil {
		userID = p.User.ID
	}
	cred := &domain.Credential{
		ID:        "cred-pat-stub",
		UserID:    userID,
		Kind:      domain.CredentialKindPAT,
		Scopes:    req.Scopes,
		Label:     &req.Label,
		CreatedAt: time.Now().UTC(),
		ExpiresAt: req.ExpiresAt,
	}
	return &PATCreationResult{
		Credential: cred,
		Token:      "tb_pat_stub_secret_token",
	}, nil
}

func (s *StubAdminService) RevokeCredential(_ context.Context, _ *domain.Principal, _ string) error {
	return nil
}

func (s *StubAdminService) ListJobs(_ context.Context, _ *domain.Principal) ([]JobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]JobInfo, len(s.Jobs))
	copy(out, s.Jobs)
	return out, nil
}

func (s *StubAdminService) GetJob(_ context.Context, _ *domain.Principal, jobID string) (*JobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, j := range s.Jobs {
		if j.ID == jobID {
			return &j, nil
		}
	}
	return nil, ErrNotFound
}
