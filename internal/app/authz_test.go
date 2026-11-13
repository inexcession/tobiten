package app

import (
	"context"
	"errors"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
)

func builtInRoleByID(t *testing.T, id string) domain.Role {
	t.Helper()
	for _, r := range domain.BuiltInRoles() {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("built-in role %q not found", id)
	return domain.Role{}
}

func testUser(id string) domain.User {
	return domain.User{ID: id, Username: "user-" + id}
}

type spyLibraryService struct {
	calls int
}

func (s *spyLibraryService) ListLibraries(context.Context, *domain.Principal) ([]Library, error) {
	s.calls++
	return []Library{{ID: "lib-1"}}, nil
}

func (s *spyLibraryService) GetLibrary(context.Context, *domain.Principal, string) (*Library, error) {
	s.calls++
	return &Library{ID: "lib-1"}, nil
}

func (s *spyLibraryService) ListSeries(context.Context, *domain.Principal, SeriesFilter) ([]Series, error) {
	s.calls++
	return []Series{{ID: "ser-1"}}, nil
}

func (s *spyLibraryService) GetSeries(context.Context, *domain.Principal, string) (*Series, error) {
	s.calls++
	return &Series{ID: "ser-1"}, nil
}

func (s *spyLibraryService) ListVolumes(context.Context, *domain.Principal, VolumeFilter) ([]Volume, error) {
	s.calls++
	return []Volume{{ID: "vol-1"}}, nil
}

func (s *spyLibraryService) GetVolume(context.Context, *domain.Principal, string) (*Volume, error) {
	s.calls++
	return &Volume{ID: "vol-1"}, nil
}

func (s *spyLibraryService) ScanLibrary(context.Context, *domain.Principal, string) (string, error) {
	s.calls++
	return "job-scan-1", nil
}

type spyReadingService struct {
	readCalls  int
	writeCalls int
}

func (s *spyReadingService) GetProgress(context.Context, *domain.Principal, string) (*ReadingProgress, error) {
	s.readCalls++
	return &ReadingProgress{VolumeID: "vol-1"}, nil
}

func (s *spyReadingService) UpdateProgress(context.Context, *domain.Principal, UpdateProgressRequest) (*ReadingProgress, error) {
	s.writeCalls++
	return &ReadingProgress{VolumeID: "vol-1", TotalProgression: 0.5}, nil
}

func (s *spyReadingService) MarkAsRead(context.Context, *domain.Principal, string) (*ReadingProgress, error) {
	s.writeCalls++
	return &ReadingProgress{VolumeID: "vol-1", Completed: true}, nil
}

func (s *spyReadingService) MarkAsUnread(context.Context, *domain.Principal, string) error {
	s.writeCalls++
	return nil
}

type spyCurationService struct {
	readCalls  int
	writeCalls int
}

func (s *spyCurationService) GetCover(context.Context, *domain.Principal, string) (*CoverRendition, error) {
	s.readCalls++
	return &CoverRendition{ContentType: "image/webp"}, nil
}

func (s *spyCurationService) HidePage(context.Context, *domain.Principal, HidePageRequest) error {
	s.writeCalls++
	return nil
}

func (s *spyCurationService) StageTrash(context.Context, *domain.Principal, StageTrashRequest) error {
	s.writeCalls++
	return nil
}

func (s *spyCurationService) ListTrash(context.Context, *domain.Principal) ([]TrashItem, error) {
	s.readCalls++
	return []TrashItem{{ID: "trash-1"}}, nil
}

func (s *spyCurationService) RestoreTrash(context.Context, *domain.Principal, string) error {
	s.writeCalls++
	return nil
}

func (s *spyCurationService) PurgeTrash(context.Context, *domain.Principal, PurgeTrashRequest) (string, error) {
	s.writeCalls++
	return "job-purge-1", nil
}

type spyAdminService struct {
	publicCalls    int
	selfCalls      int
	createPATCalls int
	adminCalls     int
}

func (s *spyAdminService) IsSetupSealed(context.Context) (bool, error) {
	s.publicCalls++
	return true, nil
}

func (s *spyAdminService) SetupInitialAdmin(context.Context, SetupAdminRequest) (*domain.User, error) {
	s.publicCalls++
	return &domain.User{ID: "u-admin"}, nil
}

func (s *spyAdminService) AuthenticateUser(context.Context, string, string) (*domain.Principal, error) {
	s.publicCalls++
	return &domain.Principal{}, nil
}

func (s *spyAdminService) ListUsers(context.Context, *domain.Principal) ([]domain.User, error) {
	s.adminCalls++
	return []domain.User{{ID: "u-1"}}, nil
}

func (s *spyAdminService) GetUser(context.Context, *domain.Principal, string) (*domain.User, error) {
	s.adminCalls++
	return &domain.User{ID: "u-1"}, nil
}

func (s *spyAdminService) CreateUser(context.Context, *domain.Principal, CreateUserRequest) (*domain.User, error) {
	s.adminCalls++
	return &domain.User{ID: "u-2"}, nil
}

func (s *spyAdminService) DeleteUser(context.Context, *domain.Principal, string) error {
	s.adminCalls++
	return nil
}

func (s *spyAdminService) CreatePAT(_ context.Context, principal *domain.Principal, req CreatePATRequest) (*PATCreationResult, error) {
	s.selfCalls++
	s.createPATCalls++
	userID := ""
	if principal != nil {
		userID = principal.User.ID
	}
	scopes := make([]domain.PATScope, len(req.Scopes))
	copy(scopes, req.Scopes)
	return &PATCreationResult{
		Credential: &domain.Credential{
			ID:     "cred-pat-created",
			UserID: userID,
			Kind:   domain.CredentialKindPAT,
			Scopes: scopes,
		},
		Token: "tobiten_pat_created_secret",
	}, nil
}

func (s *spyAdminService) RevokeCredential(context.Context, *domain.Principal, string) error {
	s.selfCalls++
	return nil
}

func (s *spyAdminService) ListJobs(context.Context, *domain.Principal) ([]JobInfo, error) {
	s.selfCalls++
	return []JobInfo{{ID: "job-1"}}, nil
}

func (s *spyAdminService) GetJob(context.Context, *domain.Principal, string) (*JobInfo, error) {
	s.selfCalls++
	return &JobInfo{ID: "job-1"}, nil
}

func TestPermissionHelpers_FailClosedAndCheckPermissionsNotRoleNames(t *testing.T) {
	t.Parallel()

	// 1. Nil principal fails closed with ErrUnauthorized across all helpers and never runs callbacks.
	called := 0
	if err := RequirePermission(nil, domain.PermissionLibraryRead); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("RequirePermission(nil) = %v, want ErrUnauthorized", err)
	}
	if err := RequireAllPermissions(nil, domain.PermissionLibraryRead); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("RequireAllPermissions(nil) = %v, want ErrUnauthorized", err)
	}
	if err := RequireAnyPermission(nil, domain.PermissionLibraryRead); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("RequireAnyPermission(nil) = %v, want ErrUnauthorized", err)
	}
	if err := Authorize(nil, domain.PermissionLibraryRead, func() error {
		called++
		return nil
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(nil) = %v, want ErrUnauthorized", err)
	}
	if _, err := AuthorizeValue(nil, domain.PermissionLibraryRead, func() (int, error) {
		called++
		return 1, nil
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("AuthorizeValue(nil) = %v, want ErrUnauthorized", err)
	}
	if called != 0 {
		t.Fatalf("nil principal invoked callback %d times, want 0", called)
	}

	// 2. Empty and invalid permission inputs fail closed even for a full Admin.
	adminPrincipal := domain.NewPrincipal(testUser("u-admin"), []domain.Role{builtInRoleByID(t, domain.RoleAdminID)}, domain.CredentialKindPassword, "cred-admin", nil)
	if err := RequirePermission(adminPrincipal, domain.Permission("not:a:perm")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("RequirePermission(invalid) = %v, want ErrForbidden", err)
	}
	if err := RequireAllPermissions(adminPrincipal); !errors.Is(err, ErrForbidden) {
		t.Fatalf("RequireAllPermissions(empty) = %v, want ErrForbidden", err)
	}
	if err := RequireAnyPermission(adminPrincipal); !errors.Is(err, ErrForbidden) {
		t.Fatalf("RequireAnyPermission(empty) = %v, want ErrForbidden", err)
	}
	if err := AuthorizeAny(adminPrincipal, nil, func() error {
		called++
		return nil
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("AuthorizeAny(empty) = %v, want ErrForbidden", err)
	}
	if _, err := AuthorizeAnyValue(adminPrincipal, []domain.Permission{"bogus"}, func() (string, error) {
		called++
		return "ok", nil
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("AuthorizeAnyValue(invalid) = %v, want ErrForbidden", err)
	}
	if called != 0 {
		t.Fatalf("empty/invalid permission invoked callback %d times, want 0", called)
	}

	// 3. Role named "Admin" with empty permission set is DENIED (checks permissions, NEVER role names).
	spoofAdminRole := domain.Role{
		ID:          "role-spoof",
		Name:        domain.RoleAdminName,
		Permissions: nil,
	}
	spoofPrincipal := domain.NewPrincipal(testUser("u-spoof"), []domain.Role{spoofAdminRole}, domain.CredentialKindPassword, "cred-spoof", nil)
	if err := Authorize(spoofPrincipal, domain.PermissionLibraryRead, func() error {
		called++
		return nil
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("role named Admin with empty perms: err = %v, want ErrForbidden", err)
	}

	// 4. Custom non-standard role name holding a permission is ALLOWED (checks permissions, NEVER role names).
	customRole := domain.Role{
		ID:          "role-custom",
		Name:        "Tagger",
		Permissions: []domain.Permission{domain.PermissionMetadataEdit},
	}
	customPrincipal := domain.NewPrincipal(testUser("u-custom"), []domain.Role{customRole}, domain.CredentialKindPassword, "cred-custom", nil)
	if err := Authorize(customPrincipal, domain.PermissionMetadataEdit, func() error {
		called++
		return nil
	}); err != nil {
		t.Fatalf("custom role with MetadataEdit: err = %v, want nil", err)
	}
	if called != 1 {
		t.Fatalf("callback invocation count = %d, want 1", called)
	}

	// 5. RequireAllPermissions and RequireAnyPermission edge cases.
	readerPrincipal := domain.NewPrincipal(testUser("u-reader"), []domain.Role{builtInRoleByID(t, domain.RoleReaderID)}, domain.CredentialKindPassword, "cred-reader", nil)
	if err := RequireAllPermissions(readerPrincipal, domain.PermissionLibraryRead, domain.PermissionProgressWrite); err != nil {
		t.Fatalf("Reader RequireAll(read, progress:write) = %v, want nil", err)
	}
	if err := RequireAllPermissions(readerPrincipal, domain.PermissionLibraryRead, domain.PermissionMetadataEdit); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Reader RequireAll(read, metadata:edit) = %v, want ErrForbidden", err)
	}
	if err := RequireAnyPermission(readerPrincipal, domain.PermissionMetadataEdit, domain.PermissionProgressWrite); err != nil {
		t.Fatalf("Reader RequireAny(metadata:edit, progress:write) = %v, want nil", err)
	}

	// 6. Nil callbacks are safe when authorized.
	if err := Authorize(adminPrincipal, domain.PermissionLibraryAdmin, nil); err != nil {
		t.Fatalf("Authorize with nil fn = %v, want nil", err)
	}
	val, err := AuthorizeValue[string](adminPrincipal, domain.PermissionLibraryAdmin, nil)
	if err != nil || val != "" {
		t.Fatalf("AuthorizeValue with nil fn = (%q, %v), want (\"\", nil)", val, err)
	}
}

func TestAuthorizingServices_ReadScopedPATCannotWriteProgressOrMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	libSpy := &spyLibraryService{}
	readSpy := &spyReadingService{}
	curSpy := &spyCurationService{}
	adminSpy := &spyAdminService{}

	svcs := NewAuthorizingServices(&Services{
		Library:  libSpy,
		Reading:  readSpy,
		Curation: curSpy,
		Admin:    adminSpy,
	})

	// Admin user holding a read-scoped PAT: PAT scope caps permissions down to library:read only (M1-AC22).
	adminRole := builtInRoleByID(t, domain.RoleAdminID)
	readPATPrincipal := domain.NewPrincipal(testUser("u-admin"), []domain.Role{adminRole}, domain.CredentialKindPAT, "cred-pat-read", []domain.PATScope{domain.ScopeRead})

	// Permitted reads still work.
	if _, err := svcs.Library.ListLibraries(ctx, readPATPrincipal); err != nil {
		t.Fatalf("ListLibraries(read PAT) = %v, want nil", err)
	}
	if _, err := svcs.Library.ListSeries(ctx, readPATPrincipal, SeriesFilter{}); err != nil {
		t.Fatalf("ListSeries(read PAT) = %v, want nil", err)
	}
	if _, err := svcs.Reading.GetProgress(ctx, readPATPrincipal, "vol-1"); err != nil {
		t.Fatalf("GetProgress(read PAT) = %v, want nil", err)
	}
	if _, err := svcs.Curation.GetCover(ctx, readPATPrincipal, "vol-1"); err != nil {
		t.Fatalf("GetCover(read PAT) = %v, want nil", err)
	}
	if libSpy.calls != 2 || readSpy.readCalls != 1 || curSpy.readCalls != 1 {
		t.Fatalf("unexpected read spy counts: lib=%d read=%d cur=%d", libSpy.calls, readSpy.readCalls, curSpy.readCalls)
	}

	// Progress mutations must return ErrForbidden AND never invoke mutation callbacks/services.
	progressCallbackCalls := 0
	if err := Authorize(readPATPrincipal, domain.PermissionProgressWrite, func() error {
		progressCallbackCalls++
		return nil
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Authorize(ProgressWrite, read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Reading.UpdateProgress(ctx, readPATPrincipal, UpdateProgressRequest{VolumeID: "vol-1", TotalProgression: 0.4}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("UpdateProgress(read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Reading.MarkAsRead(ctx, readPATPrincipal, "vol-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("MarkAsRead(read PAT) = %v, want ErrForbidden", err)
	}
	if err := svcs.Reading.MarkAsUnread(ctx, readPATPrincipal, "vol-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("MarkAsUnread(read PAT) = %v, want ErrForbidden", err)
	}
	if progressCallbackCalls != 0 || readSpy.writeCalls != 0 {
		t.Fatalf("denied progress mutations invoked callback/service: cb=%d spy=%d", progressCallbackCalls, readSpy.writeCalls)
	}

	// Metadata and admin mutations must return ErrForbidden AND never invoke mutation callbacks/services.
	metadataCallbackCalls := 0
	if err := Authorize(readPATPrincipal, domain.PermissionMetadataEdit, func() error {
		metadataCallbackCalls++
		return nil
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Authorize(MetadataEdit, read PAT) = %v, want ErrForbidden", err)
	}
	if err := svcs.Curation.HidePage(ctx, readPATPrincipal, HidePageRequest{AssetID: "asset-1", PageIndex: 1, Suppressed: true}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("HidePage(read PAT) = %v, want ErrForbidden", err)
	}
	if err := svcs.Curation.StageTrash(ctx, readPATPrincipal, StageTrashRequest{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("StageTrash(read PAT) = %v, want ErrForbidden", err)
	}
	if err := svcs.Curation.RestoreTrash(ctx, readPATPrincipal, "trash-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("RestoreTrash(read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Curation.PurgeTrash(ctx, readPATPrincipal, PurgeTrashRequest{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("PurgeTrash(read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Library.ScanLibrary(ctx, readPATPrincipal, "lib-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ScanLibrary(read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Admin.ListUsers(ctx, readPATPrincipal); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ListUsers(read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Admin.GetUser(ctx, readPATPrincipal, "u-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("GetUser(read PAT) = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Admin.CreateUser(ctx, readPATPrincipal, CreateUserRequest{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("CreateUser(read PAT) = %v, want ErrForbidden", err)
	}
	if err := svcs.Admin.DeleteUser(ctx, readPATPrincipal, "u-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("DeleteUser(read PAT) = %v, want ErrForbidden", err)
	}
	if metadataCallbackCalls != 0 || curSpy.writeCalls != 0 || adminSpy.adminCalls != 0 || libSpy.calls != 2 {
		t.Fatalf("denied metadata/admin mutations invoked callback/service: cb=%d curWrite=%d admin=%d lib=%d",
			metadataCallbackCalls, curSpy.writeCalls, adminSpy.adminCalls, libSpy.calls)
	}
}

func TestAuthorizingServices_NilAndRestrictedReaderFlagAndCuratorPermittedOps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if NewAuthorizingServices(nil) != nil {
		t.Fatal("NewAuthorizingServices(nil) != nil")
	}

	// Nil inner service fails closed with ErrForbidden when denied and ErrNotImplemented when allowed.
	emptyAuth := NewAuthorizingServices(&Services{})
	if emptyAuth.Library != nil || emptyAuth.Reading != nil || emptyAuth.Curation != nil || emptyAuth.Admin != nil {
		t.Fatal("expected nil inner service fields to remain nil on NewAuthorizingServices(&Services{})")
	}

	directNilLib := NewAuthorizingLibraryService(nil)
	restrictedRole := builtInRoleByID(t, domain.RoleRestrictedReaderID)
	restricted := domain.NewPrincipal(testUser("u-restr"), []domain.Role{restrictedRole}, domain.CredentialKindPassword, "cred-restr", nil)
	if !restricted.RequiresAllowLabels() {
		t.Fatal("Restricted reader principal should expose RequiresAllowLabels() == true (M1-T9.2 seam)")
	}
	if _, err := directNilLib.ListLibraries(ctx, restricted); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("permitted call against nil inner service = %v, want ErrNotImplemented", err)
	}
	if _, err := directNilLib.ScanLibrary(ctx, restricted, "lib-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("denied call against nil inner service = %v, want ErrForbidden", err)
	}
	if _, err := directNilLib.ScanLibrary(ctx, nil, "lib-1"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthenticated call against nil inner service = %v, want ErrUnauthorized", err)
	}

	// Curator with admin PAT stays capped at Curator permissions (no library:admin, users:admin, trash:purge).
	curSpy := &spyCurationService{}
	libSpy := &spyLibraryService{}
	adminSpy := &spyAdminService{}
	svcs := NewAuthorizingServices(&Services{
		Library:  libSpy,
		Reading:  &spyReadingService{},
		Curation: curSpy,
		Admin:    adminSpy,
	})
	curatorRole := builtInRoleByID(t, domain.RoleCuratorID)
	curatorAdminPAT := domain.NewPrincipal(testUser("u-cur"), []domain.Role{curatorRole}, domain.CredentialKindPAT, "cred-pat-cur", []domain.PATScope{domain.ScopeAdmin})
	if curatorAdminPAT.RequiresAllowLabels() {
		t.Fatal("Curator principal should have RequiresAllowLabels() == false")
	}
	if err := svcs.Curation.HidePage(ctx, curatorAdminPAT, HidePageRequest{AssetID: "asset-1", PageIndex: 2, Suppressed: true}); err != nil {
		t.Fatalf("Curator HidePage = %v, want nil", err)
	}
	if err := svcs.Curation.StageTrash(ctx, curatorAdminPAT, StageTrashRequest{}); err != nil {
		t.Fatalf("Curator StageTrash = %v, want nil", err)
	}
	if _, err := svcs.Curation.ListTrash(ctx, curatorAdminPAT); err != nil {
		t.Fatalf("Curator ListTrash = %v, want nil", err)
	}
	if err := svcs.Curation.RestoreTrash(ctx, curatorAdminPAT, "trash-1"); err != nil {
		t.Fatalf("Curator RestoreTrash = %v, want nil", err)
	}
	if _, err := svcs.Curation.PurgeTrash(ctx, curatorAdminPAT, PurgeTrashRequest{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Curator PurgeTrash = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Library.ScanLibrary(ctx, curatorAdminPAT, "lib-1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Curator ScanLibrary = %v, want ErrForbidden", err)
	}
	if _, err := svcs.Admin.ListJobs(ctx, curatorAdminPAT); err != nil {
		t.Fatalf("authenticated ListJobs = %v, want nil", err)
	}
	if _, err := svcs.Admin.ListJobs(ctx, nil); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthenticated ListJobs = %v, want ErrUnauthorized", err)
	}
	if _, err := svcs.Admin.CreatePAT(ctx, nil, CreatePATRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthenticated CreatePAT = %v, want ErrUnauthorized", err)
	}
}

func TestAuthorizingAdminService_CreatePATCallerAuthorityAndValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	adminRole := builtInRoleByID(t, domain.RoleAdminID)
	curatorRole := builtInRoleByID(t, domain.RoleCuratorID)
	readerRole := builtInRoleByID(t, domain.RoleReaderID)
	restrictedRole := builtInRoleByID(t, domain.RoleRestrictedReaderID)

	adminUser := testUser("u-admin")
	curatorUser := testUser("u-cur")
	readerUser := testUser("u-reader")
	restrictedUser := testUser("u-restr")

	adminReadPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "pat-read", []domain.PATScope{domain.ScopeRead})
	adminProgressPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "pat-prog", []domain.PATScope{domain.ScopeProgressWrite})
	adminMetaPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "pat-meta", []domain.PATScope{domain.ScopeMetadataWrite})
	adminLibAdminPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "pat-libadmin", []domain.PATScope{domain.ScopeLibraryAdmin})
	curatorAdminPAT := domain.NewPrincipal(curatorUser, []domain.Role{curatorRole}, domain.CredentialKindPAT, "pat-cur-admin", []domain.PATScope{domain.ScopeAdmin})
	emptyScopeAdminPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "pat-empty", nil)

	adminPassword := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPassword, "pw-admin", nil)
	curatorOIDC := domain.NewPrincipal(curatorUser, []domain.Role{curatorRole}, domain.CredentialKindOIDCLink, "oidc-cur", nil)
	readerPassword := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "pw-reader", nil)
	restrictedPassword := domain.NewPrincipal(restrictedUser, []domain.Role{restrictedRole}, domain.CredentialKindPassword, "pw-restr", nil)
	noRolePassword := domain.NewPrincipal(testUser("u-norole"), nil, domain.CredentialKindPassword, "pw-norole", nil)
	spoofAdminPassword := domain.NewPrincipal(testUser("u-spoof"), []domain.Role{{ID: "role-spoof", Name: domain.RoleAdminName}}, domain.CredentialKindPassword, "pw-spoof", nil)

	t.Run("rejected requests never reach delegate", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name      string
			principal *domain.Principal
			scopes    []domain.PATScope
			wantErr   error
		}{
			{
				name:      "nil principal",
				principal: nil,
				scopes:    []domain.PATScope{domain.ScopeRead},
				wantErr:   ErrUnauthorized,
			},
			{
				name:      "admin read PAT requesting ScopeAdmin (M1-T8.5 escalation guard)",
				principal: adminReadPAT,
				scopes:    []domain.PATScope{domain.ScopeAdmin},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin read PAT requesting ScopeProgressWrite",
				principal: adminReadPAT,
				scopes:    []domain.PATScope{domain.ScopeProgressWrite},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin read PAT requesting ScopeMetadataWrite",
				principal: adminReadPAT,
				scopes:    []domain.PATScope{domain.ScopeMetadataWrite},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin read PAT requesting ScopeLibraryAdmin",
				principal: adminReadPAT,
				scopes:    []domain.PATScope{domain.ScopeLibraryAdmin},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin read PAT requesting union ScopeRead+ScopeProgressWrite",
				principal: adminReadPAT,
				scopes:    []domain.PATScope{domain.ScopeRead, domain.ScopeProgressWrite},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin progress PAT requesting ScopeMetadataWrite",
				principal: adminProgressPAT,
				scopes:    []domain.PATScope{domain.ScopeMetadataWrite},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin metadata PAT requesting ScopeProgressWrite",
				principal: adminMetaPAT,
				scopes:    []domain.PATScope{domain.ScopeProgressWrite},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin library:admin PAT requesting ScopeAdmin",
				principal: adminLibAdminPAT,
				scopes:    []domain.PATScope{domain.ScopeAdmin},
				wantErr:   ErrForbidden,
			},
			{
				name:      "curator admin PAT cannot mint ScopeLibraryAdmin",
				principal: curatorAdminPAT,
				scopes:    []domain.PATScope{domain.ScopeLibraryAdmin},
				wantErr:   ErrForbidden,
			},
			{
				name:      "unscoped reader password cannot mint ScopeMetadataWrite",
				principal: readerPassword,
				scopes:    []domain.PATScope{domain.ScopeMetadataWrite},
				wantErr:   ErrForbidden,
			},
			{
				name:      "unscoped reader password cannot mint ScopeAdmin",
				principal: readerPassword,
				scopes:    []domain.PATScope{domain.ScopeAdmin},
				wantErr:   ErrForbidden,
			},
			{
				name:      "unscoped curator OIDC cannot mint ScopeLibraryAdmin",
				principal: curatorOIDC,
				scopes:    []domain.PATScope{domain.ScopeLibraryAdmin},
				wantErr:   ErrForbidden,
			},
			{
				name:      "zero-permission no-role password requesting ScopeRead",
				principal: noRolePassword,
				scopes:    []domain.PATScope{domain.ScopeRead},
				wantErr:   ErrForbidden,
			},
			{
				name:      "zero-permission no-role password with empty scopes",
				principal: noRolePassword,
				scopes:    nil,
				wantErr:   ErrForbidden,
			},
			{
				name:      "zero-permission spoofed Admin role name requesting ScopeRead",
				principal: spoofAdminPassword,
				scopes:    []domain.PATScope{domain.ScopeRead},
				wantErr:   ErrForbidden,
			},
			{
				name:      "zero-permission empty-scope PAT requesting ScopeRead",
				principal: emptyScopeAdminPAT,
				scopes:    []domain.PATScope{domain.ScopeRead},
				wantErr:   ErrForbidden,
			},
			{
				name:      "admin password with empty scopes rejected as invalid input",
				principal: adminPassword,
				scopes:    []domain.PATScope{},
				wantErr:   ErrInvalidInput,
			},
			{
				name:      "admin password with unknown scope rejected as invalid input",
				principal: adminPassword,
				scopes:    []domain.PATScope{"bogus"},
				wantErr:   ErrInvalidInput,
			},
			{
				name:      "admin password with valid + unknown scope rejected as invalid input",
				principal: adminPassword,
				scopes:    []domain.PATScope{domain.ScopeRead, domain.PATScope("library:read")},
				wantErr:   ErrInvalidInput,
			},
			{
				name:      "read PAT with unknown scope rejected as invalid input",
				principal: adminReadPAT,
				scopes:    []domain.PATScope{"not-a-scope"},
				wantErr:   ErrInvalidInput,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				adminSpy := &spyAdminService{}
				svc := NewAuthorizingAdminService(adminSpy)
				res, err := svc.CreatePAT(ctx, tc.principal, CreatePATRequest{
					Label:  "test-pat",
					Scopes: tc.scopes,
				})
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("CreatePAT() err = %v, want %v", err, tc.wantErr)
				}
				if res != nil {
					t.Fatalf("CreatePAT() result = %+v, want nil on rejection", res)
				}
				if adminSpy.createPATCalls != 0 {
					t.Fatalf("delegate CreatePAT called %d times on rejection, want 0", adminSpy.createPATCalls)
				}
			})
		}
	})

	t.Run("permitted self-service creation stays within caller authority", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name               string
			principal          *domain.Principal
			scopes             []domain.PATScope
			wantChildAllowFlag bool
		}{
			{
				name:      "unscoped admin password minting ScopeAdmin",
				principal: adminPassword,
				scopes:    []domain.PATScope{domain.ScopeAdmin},
			},
			{
				name:      "unscoped admin password minting ScopeRead+ScopeMetadataWrite",
				principal: adminPassword,
				scopes:    []domain.PATScope{domain.ScopeRead, domain.ScopeMetadataWrite},
			},
			{
				name:      "unscoped curator OIDC minting ScopeMetadataWrite",
				principal: curatorOIDC,
				scopes:    []domain.PATScope{domain.ScopeMetadataWrite},
			},
			{
				name:      "unscoped reader password minting ScopeRead",
				principal: readerPassword,
				scopes:    []domain.PATScope{domain.ScopeRead},
			},
			{
				name:      "unscoped reader password minting ScopeProgressWrite",
				principal: readerPassword,
				scopes:    []domain.PATScope{domain.ScopeProgressWrite},
			},
			{
				name:               "unscoped restricted reader password minting ScopeRead preserves RequiresAllowLabels",
				principal:          restrictedPassword,
				scopes:             []domain.PATScope{domain.ScopeRead},
				wantChildAllowFlag: true,
			},
			{
				name:      "scoped metadata PAT minting strictly narrower ScopeRead",
				principal: adminMetaPAT,
				scopes:    []domain.PATScope{domain.ScopeRead},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				adminSpy := &spyAdminService{}
				svc := NewAuthorizingAdminService(adminSpy)
				res, err := svc.CreatePAT(ctx, tc.principal, CreatePATRequest{
					Label:  "permitted-pat",
					Scopes: tc.scopes,
				})
				if err != nil {
					t.Fatalf("CreatePAT() unexpected err = %v", err)
				}
				if res == nil || res.Credential == nil {
					t.Fatalf("expected non-nil PATCreationResult and Credential, got %+v", res)
				}
				if adminSpy.createPATCalls != 1 {
					t.Fatalf("delegate CreatePAT called %d times, want 1", adminSpy.createPATCalls)
				}

				// Resolving the created PAT must never grant any permission absent from the caller.
				child := domain.NewPrincipal(tc.principal.User, tc.principal.Roles, res.Credential.Kind, res.Credential.ID, res.Credential.Scopes)
				for _, perm := range child.Permissions {
					if !tc.principal.HasPermission(perm) {
						t.Fatalf("child PAT gained permission %q absent from caller %v", perm, tc.principal.Permissions)
					}
				}
				if child.RequiresAllowLabels() != tc.wantChildAllowFlag {
					t.Fatalf("child RequiresAllowLabels() = %v, want %v", child.RequiresAllowLabels(), tc.wantChildAllowFlag)
				}
			})
		}
	})

	t.Run("nil delegate fails closed consistently", func(t *testing.T) {
		t.Parallel()
		nilAdmin := NewAuthorizingAdminService(nil)
		if _, err := nilAdmin.CreatePAT(ctx, adminReadPAT, CreatePATRequest{Scopes: []domain.PATScope{domain.ScopeAdmin}}); !errors.Is(err, ErrForbidden) {
			t.Fatalf("denied CreatePAT against nil delegate = %v, want ErrForbidden", err)
		}
		if _, err := nilAdmin.CreatePAT(ctx, adminPassword, CreatePATRequest{Scopes: []domain.PATScope{domain.ScopeRead}}); !errors.Is(err, ErrNotImplemented) {
			t.Fatalf("permitted CreatePAT against nil delegate = %v, want ErrNotImplemented", err)
		}
	})
}
