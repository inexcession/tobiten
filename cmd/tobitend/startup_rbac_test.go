package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

type spyProbeLibrary struct {
	app.LibraryService
	readCalls atomic.Int32
	scanCalls atomic.Int32
}

func (s *spyProbeLibrary) ListSeries(ctx context.Context, principal *domain.Principal, filter app.SeriesFilter) ([]app.Series, error) {
	s.readCalls.Add(1)
	return s.LibraryService.ListSeries(ctx, principal, filter)
}

func (s *spyProbeLibrary) ScanLibrary(ctx context.Context, principal *domain.Principal, libraryID string) (string, error) {
	s.scanCalls.Add(1)
	return s.LibraryService.ScanLibrary(ctx, principal, libraryID)
}

type spyProbeReading struct {
	app.ReadingService
	writeCalls atomic.Int32
}

func (s *spyProbeReading) UpdateProgress(ctx context.Context, principal *domain.Principal, req app.UpdateProgressRequest) (*app.ReadingProgress, error) {
	s.writeCalls.Add(1)
	return s.ReadingService.UpdateProgress(ctx, principal, req)
}

type spyProbeCuration struct {
	app.CurationService
	hideCalls  atomic.Int32
	purgeCalls atomic.Int32
}

func (s *spyProbeCuration) HidePage(ctx context.Context, principal *domain.Principal, req app.HidePageRequest) error {
	s.hideCalls.Add(1)
	return s.CurationService.HidePage(ctx, principal, req)
}

func (s *spyProbeCuration) PurgeTrash(ctx context.Context, principal *domain.Principal, req app.PurgeTrashRequest) (string, error) {
	s.purgeCalls.Add(1)
	return s.CurationService.PurgeTrash(ctx, principal, req)
}

func writeProbeResult(w http.ResponseWriter, err error, payload any) {
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if payload != nil {
			_ = json.NewEncoder(w).Encode(payload)
		}
	case errors.Is(err, app.ErrUnauthorized):
		http.Error(w, err.Error(), http.StatusUnauthorized)
	case errors.Is(err, app.ErrForbidden):
		http.Error(w, err.Error(), http.StatusForbidden)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func TestDaemonRBACAndWarmedPATDemotionPostgres(t *testing.T) {
	db := testdb.New(t)
	t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
	t.Setenv("TOBITEN_DATABASE_URL", db.URL)
	t.Setenv("TOBITEN_ADMIN_USER", "alice")
	t.Setenv("TOBITEN_ADMIN_PASS", "initial-secret-123")
	t.Setenv("TOBITEN_LOG_FORMAT", "json")

	originalServer, originalLogger := startServer, slog.Default()
	t.Cleanup(func() {
		startServer = originalServer
		slog.SetDefault(originalLogger)
	})

	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		if services == nil || services.PATAuth == nil {
			t.Fatal("expected daemon to wire PATAuth service")
		}

		stubs := app.NewStubServices()
		libSpy := &spyProbeLibrary{LibraryService: stubs.Library}
		readSpy := &spyProbeReading{ReadingService: stubs.Reading}
		curSpy := &spyProbeCuration{CurationService: stubs.Curation}
		wired := app.NewAuthorizingServices(&app.Services{
			Setup:     services.Setup,
			BasicAuth: services.BasicAuth,
			PATAuth:   services.PATAuth,
			Library:   libSpy,
			Reading:   readSpy,
			Curation:  curSpy,
		})

		var directMetaCallbacks atomic.Int32
		reg := BuildRegistryWithServices(cfg, wired)
		probeMod := module.NewSimpleModule("rbac-probe")
		probeMod.Mounts = []module.RouteMount{
			{
				ModuleName: "rbac-probe",
				Prefix:     "/api/v1/rbac-probe",
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					p := domain.MustPrincipalFromContext(r.Context())
					switch {
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/rbac-probe/principal":
						writeProbeResult(w, nil, struct {
							UserID              string              `json:"user_id"`
							RequiresAllowLabels bool                `json:"requires_allow_labels"`
							Permissions         []domain.Permission `json:"permissions"`
						}{
							UserID:              p.User.ID,
							RequiresAllowLabels: p.RequiresAllowLabels(),
							Permissions:         p.Permissions,
						})
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/rbac-probe/series":
						res, err := wired.Library.ListSeries(r.Context(), p, app.SeriesFilter{})
						writeProbeResult(w, err, res)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/rbac-probe/progress":
						res, err := wired.Reading.UpdateProgress(r.Context(), p, app.UpdateProgressRequest{
							VolumeID:         "vol-1",
							TotalProgression: 0.5,
						})
						writeProbeResult(w, err, res)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/rbac-probe/metadata":
						err := wired.Curation.HidePage(r.Context(), p, app.HidePageRequest{
							AssetID:    "asset-1",
							PageIndex:  1,
							Suppressed: true,
						})
						writeProbeResult(w, err, nil)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/rbac-probe/metadata-helper":
						err := app.Authorize(p, domain.PermissionMetadataEdit, func() error {
							directMetaCallbacks.Add(1)
							return nil
						})
						writeProbeResult(w, err, nil)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/rbac-probe/library-scan":
						id, err := wired.Library.ScanLibrary(r.Context(), p, "lib-1")
						writeProbeResult(w, err, map[string]string{"job_id": id})
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/rbac-probe/trash-purge":
						id, err := wired.Curation.PurgeTrash(r.Context(), p, app.PurgeTrashRequest{})
						writeProbeResult(w, err, map[string]string{"job_id": id})
					default:
						http.NotFound(w, r)
					}
				}),
			},
		}
		if err := reg.Register(probeMod); err != nil {
			t.Fatalf("register rbac-probe module: %v", err)
		}

		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   logger,
			Listener: httptest.NewUnstartedServer(nil).Listener,
			Registry: reg,
		})
		if err != nil {
			t.Fatalf("api.NewServer: %v", err)
		}
		defer func() { _ = srv.Close() }()

		call := func(method, path, token, ip string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, nil)
			req.RemoteAddr = ip
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		repo := postgres.NewUserRepository(db.Pool)

		// 1. Verify seeded built-in roles in PostgreSQL, including Restricted reader's requires_allow_labels=TRUE (I-5).
		roles, err := repo.ListRoles(ctx)
		if err != nil {
			t.Fatalf("ListRoles: %v", err)
		}
		roleByID := make(map[string]domain.Role, len(roles))
		for _, role := range roles {
			roleByID[role.ID] = role
		}
		for _, expected := range domain.BuiltInRoles() {
			got, ok := roleByID[expected.ID]
			if !ok {
				t.Fatalf("seeded role %s (%s) missing from PostgreSQL", expected.ID, expected.Name)
			}
			if got.Name != expected.Name || got.RequiresAllowLabels != expected.RequiresAllowLabels {
				t.Fatalf("seeded role %s mismatch: got=(%q, requiresAllow=%v) want=(%q, requiresAllow=%v)",
					expected.ID, got.Name, got.RequiresAllowLabels, expected.Name, expected.RequiresAllowLabels)
			}
		}
		if !roleByID[domain.RoleRestrictedReaderID].RequiresAllowLabels {
			t.Fatal("expected seeded Restricted reader role to have requires_allow_labels = TRUE")
		}

		// 2. Admin user with a read-scoped PAT cannot write progress or metadata (M1-AC22).
		alice, err := repo.GetUserByUsername(ctx, "alice")
		if err != nil {
			t.Fatalf("GetUserByUsername(alice): %v", err)
		}
		readPATID := domain.MustNewID()
		readPATToken, readPATHash, err := auth.NewPAT(readPATID)
		if err != nil {
			t.Fatalf("NewPAT(read): %v", err)
		}
		scheme := auth.HashSchemeArgon2id
		exp := time.Now().UTC().Add(1 * time.Hour)
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         readPATID,
			UserID:     alice.ID,
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeRead},
			SecretHash: &readPATHash,
			HashScheme: &scheme,
			ExpiresAt:  &exp,
		}); err != nil {
			t.Fatalf("CreateCredential(read PAT): %v", err)
		}

		if rec := call(http.MethodGet, "/api/v1/rbac-probe/series", readPATToken, "198.51.100.20:1001"); rec.Code != http.StatusOK {
			t.Fatalf("read PAT GET /series status = %d, want 200", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/progress", readPATToken, "198.51.100.20:1002"); rec.Code != http.StatusForbidden {
			t.Fatalf("read PAT POST /progress status = %d, want 403", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/metadata", readPATToken, "198.51.100.20:1003"); rec.Code != http.StatusForbidden {
			t.Fatalf("read PAT POST /metadata status = %d, want 403", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/metadata-helper", readPATToken, "198.51.100.20:1004"); rec.Code != http.StatusForbidden {
			t.Fatalf("read PAT POST /metadata-helper status = %d, want 403", rec.Code)
		}
		if libSpy.readCalls.Load() != 1 || readSpy.writeCalls.Load() != 0 || curSpy.hideCalls.Load() != 0 || directMetaCallbacks.Load() != 0 {
			t.Fatalf("unexpected callback counts for read PAT: libRead=%d progWrite=%d hide=%d metaCb=%d",
				libSpy.readCalls.Load(), readSpy.writeCalls.Load(), curSpy.hideCalls.Load(), directMetaCallbacks.Load())
		}

		// 3. Curator user with an admin-scoped PAT: PAT is capped at Curator perms;
		// warming the daemon's PAT cache and then demoting ONLY user role grants in PostgreSQL
		// immediately lowers effective permissions on subsequent requests within cache TTL.
		bob := &domain.User{
			ID:       domain.MustNewID(),
			Username: "bob",
		}
		if err := repo.CreateUser(ctx, bob); err != nil {
			t.Fatalf("CreateUser(bob): %v", err)
		}
		if err := repo.AssignUserRole(ctx, bob.ID, domain.RoleCuratorID); err != nil {
			t.Fatalf("AssignUserRole(bob, Curator): %v", err)
		}
		bobPATID := domain.MustNewID()
		bobPATToken, bobPATHash, err := auth.NewPAT(bobPATID)
		if err != nil {
			t.Fatalf("NewPAT(bob): %v", err)
		}
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         bobPATID,
			UserID:     bob.ID,
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeAdmin},
			SecretHash: &bobPATHash,
			HashScheme: &scheme,
			ExpiresAt:  &exp,
		}); err != nil {
			t.Fatalf("CreateCredential(bob admin PAT): %v", err)
		}

		// Curator + admin PAT: metadata allowed (warms daemon PAT cache), library:admin and trash:purge still capped out.
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/metadata", bobPATToken, "198.51.100.21:1001"); rec.Code != http.StatusOK {
			t.Fatalf("Curator POST /metadata status = %d, want 200", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/metadata-helper", bobPATToken, "198.51.100.21:1002"); rec.Code != http.StatusOK {
			t.Fatalf("Curator POST /metadata-helper status = %d, want 200", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/library-scan", bobPATToken, "198.51.100.21:1003"); rec.Code != http.StatusForbidden {
			t.Fatalf("Curator POST /library-scan status = %d, want 403", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/trash-purge", bobPATToken, "198.51.100.21:1004"); rec.Code != http.StatusForbidden {
			t.Fatalf("Curator POST /trash-purge status = %d, want 403", rec.Code)
		}
		if curSpy.hideCalls.Load() != 1 || directMetaCallbacks.Load() != 1 || libSpy.scanCalls.Load() != 0 || curSpy.purgeCalls.Load() != 0 {
			t.Fatalf("unexpected Curator call counts: hide=%d metaCb=%d scan=%d purge=%d",
				curSpy.hideCalls.Load(), directMetaCallbacks.Load(), libSpy.scanCalls.Load(), curSpy.purgeCalls.Load())
		}

		// Demote bob in PostgreSQL ONLY via role grants (Curator -> Reader), leaving credential/token/scopes/expiry untouched.
		if err := repo.RemoveUserRole(ctx, bob.ID, domain.RoleCuratorID); err != nil {
			t.Fatalf("RemoveUserRole(Curator): %v", err)
		}
		if err := repo.AssignUserRole(ctx, bob.ID, domain.RoleReaderID); err != nil {
			t.Fatalf("AssignUserRole(Reader): %v", err)
		}

		if rec := call(http.MethodPost, "/api/v1/rbac-probe/metadata", bobPATToken, "198.51.100.21:1005"); rec.Code != http.StatusForbidden {
			t.Fatalf("Reader demoted POST /metadata status = %d, want 403", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/metadata-helper", bobPATToken, "198.51.100.21:1006"); rec.Code != http.StatusForbidden {
			t.Fatalf("Reader demoted POST /metadata-helper status = %d, want 403", rec.Code)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/progress", bobPATToken, "198.51.100.21:1007"); rec.Code != http.StatusOK {
			t.Fatalf("Reader demoted POST /progress status = %d, want 200", rec.Code)
		}
		if curSpy.hideCalls.Load() != 1 || directMetaCallbacks.Load() != 1 || readSpy.writeCalls.Load() != 1 {
			t.Fatalf("Reader demotion leaked or failed: hide=%d metaCb=%d progWrite=%d",
				curSpy.hideCalls.Load(), directMetaCallbacks.Load(), readSpy.writeCalls.Load())
		}

		// Demote bob further to Restricted reader: RequiresAllowLabels flips to true on the existing warmed PAT.
		if err := repo.RemoveUserRole(ctx, bob.ID, domain.RoleReaderID); err != nil {
			t.Fatalf("RemoveUserRole(Reader): %v", err)
		}
		if err := repo.AssignUserRole(ctx, bob.ID, domain.RoleRestrictedReaderID); err != nil {
			t.Fatalf("AssignUserRole(Restricted reader): %v", err)
		}

		rec := call(http.MethodGet, "/api/v1/rbac-probe/principal", bobPATToken, "198.51.100.21:1008")
		if rec.Code != http.StatusOK {
			t.Fatalf("Restricted reader GET /principal status = %d, want 200", rec.Code)
		}
		var principalResp struct {
			RequiresAllowLabels bool `json:"requires_allow_labels"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&principalResp); err != nil {
			t.Fatalf("decode /principal response: %v", err)
		}
		if !principalResp.RequiresAllowLabels {
			t.Fatal("expected demotion to Restricted reader to expose requires_allow_labels=true on existing warmed PAT")
		}

		// Remove all roles from bob: even read and progress write are now denied with 403 and zero callback invocations.
		if err := repo.RemoveUserRole(ctx, bob.ID, domain.RoleRestrictedReaderID); err != nil {
			t.Fatalf("RemoveUserRole(Restricted reader): %v", err)
		}
		if rec := call(http.MethodPost, "/api/v1/rbac-probe/progress", bobPATToken, "198.51.100.21:1009"); rec.Code != http.StatusForbidden {
			t.Fatalf("zero-role POST /progress status = %d, want 403", rec.Code)
		}
		if rec := call(http.MethodGet, "/api/v1/rbac-probe/series", bobPATToken, "198.51.100.21:1010"); rec.Code != http.StatusForbidden {
			t.Fatalf("zero-role GET /series status = %d, want 403", rec.Code)
		}
		if readSpy.writeCalls.Load() != 1 || libSpy.readCalls.Load() != 1 {
			t.Fatalf("zero-role calls invoked backend services: progWrite=%d libRead=%d",
				readSpy.writeCalls.Load(), libSpy.readCalls.Load())
		}

		return nil
	}

	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("runContext exit code = %d, want 0", code)
	}
}
