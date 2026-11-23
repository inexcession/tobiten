package openapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

type recordingLibraryService struct {
	lastLibFilter    app.LibraryFilter
	lastFolderFilter app.FolderBrowseFilter
	lastSeriesFilter app.SeriesFilter
	lastVolumeFilter app.VolumeFilter
	lastGetLibID     string
	lastGetSeriesID  string
	lastGetVolumeID  string
	lastScanLibID    string
	scanCalls        int
	scanJobID        string
	notFound         bool
	invalidInput     bool
	serviceBusy      bool
}

func (r *recordingLibraryService) ListLibraries(_ context.Context, principal *domain.Principal, filter app.LibraryFilter) (*app.LibraryPage, error) {
	r.lastLibFilter = filter
	limit := filter.Limit
	if limit <= 0 {
		limit = app.DefaultLibraryPageLimit
	} else if limit > app.MaxLibraryPageLimit {
		limit = app.MaxLibraryPageLimit
	}
	lib := domain.Library{
		ID:                 "lib-1",
		Name:               "Manga",
		Mode:               domain.LibraryModeVirtual,
		ResolverChain:      domain.DefaultResolverChain(),
		MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline,
		DefaultCoverPolicy: domain.CoverPolicyFirst,
		CreatedAt:          time.Unix(1700000000, 0).UTC(),
	}
	roots := []domain.LibraryRoot{
		{
			ID:        "root-1",
			LibraryID: "lib-1",
			Path:      domain.NewPathIdentity([]byte("/mnt/manga")),
			State:     domain.LibraryRootStateOnline,
		},
	}
	return &app.LibraryPage{
		Items:      []app.Library{app.LibraryFromDomain(lib, roots, principal)},
		TotalCount: 1,
		Limit:      limit,
		Offset:     filter.Offset,
	}, nil
}

func (r *recordingLibraryService) GetLibrary(_ context.Context, principal *domain.Principal, id string) (*app.Library, error) {
	r.lastGetLibID = id
	if r.notFound {
		return nil, app.ErrNotFound
	}
	lib := app.LibraryFromDomain(domain.Library{
		ID:                 id,
		Name:               "Manga",
		Mode:               domain.LibraryModeVirtual,
		MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline,
		DefaultCoverPolicy: domain.CoverPolicyFirst,
	}, nil, principal)
	return &lib, nil
}

func (r *recordingLibraryService) BrowseFolder(_ context.Context, _ *domain.Principal, filter app.FolderBrowseFilter) (*app.FolderBrowseResult, error) {
	r.lastFolderFilter = filter
	if r.notFound {
		return nil, app.ErrNotFound
	}
	if r.invalidInput {
		return nil, app.ErrInvalidInput
	}
	return &app.FolderBrowseResult{
		LibraryID: filter.LibraryID,
		Entries: []app.FolderEntry{
			{
				Kind:           "FOLDER",
				NameDisplay:    "SeriesA",
				NameHex:        "53657269657341",
				RelPathDisplay: "SeriesA",
				RelPathHex:     "53657269657341",
			},
		},
		TotalCount: 1,
		Limit:      app.DefaultLibraryPageLimit,
		Offset:     filter.Offset,
	}, nil
}

func (r *recordingLibraryService) ListSeries(_ context.Context, _ *domain.Principal, filter app.SeriesFilter) (*app.SeriesPage, error) {
	r.lastSeriesFilter = filter
	return &app.SeriesPage{
		Items: []app.Series{{ID: "ser-1", LibraryID: "lib-1", Title: "Series One", Genres: []string{}, Tags: []string{}}},
		Limit: app.DefaultLibraryPageLimit,
	}, nil
}

func (r *recordingLibraryService) GetSeries(_ context.Context, _ *domain.Principal, id string) (*app.Series, error) {
	r.lastGetSeriesID = id
	if r.notFound {
		return nil, app.ErrNotFound
	}
	return &app.Series{ID: id, LibraryID: "lib-1", Title: "Series One", Genres: []string{}, Tags: []string{}}, nil
}

func (r *recordingLibraryService) ListVolumes(_ context.Context, _ *domain.Principal, filter app.VolumeFilter) (*app.VolumePage, error) {
	r.lastVolumeFilter = filter
	return &app.VolumePage{
		Items:      []app.Volume{{ID: "vol-standalone", LibraryID: "lib-1", Kind: "VOLUME", SeriesID: nil, IsMissing: true}},
		TotalCount: 1,
		Limit:      app.DefaultLibraryPageLimit,
	}, nil
}

func (r *recordingLibraryService) GetVolume(_ context.Context, _ *domain.Principal, id string) (*app.Volume, error) {
	r.lastGetVolumeID = id
	if r.notFound {
		return nil, app.ErrNotFound
	}
	return &app.Volume{ID: id, LibraryID: "lib-1", Kind: "VOLUME", SeriesID: nil}, nil
}

func (r *recordingLibraryService) ScanLibrary(_ context.Context, _ *domain.Principal, libraryID string) (string, error) {
	r.scanCalls++
	r.lastScanLibID = libraryID
	if r.serviceBusy {
		return "", app.ErrServiceBusy
	}
	if r.notFound {
		return "", app.ErrNotFound
	}
	if r.invalidInput {
		return "", app.ErrInvalidInput
	}
	if r.scanJobID != "" {
		return r.scanJobID, nil
	}
	return "job-scan-" + libraryID, nil
}

func libraryTestPrincipal(perms ...domain.Permission) *domain.Principal {
	return domain.NewPrincipal(
		domain.User{ID: "u-1", Username: "tester"},
		[]domain.Role{{ID: "role-custom", Name: "Custom", Permissions: perms}},
		domain.CredentialKindPassword,
		"cred-1",
		nil,
	)
}

func TestLibraryReadRoutes_AuthAndServiceUnavailable(t *testing.T) {
	paths := []string{
		"/api/v1/libraries",
		"/api/v1/libraries/lib-1",
		"/api/v1/libraries/lib-1/folders",
		"/api/v1/series",
		"/api/v1/series/ser-1",
		"/api/v1/volumes",
		"/api/v1/volumes/vol-1",
	}

	modNil := NewModule(nil)
	modAuthOnly := NewModule(app.NewAuthorizingServices(&app.Services{Library: &recordingLibraryService{}}))

	for _, path := range paths {
		reqUnauth := httptest.NewRequest(http.MethodGet, path, nil)
		recUnauth := httptest.NewRecorder()
		modAuthOnly.router.ServeHTTP(recUnauth, reqUnauth)
		if recUnauth.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated GET %s = %d, want 401", path, recUnauth.Code)
		}

		reqNoPerm := httptest.NewRequest(http.MethodGet, path, nil)
		reqNoPerm = reqNoPerm.WithContext(domain.ContextWithPrincipal(reqNoPerm.Context(), libraryTestPrincipal(domain.PermissionJobsReadAll)))
		recNoPerm := httptest.NewRecorder()
		modAuthOnly.router.ServeHTTP(recNoPerm, reqNoPerm)
		if recNoPerm.Code != http.StatusForbidden {
			t.Fatalf("no library:read GET %s = %d, want 403", path, recNoPerm.Code)
		}

		reqUnavail := httptest.NewRequest(http.MethodGet, path, nil)
		reqUnavail = reqUnavail.WithContext(domain.ContextWithPrincipal(reqUnavail.Context(), libraryTestPrincipal(domain.PermissionLibraryRead)))
		recUnavail := httptest.NewRecorder()
		modNil.router.ServeHTTP(recUnavail, reqUnavail)
		if recUnavail.Code != http.StatusServiceUnavailable {
			t.Fatalf("nil service GET %s = %d, want 503", path, recUnavail.Code)
		}
	}

	for _, invalidPath := range []string{
		"/api/v1/libraries?offset=bad",
		"/api/v1/libraries/%20",
		"/api/v1/libraries/lib-1/folders?path_hex=zzzz",
		"/api/v1/series?library_id=%20",
		"/api/v1/series/%20",
		"/api/v1/volumes?limit=-1",
		"/api/v1/volumes/%20",
	} {
		reqNoPerm := httptest.NewRequest(http.MethodGet, invalidPath, nil)
		reqNoPerm = reqNoPerm.WithContext(domain.ContextWithPrincipal(reqNoPerm.Context(), libraryTestPrincipal(domain.PermissionJobsReadAll)))
		recNoPerm := httptest.NewRecorder()
		modAuthOnly.router.ServeHTTP(recNoPerm, reqNoPerm)
		if recNoPerm.Code != http.StatusForbidden {
			t.Fatalf("no library:read invalid query GET %s = %d, want 403", invalidPath, recNoPerm.Code)
		}
	}
}

func TestLibraryReadRoutes_QueryValidationJSONContractAndHEAD(t *testing.T) {
	recSvc := &recordingLibraryService{}
	mod := NewModule(app.NewAuthorizingServices(&app.Services{Library: recSvc}))
	reader := libraryTestPrincipal(domain.PermissionLibraryRead)
	admin := libraryTestPrincipal(domain.PermissionLibraryRead, domain.PermissionLibraryAdmin)

	t.Run("GET /api/v1/volumes emits explicit null series_id and HEAD omits body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/volumes?library_id=lib-1&series_id=ser-1&search=vol&limit=10&offset=5", nil)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), reader))
		rec := httptest.NewRecorder()
		mod.router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /api/v1/volumes = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
		}
		if !strings.Contains(rec.Body.String(), `"series_id":null`) {
			t.Fatalf("expected explicit null series_id in JSON, got %s", rec.Body.String())
		}
		if recSvc.lastVolumeFilter != (app.VolumeFilter{LibraryID: "lib-1", SeriesID: "ser-1", Search: "vol", Limit: 10, Offset: 5}) {
			t.Fatalf("unexpected VolumeFilter forwarded: %+v", recSvc.lastVolumeFilter)
		}

		headReq := httptest.NewRequest(http.MethodHead, "/api/v1/volumes", nil)
		headReq = headReq.WithContext(domain.ContextWithPrincipal(headReq.Context(), reader))
		headRec := httptest.NewRecorder()
		mod.router.ServeHTTP(headRec, headReq)
		if headRec.Code != http.StatusOK || headRec.Body.Len() != 0 {
			t.Fatalf("HEAD /api/v1/volumes = (%d, %q), want (200, empty)", headRec.Code, headRec.Body.String())
		}
	})

	t.Run("GET /api/v1/libraries redacts root paths for reader and shows for admin", func(t *testing.T) {
		reqReader := httptest.NewRequest(http.MethodGet, "/api/v1/libraries", nil)
		reqReader = reqReader.WithContext(domain.ContextWithPrincipal(reqReader.Context(), reader))
		recReader := httptest.NewRecorder()
		mod.router.ServeHTTP(recReader, reqReader)
		if recReader.Code != http.StatusOK || !strings.Contains(recReader.Body.String(), `"path_display":null`) {
			t.Fatalf("reader libraries should have explicit null path_display, got %d %s", recReader.Code, recReader.Body.String())
		}

		reqAdmin := httptest.NewRequest(http.MethodGet, "/api/v1/libraries", nil)
		reqAdmin = reqAdmin.WithContext(domain.ContextWithPrincipal(reqAdmin.Context(), admin))
		recAdmin := httptest.NewRecorder()
		mod.router.ServeHTTP(recAdmin, reqAdmin)
		if recAdmin.Code != http.StatusOK || !strings.Contains(recAdmin.Body.String(), `"/mnt/manga"`) {
			t.Fatalf("admin libraries should include root path_display, got %d %s", recAdmin.Code, recAdmin.Body.String())
		}
	})

	t.Run("GET /api/v1/libraries/{id}/folders preserves raw bytes and rejects bad params", func(t *testing.T) {
		reqRaw := httptest.NewRequest(http.MethodGet, "/api/v1/libraries/lib-1/folders?path=Raw-%FF-Dir&limit=25&offset=2", nil)
		reqRaw = reqRaw.WithContext(domain.ContextWithPrincipal(reqRaw.Context(), reader))
		recRaw := httptest.NewRecorder()
		mod.router.ServeHTTP(recRaw, reqRaw)
		if recRaw.Code != http.StatusOK {
			t.Fatalf("raw folder browse = %d (%s), want 200", recRaw.Code, recRaw.Body.String())
		}
		if !bytes.Equal(recSvc.lastFolderFilter.FolderPathBytes, []byte{'R', 'a', 'w', '-', 0xff, '-', 'D', 'i', 'r'}) {
			t.Fatalf("expected raw non-UTF-8 bytes preserved, got %x", recSvc.lastFolderFilter.FolderPathBytes)
		}

		reqHex := httptest.NewRequest(http.MethodGet, "/api/v1/libraries/lib-1/folders?path_hex=5261772dff", nil)
		reqHex = reqHex.WithContext(domain.ContextWithPrincipal(reqHex.Context(), reader))
		recHex := httptest.NewRecorder()
		mod.router.ServeHTTP(recHex, reqHex)
		if recHex.Code != http.StatusOK || !bytes.Equal(recSvc.lastFolderFilter.FolderPathBytes, []byte{0x52, 0x61, 0x77, 0x2d, 0xff}) {
			t.Fatalf("path_hex browse = (%d, %x), want (200, 5261772dff)", recHex.Code, recSvc.lastFolderFilter.FolderPathBytes)
		}

		var body app.FolderBrowseResult
		if err := json.Unmarshal(recRaw.Body.Bytes(), &body); err != nil {
			t.Fatalf("unmarshal folder response: %v", err)
		}
		if !strings.Contains(recRaw.Body.String(), `"asset_id":null`) {
			t.Fatalf("expected explicit null asset_id on FOLDER entry, got %s", recRaw.Body.String())
		}

		for _, badURL := range []string{
			"/api/v1/libraries/lib-1/folders?limit=-1",
			"/api/v1/libraries/lib-1/folders?limit=abc",
			"/api/v1/libraries/lib-1/folders?offset=-1",
			"/api/v1/libraries/lib-1/folders?path=a&path_hex=61",
			"/api/v1/libraries/lib-1/folders?path_hex=zzzz",
			"/api/v1/libraries/lib-1/folders?path=%ZZ",
			"/api/v1/series?library_id=%20",
			"/api/v1/volumes?limit=1&limit=2",
		} {
			badReq := httptest.NewRequest(http.MethodGet, badURL, nil)
			badReq = badReq.WithContext(domain.ContextWithPrincipal(badReq.Context(), reader))
			badRec := httptest.NewRecorder()
			mod.router.ServeHTTP(badRec, badReq)
			if badRec.Code != http.StatusBadRequest {
				t.Fatalf("GET %s = %d, want 400 (body=%s)", badURL, badRec.Code, badRec.Body.String())
			}
		}
	})

	t.Run("missing or restricted resource maps to 404", func(t *testing.T) {
		recSvc.notFound = true
		defer func() { recSvc.notFound = false }()
		for _, p := range []string{"/api/v1/libraries/x", "/api/v1/libraries/x/folders", "/api/v1/series/x", "/api/v1/volumes/x"} {
			req := httptest.NewRequest(http.MethodGet, p, nil)
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), reader))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("GET %s = %d, want 404", p, rec.Code)
			}
		}
	})
}

type recordingLibraryConfigService struct {
	createCalls   int
	addCalls      int
	lastCreateReq app.CreateLibraryRequest
	lastAddReq    app.AddLibraryRootRequest
	defaultMode   domain.LibraryMode
	conflictErr   bool
	notFoundErr   bool
	invalidErr    bool
}

func (r *recordingLibraryConfigService) CreateLibrary(_ context.Context, _ *domain.Principal, req app.CreateLibraryRequest) (*app.CreateLibraryResult, error) {
	r.createCalls++
	r.lastCreateReq = req
	if r.conflictErr {
		return nil, app.ErrConflict
	}
	if r.invalidErr {
		return nil, app.ErrInvalidInput
	}
	mode := req.Mode
	if mode == "" {
		mode = r.defaultMode
		if mode == "" {
			mode = domain.LibraryModeVirtual
		}
	}
	var intake *domain.PathIdentity
	if len(req.IntakePath) > 0 {
		id := domain.NewPathIdentity(req.IntakePath)
		intake = &id
	}
	chain := req.ResolverChain
	if chain == nil {
		chain = domain.DefaultResolverChain()
	}
	prec := req.MetadataPrecedence
	if prec == "" {
		prec = domain.MetadataPrecedenceEmbeddedOverOnline
	}
	cov := req.DefaultCoverPolicy
	if cov == "" {
		cov = domain.CoverPolicyFirst
	}
	lib := domain.Library{
		ID:                    "lib-created-1",
		Name:                  req.Name,
		Mode:                  mode,
		IntakePath:            intake,
		AutoMetadataWriteback: req.AutoMetadataWriteback,
		ResolverChain:         chain,
		MetadataPrecedence:    prec,
		DefaultCoverPolicy:    cov,
		CreatedAt:             time.Unix(1700000000, 0).UTC(),
	}
	var roots []domain.LibraryRoot
	for _, raw := range req.Roots {
		roots = append(roots, domain.LibraryRoot{
			ID:        "root-created-1",
			LibraryID: lib.ID,
			Path:      domain.NewPathIdentity(raw),
			State:     domain.LibraryRootStateOnline,
		})
	}
	return &app.CreateLibraryResult{Library: lib, Roots: roots}, nil
}

func (r *recordingLibraryConfigService) AddRoot(_ context.Context, _ *domain.Principal, req app.AddLibraryRootRequest) (*domain.LibraryRoot, error) {
	r.addCalls++
	r.lastAddReq = req
	if r.conflictErr {
		return nil, app.ErrConflict
	}
	if r.notFoundErr {
		return nil, app.ErrNotFound
	}
	if r.invalidErr {
		return nil, app.ErrInvalidInput
	}
	return &domain.LibraryRoot{
		ID:        "root-added-1",
		LibraryID: req.LibraryID,
		Path:      domain.NewPathIdentity(req.Path),
		State:     domain.LibraryRootStateOnline,
	}, nil
}

func (r *recordingLibraryConfigService) SetManagedIntakePath(context.Context, *domain.Principal, app.SetManagedIntakePathRequest) (*domain.Library, error) {
	return nil, app.ErrNotImplemented
}

func TestLibraryProvisioningRoutes_AuthCSRFValidationAndMapping(t *testing.T) {
	recCfg := &recordingLibraryConfigService{defaultMode: domain.LibraryModeVirtual}
	mod := NewModule(app.NewAuthorizingServices(&app.Services{LibraryConfig: recCfg}))
	modNil := NewModule(nil)

	reader := libraryTestPrincipal(domain.PermissionLibraryRead)
	admin := libraryTestPrincipal(domain.PermissionLibraryRead, domain.PermissionLibraryAdmin)

	routes := []string{
		"/api/v1/libraries",
		"/api/v1/libraries/lib-1/roots",
	}

	for _, path := range routes {
		// 1. Unauthenticated -> 401
		reqUnauth := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		recUnauth := httptest.NewRecorder()
		mod.router.ServeHTTP(recUnauth, reqUnauth)
		if recUnauth.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated POST %s = %d, want 401", path, recUnauth.Code)
		}

		// 2. Unverified CSRF -> 403
		reqCSRF := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		ctxCSRF := middleware.WithCSRFUnverified(domain.ContextWithPrincipal(reqCSRF.Context(), admin))
		recCSRF := httptest.NewRecorder()
		mod.router.ServeHTTP(recCSRF, reqCSRF.WithContext(ctxCSRF))
		if recCSRF.Code != http.StatusForbidden {
			t.Fatalf("unverified CSRF POST %s = %d, want 403", path, recCSRF.Code)
		}

		// 3. Missing library:admin -> 403 EVEN with malformed JSON body
		reqNoPerm := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{invalid`))
		reqNoPerm = reqNoPerm.WithContext(domain.ContextWithPrincipal(reqNoPerm.Context(), reader))
		recNoPerm := httptest.NewRecorder()
		mod.router.ServeHTTP(recNoPerm, reqNoPerm)
		if recNoPerm.Code != http.StatusForbidden {
			t.Fatalf("no library:admin POST %s = %d, want 403", path, recNoPerm.Code)
		}

		// 4. Nil service -> 503
		reqUnavail := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		reqUnavail = reqUnavail.WithContext(domain.ContextWithPrincipal(reqUnavail.Context(), admin))
		recUnavail := httptest.NewRecorder()
		modNil.router.ServeHTTP(recUnavail, reqUnavail)
		if recUnavail.Code != http.StatusServiceUnavailable {
			t.Fatalf("nil service POST %s = %d, want 503", path, recUnavail.Code)
		}
	}

	// Wrong methods return 405
	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/api/v1/libraries"},
		{http.MethodGet, "/api/v1/libraries/lib-1/roots"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
		rec := httptest.NewRecorder()
		mod.router.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s = %d, want 405", tc.method, tc.path, rec.Code)
		}
	}

	t.Run("POST /api/v1/libraries forwards fields and hex paths and returns 201", func(t *testing.T) {
		body := `{
			"name": "  Manga Vault  ",
			"mode": "managed",
			"root_path_hex": "2f6d6e742fff",
			"intake_path": "/mnt/intake",
			"auto_metadata_writeback": true,
			"resolver_chain": ["folder", "metadata"],
			"metadata_precedence": "online_over_embedded",
			"default_cover_policy": "latest"
		}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries", strings.NewReader(body))
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
		rec := httptest.NewRecorder()
		mod.router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("POST /api/v1/libraries = %d (%s), want 201", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
		}
		if recCfg.lastCreateReq.Name != "Manga Vault" ||
			recCfg.lastCreateReq.Mode != domain.LibraryModeManaged ||
			!bytes.Equal(recCfg.lastCreateReq.Roots[0], []byte{0x2f, 0x6d, 0x6e, 0x74, 0x2f, 0xff}) ||
			string(recCfg.lastCreateReq.IntakePath) != "/mnt/intake" ||
			!recCfg.lastCreateReq.AutoMetadataWriteback ||
			len(recCfg.lastCreateReq.ResolverChain) != 2 ||
			recCfg.lastCreateReq.MetadataPrecedence != domain.MetadataPrecedenceOnlineOverEmbedded ||
			recCfg.lastCreateReq.DefaultCoverPolicy != domain.CoverPolicyLatest {
			t.Fatalf("unexpected CreateLibraryRequest: %+v", recCfg.lastCreateReq)
		}
		var out app.Library
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		if out.ID != "lib-created-1" || out.Mode != "MANAGED" || len(out.Roots) != 1 || !out.Roots[0].InvalidUTF8 {
			t.Fatalf("unexpected created Library DTO: %+v", out)
		}
	})

	t.Run("POST /api/v1/libraries/{id}/roots forwards path and path_hex and returns 201", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-42/roots", strings.NewReader(`{"path":"/mnt/extra"}`))
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
		rec := httptest.NewRecorder()
		mod.router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("POST /roots = %d (%s), want 201", rec.Code, rec.Body.String())
		}
		if recCfg.lastAddReq.LibraryID != "lib-42" || string(recCfg.lastAddReq.Path) != "/mnt/extra" {
			t.Fatalf("unexpected AddLibraryRootRequest: %+v", recCfg.lastAddReq)
		}
		var out app.LibraryRoot
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("unmarshal root DTO: %v", err)
		}
		if out.LibraryID != "lib-42" || out.PathDisplay == nil || *out.PathDisplay != "/mnt/extra" {
			t.Fatalf("unexpected root DTO: %+v", out)
		}

		reqHex := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-42/roots", strings.NewReader(`{"path_hex":"2f6d6e742fff"}`))
		reqHex = reqHex.WithContext(domain.ContextWithPrincipal(reqHex.Context(), admin))
		recHex := httptest.NewRecorder()
		mod.router.ServeHTTP(recHex, reqHex)
		if recHex.Code != http.StatusCreated {
			t.Fatalf("POST /roots (path_hex) = %d (%s), want 201", recHex.Code, recHex.Body.String())
		}
		if !bytes.Equal(recCfg.lastAddReq.Path, []byte{0x2f, 0x6d, 0x6e, 0x74, 0x2f, 0xff}) {
			t.Fatalf("unexpected path_hex bytes: %x", recCfg.lastAddReq.Path)
		}
		var outHex app.LibraryRoot
		if err := json.Unmarshal(recHex.Body.Bytes(), &outHex); err != nil {
			t.Fatalf("unmarshal root hex DTO: %v", err)
		}
		if !outHex.InvalidUTF8 || outHex.PathDisplay == nil || *outHex.PathDisplay != `/mnt/\xff` {
			t.Fatalf("unexpected root hex DTO: %+v", outHex)
		}
	})

	t.Run("invalid payloads return 400 and service errors map to 409/404", func(t *testing.T) {
		badCreateBodies := []string{
			``,
			`{`,
			`{"name":"Lib"} {}`,
			`{"name":"Lib"}`,
			`{"name":"Lib","root_path":""}`,
			`{"name":"Lib","root_path":"/a","root_path_hex":"2f61"}`,
			`{"name":"Lib","root_path_hex":"zzzz"}`,
			`{"name":"Lib","root_path":"/a","intake_path":"/b","intake_path_hex":"2f62"}`,
			`{"name":"Lib","root_path":"/a","intake_path":""}`,
			`{"name":"Lib","root_path":"/a","mode":"BOGUS"}`,
		}
		for _, body := range badCreateBodies {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries", strings.NewReader(body))
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("POST /libraries body=%q got %d, want 400", body, rec.Code)
			}
		}

		badRootBodies := []string{
			``,
			`{}`,
			`{"path":""}`,
			`{"path":"/a","path_hex":"2f61"}`,
			`{"path_hex":"zzzz"}`,
		}
		for _, body := range badRootBodies {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/roots", strings.NewReader(body))
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("POST /roots body=%q got %d, want 400", body, rec.Code)
			}
		}

		// Whitespace library ID -> 400
		reqBadID := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/%20/roots", strings.NewReader(`{"path":"/a"}`))
		reqBadID = reqBadID.WithContext(domain.ContextWithPrincipal(reqBadID.Context(), admin))
		recBadID := httptest.NewRecorder()
		mod.router.ServeHTTP(recBadID, reqBadID)
		if recBadID.Code != http.StatusBadRequest {
			t.Fatalf("POST /libraries/%%20/roots = %d, want 400", recBadID.Code)
		}

		// Conflict -> 409
		recCfg.conflictErr = true
		for _, tc := range []struct {
			path string
			body string
		}{
			{"/api/v1/libraries", `{"name":"Lib","root_path":"/mnt/a"}`},
			{"/api/v1/libraries/lib-1/roots", `{"path":"/mnt/a"}`},
		} {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusConflict {
				t.Fatalf("conflict POST %s = %d, want 409", tc.path, rec.Code)
			}
		}
		recCfg.conflictErr = false

		// Missing library on AddRoot -> 404
		recCfg.notFoundErr = true
		req404 := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/missing/roots", strings.NewReader(`{"path":"/mnt/a"}`))
		req404 = req404.WithContext(domain.ContextWithPrincipal(req404.Context(), admin))
		rec404 := httptest.NewRecorder()
		mod.router.ServeHTTP(rec404, req404)
		if rec404.Code != http.StatusNotFound {
			t.Fatalf("missing library AddRoot = %d, want 404", rec404.Code)
		}
		recCfg.notFoundErr = false
	})

	t.Run("rejects lossy UTF-8 and unpaired surrogates in text paths while preserving literal U+FFFD and hex", func(t *testing.T) {
		invalidPayloads := []struct {
			name string
			path string
			body []byte
		}{
			{
				name: "CreateLibrary raw invalid UTF-8 root_path",
				path: "/api/v1/libraries",
				body: []byte(`{"name":"Lib","root_path":"/mnt/bad-` + "\xff" + `-dir"}`),
			},
			{
				name: "CreateLibrary raw invalid UTF-8 intake_path",
				path: "/api/v1/libraries",
				body: []byte(`{"name":"Lib","mode":"MANAGED","root_path":"/mnt/r","intake_path":"/mnt/in-` + "\xff" + `"}`),
			},
			{
				name: "AddRoot raw invalid UTF-8 path",
				path: "/api/v1/libraries/lib-1/roots",
				body: []byte(`{"path":"/mnt/bad-` + "\xff" + `-dir"}`),
			},
			{
				name: "CreateLibrary unpaired high surrogate root_path",
				path: "/api/v1/libraries",
				body: []byte(`{"name":"Lib","root_path":"/mnt/\ud800"}`),
			},
			{
				name: "CreateLibrary unpaired low surrogate root_path",
				path: "/api/v1/libraries",
				body: []byte(`{"name":"Lib","root_path":"/mnt/\udc00"}`),
			},
			{
				name: "CreateLibrary high surrogate followed by non-low surrogate root_path",
				path: "/api/v1/libraries",
				body: []byte(`{"name":"Lib","root_path":"/mnt/\ud800\u0041"}`),
			},
			{
				name: "CreateLibrary unpaired surrogate intake_path",
				path: "/api/v1/libraries",
				body: []byte(`{"name":"Lib","mode":"MANAGED","root_path":"/mnt/r","intake_path":"/mnt/\ud83d"}`),
			},
			{
				name: "AddRoot unpaired high surrogate path",
				path: "/api/v1/libraries/lib-1/roots",
				body: []byte(`{"path":"/mnt/\ud800"}`),
			},
			{
				name: "AddRoot unpaired low surrogate path",
				path: "/api/v1/libraries/lib-1/roots",
				body: []byte(`{"path":"/mnt/\udfff"}`),
			},
		}

		for _, tc := range invalidPayloads {
			t.Run(tc.name, func(t *testing.T) {
				beforeCreate, beforeAdd := recCfg.createCalls, recCfg.addCalls
				req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(tc.body))
				req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
				rec := httptest.NewRecorder()
				mod.router.ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("%s status = %d (%s), want 400", tc.name, rec.Code, rec.Body.String())
				}
				if recCfg.createCalls != beforeCreate || recCfg.addCalls != beforeAdd {
					t.Fatalf("%s invoked config service despite invalid text path", tc.name)
				}
			})
		}

		// Valid literal U+FFFD (raw UTF-8 \xef\xbf\xbd and \ufffd escape) and valid surrogate pairs succeed verbatim.
		for _, tc := range []struct {
			name     string
			body     string
			wantRoot string
			wantIn   string
		}{
			{
				name:     "raw UTF-8 U+FFFD and escaped U+FFFD",
				body:     `{"name":"FFFD Lib","mode":"MANAGED","root_path":"/mnt/literal-` + "\ufffd" + `","intake_path":"/mnt/escaped-\ufffd"}`,
				wantRoot: "/mnt/literal-\ufffd",
				wantIn:   "/mnt/escaped-\ufffd",
			},
			{
				name:     "valid surrogate pair and escaped backslash before ud800",
				body:     `{"name":"Pair Lib","mode":"MANAGED","root_path":"/mnt/\ud83d\ude00","intake_path":"/mnt/\\ud800"}`,
				wantRoot: "/mnt/\U0001f600",
				wantIn:   `/mnt/\ud800`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries", strings.NewReader(tc.body))
				req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
				rec := httptest.NewRecorder()
				mod.router.ServeHTTP(rec, req)
				if rec.Code != http.StatusCreated {
					t.Fatalf("%s CreateLibrary = %d (%s), want 201", tc.name, rec.Code, rec.Body.String())
				}
				if string(recCfg.lastCreateReq.Roots[0]) != tc.wantRoot || string(recCfg.lastCreateReq.IntakePath) != tc.wantIn {
					t.Fatalf("%s got root=%q intake=%q, want root=%q intake=%q",
						tc.name, recCfg.lastCreateReq.Roots[0], recCfg.lastCreateReq.IntakePath, tc.wantRoot, tc.wantIn)
				}

				addBody := `{"path":"/mnt/add-` + "\ufffd" + `-\ud83d\ude00"}`
				addReq := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/roots", strings.NewReader(addBody))
				addReq = addReq.WithContext(domain.ContextWithPrincipal(addReq.Context(), admin))
				addRec := httptest.NewRecorder()
				mod.router.ServeHTTP(addRec, addReq)
				if addRec.Code != http.StatusCreated {
					t.Fatalf("%s AddRoot = %d (%s), want 201", tc.name, addRec.Code, addRec.Body.String())
				}
				if string(recCfg.lastAddReq.Path) != "/mnt/add-\ufffd-\U0001f600" {
					t.Fatalf("%s AddRoot path = %q", tc.name, recCfg.lastAddReq.Path)
				}
			})
		}
	})

	t.Run("enforces exact 1 MiB body limit and rejects trailing data beyond cap", func(t *testing.T) {
		// 1. Valid JSON object padded with whitespace to exact maxLibraryJSONBodyBytes succeeds.
		for _, tc := range []struct {
			name   string
			path   string
			prefix string
		}{
			{"CreateLibrary at exact cap", "/api/v1/libraries", `{"name":"AtLimit","root_path":"/mnt/limit"}`},
			{"AddRoot at exact cap", "/api/v1/libraries/lib-1/roots", `{"path":"/mnt/limit"}`},
		} {
			pad := strings.Repeat(" ", maxLibraryJSONBodyBytes-len(tc.prefix))
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.prefix+pad))
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusCreated {
				t.Fatalf("%s status = %d (%s), want 201", tc.name, rec.Code, rec.Body.String())
			}
		}

		// 2. Valid JSON object followed by whitespace up to cap + second JSON value beyond cap -> 413 without service call.
		for _, tc := range []struct {
			name   string
			path   string
			prefix string
			suffix string
		}{
			{"CreateLibrary padded overflow with trailing value", "/api/v1/libraries", `{"name":"Over","root_path":"/mnt/over"}`, `{}`},
			{"AddRoot padded overflow with trailing value", "/api/v1/libraries/lib-1/roots", `{"path":"/mnt/over"}`, `{}`},
			{"CreateLibrary 1 byte over cap", "/api/v1/libraries", `{"name":"Over1","root_path":"/mnt/over1"}`, " "},
			{"AddRoot 1 byte over cap", "/api/v1/libraries/lib-1/roots", `{"path":"/mnt/over1"}`, " "},
		} {
			pad := strings.Repeat(" ", maxLibraryJSONBodyBytes-len(tc.prefix))
			beforeCreate, beforeAdd := recCfg.createCalls, recCfg.addCalls
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.prefix+pad+tc.suffix))
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("%s status = %d (%s), want 413", tc.name, rec.Code, rec.Body.String())
			}
			if recCfg.createCalls != beforeCreate || recCfg.addCalls != beforeAdd {
				t.Fatalf("%s invoked config service on oversized body", tc.name)
			}
		}

		// 3. Ordinary trailing data under cap -> 400 without service call.
		for _, tc := range []struct {
			path string
			body string
		}{
			{"/api/v1/libraries", `{"name":"Trail","root_path":"/mnt/a"} {"extra":true}`},
			{"/api/v1/libraries/lib-1/roots", `{"path":"/mnt/a"} {"extra":true}`},
		} {
			beforeCreate, beforeAdd := recCfg.createCalls, recCfg.addCalls
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), admin))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("trailing data POST %s = %d, want 400", tc.path, rec.Code)
			}
			if recCfg.createCalls != beforeCreate || recCfg.addCalls != beforeAdd {
				t.Fatalf("trailing data POST %s invoked config service", tc.path)
			}
		}
	})
}

func TestLibraryScanRoute_AuthValidationAndResponse(t *testing.T) {
	recSvc := &recordingLibraryService{scanJobID: "job-scan-42"}
	mod := NewModule(app.NewAuthorizingServices(&app.Services{Library: recSvc}))
	modNil := NewModule(nil)

	reader := libraryTestPrincipal(domain.PermissionLibraryRead)
	admin := libraryTestPrincipal(domain.PermissionLibraryRead, domain.PermissionLibraryAdmin)

	// 1. Unauthenticated -> 401
	reqUnauth := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
	recUnauth := httptest.NewRecorder()
	mod.router.ServeHTTP(recUnauth, reqUnauth)
	if recUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated POST /scan = %d, want 401", recUnauth.Code)
	}

	// 2. Unverified CSRF -> 403
	reqCSRF := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
	ctxCSRF := middleware.WithCSRFUnverified(domain.ContextWithPrincipal(reqCSRF.Context(), admin))
	recCSRF := httptest.NewRecorder()
	mod.router.ServeHTTP(recCSRF, reqCSRF.WithContext(ctxCSRF))
	if recCSRF.Code != http.StatusForbidden {
		t.Fatalf("unverified CSRF POST /scan = %d, want 403", recCSRF.Code)
	}

	// 3. Missing library:admin -> 403 before {id} validation or service call
	for _, path := range []string{"/api/v1/libraries/lib-1/scan", "/api/v1/libraries/%20/scan"} {
		before := recSvc.scanCalls
		reqNoPerm := httptest.NewRequest(http.MethodPost, path, nil)
		reqNoPerm = reqNoPerm.WithContext(domain.ContextWithPrincipal(reqNoPerm.Context(), reader))
		recNoPerm := httptest.NewRecorder()
		mod.router.ServeHTTP(recNoPerm, reqNoPerm)
		if recNoPerm.Code != http.StatusForbidden {
			t.Fatalf("no library:admin POST %s = %d, want 403", path, recNoPerm.Code)
		}
		if recSvc.scanCalls != before {
			t.Fatalf("no library:admin POST %s invoked service", path)
		}
	}

	// 4. Nil service -> 503
	reqUnavail := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
	reqUnavail = reqUnavail.WithContext(domain.ContextWithPrincipal(reqUnavail.Context(), admin))
	recUnavail := httptest.NewRecorder()
	modNil.router.ServeHTTP(recUnavail, reqUnavail)
	if recUnavail.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil service POST /scan = %d, want 503", recUnavail.Code)
	}

	// 5. Wrong methods -> 405
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		reqMethod := httptest.NewRequest(method, "/api/v1/libraries/lib-1/scan", nil)
		reqMethod = reqMethod.WithContext(domain.ContextWithPrincipal(reqMethod.Context(), admin))
		recMethod := httptest.NewRecorder()
		mod.router.ServeHTTP(recMethod, reqMethod)
		if recMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s /api/v1/libraries/lib-1/scan = %d, want 405", method, recMethod.Code)
		}
	}

	// 6. Blank library ID -> 400 without calling service
	beforeBlank := recSvc.scanCalls
	reqBlank := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/%20/scan", nil)
	reqBlank = reqBlank.WithContext(domain.ContextWithPrincipal(reqBlank.Context(), admin))
	recBlank := httptest.NewRecorder()
	mod.router.ServeHTTP(recBlank, reqBlank)
	if recBlank.Code != http.StatusBadRequest || recSvc.scanCalls != beforeBlank {
		t.Fatalf("blank ID POST /scan = (%d, calls=%d/%d), want 400 without service call", recBlank.Code, recSvc.scanCalls, beforeBlank)
	}

	// 7. Unknown library -> 404
	recSvc.notFound = true
	req404 := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/missing/scan", nil)
	req404 = req404.WithContext(domain.ContextWithPrincipal(req404.Context(), admin))
	rec404 := httptest.NewRecorder()
	mod.router.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("unknown library POST /scan = %d, want 404", rec404.Code)
	}
	recSvc.notFound = false

	// 8. Valid library:admin trigger -> 202 Accepted with {"job_id":"job-scan-42"} and Cache-Control: no-store
	reqOK := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-42/scan", nil)
	reqOK = reqOK.WithContext(domain.ContextWithPrincipal(reqOK.Context(), admin))
	recOK := httptest.NewRecorder()
	mod.router.ServeHTTP(recOK, reqOK)
	if recOK.Code != http.StatusAccepted {
		t.Fatalf("POST /api/v1/libraries/lib-42/scan = %d (%s), want 202", recOK.Code, recOK.Body.String())
	}
	if cc := recOK.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("POST /scan Cache-Control = %q, want no-store", cc)
	}
	if recSvc.lastScanLibID != "lib-42" {
		t.Fatalf("lastScanLibID = %q, want lib-42", recSvc.lastScanLibID)
	}
	var out middleware.JobAcceptedResponse
	if err := json.Unmarshal(recOK.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode JobAcceptedResponse: %v", err)
	}
	if out.JobID != "job-scan-42" {
		t.Fatalf("JobID = %q, want job-scan-42", out.JobID)
	}
}
