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
	notFound         bool
	invalidInput     bool
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

func (r *recordingLibraryService) ScanLibrary(context.Context, *domain.Principal, string) (string, error) {
	return "", app.ErrNotImplemented
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
