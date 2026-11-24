package openapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

type recordingCurationService struct {
	calls          int
	lastPrincipal  *domain.Principal
	lastTargetID   string
	lastTargetKind domain.ItemType
	hasTargetKind  bool
	renditions     map[string]*app.CoverRendition
	errByID        map[string]error
	defaultErr     error
}

func (r *recordingCurationService) GetCover(ctx context.Context, principal *domain.Principal, targetID string) (*app.CoverRendition, error) {
	r.calls++
	r.lastPrincipal = principal
	r.lastTargetID = targetID
	r.lastTargetKind, r.hasTargetKind = app.CoverTargetKindFromContext(ctx)
	if err, ok := r.errByID[targetID]; ok {
		return nil, err
	}
	if r.defaultErr != nil {
		return nil, r.defaultErr
	}
	if ren, ok := r.renditions[targetID]; ok {
		return ren, nil
	}
	return nil, app.ErrNotFound
}

func (r *recordingCurationService) HidePage(context.Context, *domain.Principal, app.HidePageRequest) error {
	return app.ErrNotImplemented
}

func (r *recordingCurationService) StageTrash(context.Context, *domain.Principal, app.StageTrashRequest) error {
	return app.ErrNotImplemented
}

func (r *recordingCurationService) ListTrash(context.Context, *domain.Principal) ([]app.TrashItem, error) {
	return nil, app.ErrNotImplemented
}

func (r *recordingCurationService) RestoreTrash(context.Context, *domain.Principal, string) error {
	return app.ErrNotImplemented
}

func (r *recordingCurationService) PurgeTrash(context.Context, *domain.Principal, app.PurgeTrashRequest) (string, error) {
	return "", app.ErrNotImplemented
}

func TestThumbnailRoutes_AuthPermissionsCachingAndErrors(t *testing.T) {
	t.Parallel()

	jpegPayload := []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00, 0xff, 0xd9}
	recSvc := &recordingCurationService{
		renditions: map[string]*app.CoverRendition{
			"vol-1": {
				TargetID:    "vol-1",
				ContentType: "image/jpeg",
				Data:        jpegPayload,
				ETag:        `"vol-1-strong-etag"`,
			},
			"ser-1": {
				TargetID:    "ser-1",
				ContentType: "image/jpeg",
				Data:        jpegPayload,
				ETag:        `"ser-1-strong-etag"`,
			},
		},
		errByID: map[string]error{
			"vol-restricted": app.ErrNotFound,
			"vol-busy":       app.ErrServiceBusy,
		},
	}

	mod := NewModule(app.NewAuthorizingServices(&app.Services{Curation: recSvc}))
	modNil := NewModule(nil)

	reader := libraryTestPrincipal(domain.PermissionLibraryRead)
	noRead := libraryTestPrincipal(domain.PermissionJobsReadAll)

	for _, tc := range []struct {
		name     string
		path     string
		targetID string
		wantKind domain.ItemType
		wantETag string
	}{
		{
			name:     "volume thumbnail",
			path:     "/api/v1/volumes/vol-1/thumbnail",
			targetID: "vol-1",
			wantKind: domain.ItemTypeVolume,
			wantETag: `"vol-1-strong-etag"`,
		},
		{
			name:     "series thumbnail",
			path:     "/api/v1/series/ser-1/thumbnail",
			targetID: "ser-1",
			wantKind: domain.ItemTypeSeries,
			wantETag: `"ser-1-strong-etag"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeCalls := recSvc.calls

			// Unauthenticated -> 401 before service call, even with matching If-None-Match.
			reqUnauth := httptest.NewRequest(http.MethodGet, tc.path, nil)
			reqUnauth.Header.Set("If-None-Match", tc.wantETag)
			recUnauth := httptest.NewRecorder()
			mod.router.ServeHTTP(recUnauth, reqUnauth)
			if recUnauth.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated GET %s = %d, want 401", tc.path, recUnauth.Code)
			}

			// Missing library:read -> 403 before service call, even with matching If-None-Match.
			reqNoPerm := httptest.NewRequest(http.MethodGet, tc.path, nil)
			reqNoPerm.Header.Set("If-None-Match", tc.wantETag)
			reqNoPerm = reqNoPerm.WithContext(domain.ContextWithPrincipal(reqNoPerm.Context(), noRead))
			recNoPerm := httptest.NewRecorder()
			mod.router.ServeHTTP(recNoPerm, reqNoPerm)
			if recNoPerm.Code != http.StatusForbidden {
				t.Fatalf("no library:read GET %s = %d, want 403", tc.path, recNoPerm.Code)
			}
			if recSvc.calls != beforeCalls {
				t.Fatalf("unauthorized/forbidden calls invoked CurationService %d times, want 0", recSvc.calls-beforeCalls)
			}

			// Nil service -> 503.
			reqNil := httptest.NewRequest(http.MethodGet, tc.path, nil)
			reqNil = reqNil.WithContext(domain.ContextWithPrincipal(reqNil.Context(), reader))
			recNil := httptest.NewRecorder()
			modNil.router.ServeHTTP(recNil, reqNil)
			if recNil.Code != http.StatusServiceUnavailable {
				t.Fatalf("nil service GET %s = %d, want 503", tc.path, recNil.Code)
			}

			// Authorized GET -> 200 image/jpeg, Cache-Control: private, no-cache, strong ETag.
			reqOK := httptest.NewRequest(http.MethodGet, tc.path, nil)
			reqOK = reqOK.WithContext(domain.ContextWithPrincipal(reqOK.Context(), reader))
			recOK := httptest.NewRecorder()
			mod.router.ServeHTTP(recOK, reqOK)
			if recOK.Code != http.StatusOK {
				t.Fatalf("GET %s = %d, want 200 (body=%s)", tc.path, recOK.Code, recOK.Body.String())
			}
			if !recSvc.hasTargetKind || recSvc.lastTargetKind != tc.wantKind || recSvc.lastTargetID != tc.targetID {
				t.Fatalf("GetCover target = (%q, %v, %q), want (%q, true, %q)", recSvc.lastTargetKind, recSvc.hasTargetKind, recSvc.lastTargetID, tc.wantKind, tc.targetID)
			}
			if gotCT := recOK.Header().Get("Content-Type"); gotCT != "image/jpeg" {
				t.Fatalf("Content-Type = %q, want image/jpeg", gotCT)
			}
			if gotCC := recOK.Header().Get("Cache-Control"); gotCC != "private, no-cache" {
				t.Fatalf("Cache-Control = %q, want private, no-cache", gotCC)
			}
			if gotETag := recOK.Header().Get("ETag"); gotETag != tc.wantETag || strings.HasPrefix(gotETag, "W/") {
				t.Fatalf("ETag = %q, want strong ETag %q", gotETag, tc.wantETag)
			}
			if !bytes.Equal(recOK.Body.Bytes(), jpegPayload) {
				t.Fatalf("body mismatch: got %x, want %x", recOK.Body.Bytes(), jpegPayload)
			}

			// Authorized HEAD -> 200 with same headers and empty body.
			reqHead := httptest.NewRequest(http.MethodHead, tc.path, nil)
			reqHead = reqHead.WithContext(domain.ContextWithPrincipal(reqHead.Context(), reader))
			recHead := httptest.NewRecorder()
			mod.router.ServeHTTP(recHead, reqHead)
			if recHead.Code != http.StatusOK || recHead.Body.Len() != 0 {
				t.Fatalf("HEAD %s = (%d, len=%d), want (200, 0)", tc.path, recHead.Code, recHead.Body.Len())
			}
			if recHead.Header().Get("ETag") != tc.wantETag || recHead.Header().Get("Cache-Control") != "private, no-cache" {
				t.Fatalf("HEAD headers = ETag:%q Cache-Control:%q", recHead.Header().Get("ETag"), recHead.Header().Get("Cache-Control"))
			}

			// Matching If-None-Match -> 304 Not Modified with private, no-cache and ETag.
			req304 := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req304.Header.Set("If-None-Match", `"other-tag", `+tc.wantETag)
			req304 = req304.WithContext(domain.ContextWithPrincipal(req304.Context(), reader))
			rec304 := httptest.NewRecorder()
			mod.router.ServeHTTP(rec304, req304)
			if rec304.Code != http.StatusNotModified {
				t.Fatalf("If-None-Match GET %s = %d, want 304", tc.path, rec304.Code)
			}
			if rec304.Body.Len() != 0 {
				t.Fatalf("304 body len = %d, want 0", rec304.Body.Len())
			}
			if rec304.Header().Get("Cache-Control") != "private, no-cache" || rec304.Header().Get("ETag") != tc.wantETag {
				t.Fatalf("304 headers = ETag:%q Cache-Control:%q", rec304.Header().Get("ETag"), rec304.Header().Get("Cache-Control"))
			}

			// Non-matching If-None-Match -> 200 OK with JPEG body.
			reqMismatch := httptest.NewRequest(http.MethodGet, tc.path, nil)
			reqMismatch.Header.Set("If-None-Match", `"stale-etag"`)
			reqMismatch = reqMismatch.WithContext(domain.ContextWithPrincipal(reqMismatch.Context(), reader))
			recMismatch := httptest.NewRecorder()
			mod.router.ServeHTTP(recMismatch, reqMismatch)
			if recMismatch.Code != http.StatusOK || !bytes.Equal(recMismatch.Body.Bytes(), jpegPayload) {
				t.Fatalf("stale If-None-Match GET %s = (%d, len=%d), want (200, %d)", tc.path, recMismatch.Code, recMismatch.Body.Len(), len(jpegPayload))
			}

			// Wrong method (POST) -> 405 Method Not Allowed.
			reqPost := httptest.NewRequest(http.MethodPost, tc.path, nil)
			reqPost = reqPost.WithContext(domain.ContextWithPrincipal(reqPost.Context(), reader))
			recPost := httptest.NewRecorder()
			mod.router.ServeHTTP(recPost, reqPost)
			if recPost.Code != http.StatusMethodNotAllowed {
				t.Fatalf("POST %s = %d, want 405", tc.path, recPost.Code)
			}
		})
	}

	t.Run("404 before If-None-Match, 400 blank ID, and 503 busy", func(t *testing.T) {
		for _, path := range []string{
			"/api/v1/volumes/vol-restricted/thumbnail",
			"/api/v1/series/ser-unknown/thumbnail",
		} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("If-None-Match", "*")
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), reader))
			rec := httptest.NewRecorder()
			mod.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("GET %s = %d, want 404", path, rec.Code)
			}
			if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("ETag") != "" {
				t.Fatalf("404 headers leaked cache/etag: Cache-Control=%q ETag=%q", rec.Header().Get("Cache-Control"), rec.Header().Get("ETag"))
			}
		}

		reqBlank := httptest.NewRequest(http.MethodGet, "/api/v1/volumes/%20/thumbnail", nil)
		reqBlank = reqBlank.WithContext(domain.ContextWithPrincipal(reqBlank.Context(), reader))
		recBlank := httptest.NewRecorder()
		mod.router.ServeHTTP(recBlank, reqBlank)
		if recBlank.Code != http.StatusBadRequest {
			t.Fatalf("blank volume id GET = %d, want 400", recBlank.Code)
		}

		reqBusy := httptest.NewRequest(http.MethodGet, "/api/v1/volumes/vol-busy/thumbnail", nil)
		reqBusy = reqBusy.WithContext(domain.ContextWithPrincipal(reqBusy.Context(), reader))
		recBusy := httptest.NewRecorder()
		mod.router.ServeHTTP(recBusy, reqBusy)
		if recBusy.Code != http.StatusServiceUnavailable {
			t.Fatalf("busy volume GET = %d, want 503", recBusy.Code)
		}
	})

	t.Run("NewAuthorizingServices enforces library:read on CurationService.GetCover", func(t *testing.T) {
		authSvcs := app.NewAuthorizingServices(&app.Services{Curation: recSvc})
		if _, err := authSvcs.Curation.GetCover(context.Background(), nil, "vol-1"); !errors.Is(err, app.ErrUnauthorized) {
			t.Fatalf("nil principal GetCover = %v, want ErrUnauthorized", err)
		}
		if _, err := authSvcs.Curation.GetCover(context.Background(), noRead, "vol-1"); !errors.Is(err, app.ErrForbidden) {
			t.Fatalf("no library:read GetCover = %v, want ErrForbidden", err)
		}
		got, err := authSvcs.Curation.GetCover(context.Background(), reader, "vol-1")
		if err != nil || got.ETag != `"vol-1-strong-etag"` {
			t.Fatalf("authorized GetCover = (%+v, %v)", got, err)
		}
	})
}
