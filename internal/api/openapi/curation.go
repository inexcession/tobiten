package openapi

import (
	"bytes"
	"net/http"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

func (m *Module) curationService() app.CurationService {
	if m.services == nil {
		return nil
	}
	return m.services.Curation
}

func (m *Module) handleGetVolumeThumbnail(w http.ResponseWriter, r *http.Request) {
	m.handleGetThumbnail(w, r, domain.ItemTypeVolume)
}

func (m *Module) handleGetSeriesThumbnail(w http.ResponseWriter, r *http.Request) {
	m.handleGetThumbnail(w, r, domain.ItemTypeSeries)
}

func (m *Module) handleGetThumbnail(w http.ResponseWriter, r *http.Request, kind domain.ItemType) {
	p, ok := m.preparePrincipalPermission(w, r, domain.PermissionLibraryRead)
	if !ok {
		return
	}
	svc := m.curationService()
	if svc == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}
	id, err := requireIDParam(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}

	ctx := app.ContextWithCoverTargetKind(r.Context(), kind)
	rendition, err := svc.GetCover(ctx, p, id)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	if rendition == nil || len(rendition.Data) == 0 || strings.TrimSpace(rendition.ETag) == "" {
		writeError(w, http.StatusInternalServerError, "invalid cover rendition")
		return
	}

	contentType := strings.TrimSpace(rendition.ContentType)
	if contentType == "" {
		contentType = "image/jpeg"
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("ETag", rendition.ETag)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(rendition.Data))
}
