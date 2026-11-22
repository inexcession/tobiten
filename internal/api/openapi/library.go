package openapi

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

func (m *Module) libraryService() app.LibraryService {
	if m.services == nil {
		return nil
	}
	return m.services.Library
}

func (m *Module) handleListLibraries(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	vals, err := parseQueryValues(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	limit, offset, err := parsePaginationQuery(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	page, err := svc.ListLibraries(r.Context(), p, app.LibraryFilter{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, page)
}

func (m *Module) handleGetLibrary(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	id, err := requireIDParam(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	lib, err := svc.GetLibrary(r.Context(), p, id)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, lib)
}

func (m *Module) handleBrowseLibraryFolder(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	id, err := requireIDParam(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	vals, err := parseQueryValues(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	limit, offset, err := parsePaginationQuery(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	folderPathBytes, err := parseFolderPathQuery(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	res, err := svc.BrowseFolder(r.Context(), p, app.FolderBrowseFilter{
		LibraryID:       id,
		FolderPathBytes: folderPathBytes,
		Limit:           limit,
		Offset:          offset,
	})
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, res)
}

func (m *Module) handleListSeries(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	vals, err := parseQueryValues(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	limit, offset, err := parsePaginationQuery(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	libraryID, err := parseOptionalIDFilter(vals, "library_id")
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	search, err := parseOptionalSearch(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	page, err := svc.ListSeries(r.Context(), p, app.SeriesFilter{
		LibraryID: libraryID,
		Search:    search,
		Limit:     limit,
		Offset:    offset,
	})
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, page)
}

func (m *Module) handleGetSeries(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	id, err := requireIDParam(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	ser, err := svc.GetSeries(r.Context(), p, id)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, ser)
}

func (m *Module) handleListVolumes(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	vals, err := parseQueryValues(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	limit, offset, err := parsePaginationQuery(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	libraryID, err := parseOptionalIDFilter(vals, "library_id")
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	seriesID, err := parseOptionalIDFilter(vals, "series_id")
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	search, err := parseOptionalSearch(vals)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	page, err := svc.ListVolumes(r.Context(), p, app.VolumeFilter{
		LibraryID: libraryID,
		SeriesID:  seriesID,
		Search:    search,
		Limit:     limit,
		Offset:    offset,
	})
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, page)
}

func (m *Module) handleGetVolume(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryRequest(w, r)
	if !ok {
		return
	}
	id, err := requireIDParam(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	vol, err := svc.GetVolume(r.Context(), p, id)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeLibraryJSON(w, r, vol)
}

func (m *Module) prepareLibraryRequest(w http.ResponseWriter, r *http.Request) (*domain.Principal, app.LibraryService, bool) {
	w.Header().Set("Cache-Control", "no-store")
	p, ok := domain.PrincipalFromContext(r.Context())
	if !ok || p == nil {
		middleware.WriteUnauthorized(w, r)
		return nil, nil, false
	}
	svc := m.libraryService()
	if svc == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return nil, nil, false
	}
	return p, svc, true
}

func writeLibraryJSON(w http.ResponseWriter, r *http.Request, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func requireIDParam(r *http.Request) (string, error) {
	id := r.PathValue("id")
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("%w: id is required", app.ErrInvalidInput)
	}
	return id, nil
}

func parseQueryValues(r *http.Request) (url.Values, error) {
	vals, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid query string: %v", app.ErrInvalidInput, err)
	}
	return vals, nil
}

func parsePaginationQuery(vals url.Values) (limit, offset int, err error) {
	if limit, err = parseOptionalNonNegativeInt(vals, "limit"); err != nil {
		return 0, 0, err
	}
	if offset, err = parseOptionalNonNegativeInt(vals, "offset"); err != nil {
		return 0, 0, err
	}
	if offset > math.MaxInt32 {
		return 0, 0, fmt.Errorf("%w: offset exceeds maximum supported range", app.ErrInvalidInput)
	}
	return limit, offset, nil
}

func parseOptionalNonNegativeInt(vals url.Values, key string) (int, error) {
	vs, ok := vals[key]
	if !ok {
		return 0, nil
	}
	if len(vs) > 1 {
		return 0, fmt.Errorf("%w: duplicate query parameter %q", app.ErrInvalidInput, key)
	}
	v, err := strconv.Atoi(vs[0])
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%w: %s must be a non-negative integer", app.ErrInvalidInput, key)
	}
	return v, nil
}

func parseOptionalIDFilter(vals url.Values, key string) (string, error) {
	vs, ok := vals[key]
	if !ok {
		return "", nil
	}
	if len(vs) > 1 {
		return "", fmt.Errorf("%w: duplicate query parameter %q", app.ErrInvalidInput, key)
	}
	trimmed := strings.TrimSpace(vs[0])
	if trimmed == "" {
		return "", fmt.Errorf("%w: %s must not be empty", app.ErrInvalidInput, key)
	}
	return trimmed, nil
}

func parseOptionalSearch(vals url.Values) (string, error) {
	vs, ok := vals["search"]
	if !ok {
		return "", nil
	}
	if len(vs) > 1 {
		return "", fmt.Errorf("%w: duplicate query parameter \"search\"", app.ErrInvalidInput)
	}
	return vs[0], nil
}

func parseFolderPathQuery(vals url.Values) ([]byte, error) {
	rawVals, hasPath := vals["path"]
	hexVals, hasHex := vals["path_hex"]
	if hasPath && hasHex {
		return nil, fmt.Errorf("%w: specify either path or path_hex, not both", app.ErrInvalidInput)
	}
	if len(rawVals) > 1 || len(hexVals) > 1 {
		return nil, fmt.Errorf("%w: duplicate folder path query parameter", app.ErrInvalidInput)
	}
	if hasPath {
		return []byte(rawVals[0]), nil
	}
	if hasHex {
		if hexVals[0] == "" {
			return nil, nil
		}
		decoded, err := hex.DecodeString(hexVals[0])
		if err != nil {
			return nil, fmt.Errorf("%w: invalid path_hex: %v", app.ErrInvalidInput, err)
		}
		return decoded, nil
	}
	return nil, nil
}
