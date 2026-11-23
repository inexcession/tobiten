package openapi

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

const maxLibraryJSONBodyBytes = 1 << 20

func (m *Module) libraryService() app.LibraryService {
	if m.services == nil {
		return nil
	}
	return m.services.Library
}

func (m *Module) libraryConfigService() app.LibraryConfigService {
	if m.services == nil {
		return nil
	}
	return m.services.LibraryConfig
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

func (m *Module) preparePrincipalPermission(w http.ResponseWriter, r *http.Request, perm domain.Permission) (*domain.Principal, bool) {
	w.Header().Set("Cache-Control", "no-store")
	p, ok := domain.PrincipalFromContext(r.Context())
	if !ok || p == nil {
		middleware.WriteUnauthorized(w, r)
		return nil, false
	}
	if !p.HasPermission(perm) {
		middleware.WriteAppError(w, r, app.ErrForbidden)
		return nil, false
	}
	return p, true
}

func (m *Module) prepareLibraryRequest(w http.ResponseWriter, r *http.Request) (*domain.Principal, app.LibraryService, bool) {
	p, ok := m.preparePrincipalPermission(w, r, domain.PermissionLibraryRead)
	if !ok {
		return nil, nil, false
	}
	svc := m.libraryService()
	if svc == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return nil, nil, false
	}
	return p, svc, true
}

func (m *Module) prepareLibraryConfigRequest(w http.ResponseWriter, r *http.Request) (*domain.Principal, app.LibraryConfigService, bool) {
	p, ok := m.preparePrincipalPermission(w, r, domain.PermissionLibraryAdmin)
	if !ok {
		return nil, nil, false
	}
	svc := m.libraryConfigService()
	if svc == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return nil, nil, false
	}
	return p, svc, true
}

type strictJSONPath string

func (p *strictJSONPath) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("%w: path must be a JSON string", app.ErrInvalidInput)
	}
	if err := validateJSONStringEscapes(data[1 : len(data)-1]); err != nil {
		return err
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%w: invalid path string: %v", app.ErrInvalidInput, err)
	}
	*p = strictJSONPath(s)
	return nil
}

func (p *strictJSONPath) asStringPtr() *string {
	if p == nil {
		return nil
	}
	s := string(*p)
	return &s
}

func validateJSONStringEscapes(inner []byte) error {
	for i := 0; i < len(inner); {
		if inner[i] != '\\' {
			i++
			continue
		}
		if i+1 >= len(inner) {
			return fmt.Errorf("%w: invalid json string escape", app.ErrInvalidInput)
		}
		if inner[i+1] != 'u' {
			i += 2
			continue
		}
		if i+6 > len(inner) {
			return fmt.Errorf("%w: truncated unicode escape", app.ErrInvalidInput)
		}
		u1, err := strconv.ParseUint(string(inner[i+2:i+6]), 16, 16)
		if err != nil {
			return fmt.Errorf("%w: invalid unicode escape", app.ErrInvalidInput)
		}
		i += 6
		if u1 >= 0xDC00 && u1 <= 0xDFFF {
			return fmt.Errorf("%w: unpaired UTF-16 low surrogate in path", app.ErrInvalidInput)
		}
		if u1 >= 0xD800 && u1 <= 0xDBFF {
			if i+6 > len(inner) || inner[i] != '\\' || inner[i+1] != 'u' {
				return fmt.Errorf("%w: unpaired UTF-16 high surrogate in path", app.ErrInvalidInput)
			}
			u2, err := strconv.ParseUint(string(inner[i+2:i+6]), 16, 16)
			if err != nil {
				return fmt.Errorf("%w: invalid unicode escape", app.ErrInvalidInput)
			}
			if u2 < 0xDC00 || u2 > 0xDFFF {
				return fmt.Errorf("%w: unpaired UTF-16 high surrogate in path", app.ErrInvalidInput)
			}
			i += 6
		}
	}
	return nil
}

type createLibraryHTTPRequest struct {
	Name                  string          `json:"name"`
	Mode                  string          `json:"mode"`
	RootPath              *strictJSONPath `json:"root_path"`
	RootPathHex           *string         `json:"root_path_hex"`
	IntakePath            *strictJSONPath `json:"intake_path"`
	IntakePathHex         *string         `json:"intake_path_hex"`
	AutoMetadataWriteback bool            `json:"auto_metadata_writeback"`
	ResolverChain         []string        `json:"resolver_chain"`
	MetadataPrecedence    string          `json:"metadata_precedence"`
	DefaultCoverPolicy    string          `json:"default_cover_policy"`
}

type addLibraryRootHTTPRequest struct {
	Path    *strictJSONPath `json:"path"`
	PathHex *string         `json:"path_hex"`
}

func (m *Module) handleCreateLibrary(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryConfigRequest(w, r)
	if !ok {
		return
	}
	var body createLibraryHTTPRequest
	if err := decodeLibraryJSONBody(w, r, &body); err != nil {
		writeLibraryDecodeError(w, r, err)
		return
	}
	req, err := buildCreateLibraryRequest(body)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	res, err := svc.CreateLibrary(r.Context(), p, req)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, app.LibraryFromDomain(res.Library, res.Roots, p))
}

func (m *Module) handleAddLibraryRoot(w http.ResponseWriter, r *http.Request) {
	p, svc, ok := m.prepareLibraryConfigRequest(w, r)
	if !ok {
		return
	}
	id, err := requireIDParam(r)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	var body addLibraryRootHTTPRequest
	if err := decodeLibraryJSONBody(w, r, &body); err != nil {
		writeLibraryDecodeError(w, r, err)
		return
	}
	pathBytes, err := parseJSONPathPair(body.Path.asStringPtr(), body.PathHex, "path", "path_hex", true)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	root, err := svc.AddRoot(r.Context(), p, app.AddLibraryRootRequest{
		LibraryID: id,
		Path:      pathBytes,
	})
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, app.LibraryRootFromDomain(*root, p))
}

func buildCreateLibraryRequest(body createLibraryHTTPRequest) (app.CreateLibraryRequest, error) {
	rootBytes, err := parseJSONPathPair(body.RootPath.asStringPtr(), body.RootPathHex, "root_path", "root_path_hex", true)
	if err != nil {
		return app.CreateLibraryRequest{}, err
	}
	intakeBytes, err := parseJSONPathPair(body.IntakePath.asStringPtr(), body.IntakePathHex, "intake_path", "intake_path_hex", false)
	if err != nil {
		return app.CreateLibraryRequest{}, err
	}

	var mode domain.LibraryMode
	if strings.TrimSpace(body.Mode) != "" {
		mode, err = domain.ParseLibraryMode(body.Mode)
		if err != nil {
			return app.CreateLibraryRequest{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		}
	}

	var chain []domain.ResolverName
	if body.ResolverChain != nil {
		chain = make([]domain.ResolverName, len(body.ResolverChain))
		for i, raw := range body.ResolverChain {
			chain[i] = domain.ResolverName(strings.ToLower(strings.TrimSpace(raw)))
		}
	}

	var precedence domain.MetadataPrecedence
	if strings.TrimSpace(body.MetadataPrecedence) != "" {
		precedence = domain.MetadataPrecedence(strings.ToUpper(strings.TrimSpace(body.MetadataPrecedence)))
	}

	var coverPolicy domain.CoverPolicy
	if strings.TrimSpace(body.DefaultCoverPolicy) != "" {
		coverPolicy = domain.CoverPolicy(strings.ToUpper(strings.TrimSpace(body.DefaultCoverPolicy)))
	}

	return app.CreateLibraryRequest{
		Name:                  strings.TrimSpace(body.Name),
		Mode:                  mode,
		IntakePath:            intakeBytes,
		Roots:                 [][]byte{rootBytes},
		AutoMetadataWriteback: body.AutoMetadataWriteback,
		ResolverChain:         chain,
		MetadataPrecedence:    precedence,
		DefaultCoverPolicy:    coverPolicy,
	}, nil
}

func decodeLibraryJSONBody(w http.ResponseWriter, r *http.Request, dst any) error {
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, body, maxLibraryJSONBodyBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return err
		}
		return fmt.Errorf("%w: reading request body: %v", app.ErrInvalidInput, err)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("%w: request body must be valid UTF-8", app.ErrInvalidInput)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: invalid json body: %v", app.ErrInvalidInput, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: trailing data after json body", app.ErrInvalidInput)
	}
	return nil
}

func writeLibraryDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	middleware.WriteAppError(w, r, err)
}

func parseJSONPathPair(raw, rawHex *string, field, hexField string, required bool) ([]byte, error) {
	if raw != nil && rawHex != nil {
		return nil, fmt.Errorf("%w: specify either %s or %s, not both", app.ErrInvalidInput, field, hexField)
	}
	if raw == nil && rawHex == nil {
		if required {
			return nil, fmt.Errorf("%w: %s or %s is required", app.ErrInvalidInput, field, hexField)
		}
		return nil, nil
	}
	if raw != nil {
		if *raw == "" {
			return nil, fmt.Errorf("%w: %s must not be empty", app.ErrInvalidInput, field)
		}
		return []byte(*raw), nil
	}
	if *rawHex == "" {
		return nil, fmt.Errorf("%w: %s must not be empty", app.ErrInvalidInput, hexField)
	}
	decoded, err := hex.DecodeString(*rawHex)
	if err != nil || len(decoded) == 0 {
		return nil, fmt.Errorf("%w: invalid %s", app.ErrInvalidInput, hexField)
	}
	return decoded, nil
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
