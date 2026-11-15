package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

var _ storage.CatalogueRepository = (*CatalogueRepository)(nil)

const (
	selectSeriesColumns = `
		SELECT s.id, s.library_id, s.title, s.title_sort, s.language, s.original_title,
		       s.romaji_title, s.franchise, s.edition_name, s.publisher, s.imprint,
		       s.status, s.summary, s.age_rating, s.genres, s.tags, s.cover_policy,
		       s.cover_volume_id, s.cover_image_digest, s.locked_fields, s.external_ids,
		       s.total_volumes, s.release_year, s.created_at, s.updated_at
		FROM series s`

	selectVolumeColumns = `
		SELECT v.id, v.library_id, v.kind, v.number, v.chapter_start, v.chapter_end,
		       v.chapter_range_source, v.title, v.summary, v.release_date, v.page_count,
		       v.preferred_asset_id, v.external_ids, v.locked_fields, v.created_at, v.updated_at
		FROM volumes v`

	selectAssetColumns = `
		SELECT a.id, a.volume_id, a.library_root_id, a.rel_path_bytes, a.rel_path_display,
		       a.rel_path_match, a.file_size, a.file_mtime, a.format, a.page_count,
		       a.cover_page_index, a.suppressed_pages, a.diagnostics, a.missing_since,
		       a.retired_at, a.created_at, a.updated_at
		FROM media_assets a`

	selectAuthorizedAssetFileColumns = `
		SELECT a.id, a.volume_id, r.library_id, a.library_root_id,
		       r.path_bytes, r.path_display, r.state,
		       a.rel_path_bytes, a.rel_path_display,
		       a.file_size, a.format, a.page_count, a.cover_page_index, a.suppressed_pages,
		       a.missing_since, a.retired_at
		FROM media_assets a
		JOIN library_roots r ON r.id = a.library_root_id`

	selectReadingProgressColumns = `
		SELECT rp.user_id, rp.volume_id, rp.locator, rp.total_progression,
		       rp.device, rp.source, rp.completed, rp.updated_at
		FROM reading_progress rp`

	selectAuthorizedAssetCandidatesColumns = `
		SELECT a.id, a.volume_id, r.library_id, a.library_root_id,
		       r.path_bytes, r.path_display, r.state,
		       a.rel_path_bytes, a.rel_path_display, a.rel_path_match,
		       a.file_size, a.file_mtime, a.format, a.page_count,
		       a.cover_page_index, a.suppressed_pages, a.diagnostics,
		       a.missing_since, a.retired_at, a.created_at, a.updated_at
		FROM media_assets a
		JOIN library_roots r ON r.id = a.library_root_id`

	selectSeriesMembershipColumns = `
		SELECT sm.volume_id, sm.series_id, sm.is_primary, sm.kind, sm.position,
		       sm.source, sm.locked_fields
		FROM series_memberships sm
		JOIN series s_m ON s_m.id = sm.series_id`
)

// predicateClause binds a table alias in the query to a LabelPredicate SQL fragment generator.
type predicateClause struct {
	alias      string
	fragmentFn func(*LabelPredicate, string, int) (SQLFragment, error)
}

// guardedQuerySpec describes a parameterized SQL query executed through guardedCatalogueDB.
type guardedQuerySpec struct {
	selectFrom string
	whereParts []string
	args       []any
	predicates []predicateClause
	orderBy    string
	limit      int
	offset     int
}

// guardedCatalogueDB is the shared query boundary for CatalogueRepository.
// Every catalogue SELECT/COUNT executes through query or queryRow, which unconditionally
// requires a non-nil LabelPredicate and applies every declared predicateClause fail-closed.
type guardedCatalogueDB struct {
	db DBTX
}

func (g *guardedCatalogueDB) loadPredicate(ctx context.Context, scope storage.CatalogueReadScope) (*LabelPredicate, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("catalogue repository is not initialized")
	}
	pred, err := LoadLabelPredicate(ctx, g.db, scope.Principal, scope.GroupPolicies, LabelPredicateOptions{
		IncludeTrashed: scope.IncludeTrashed,
	})
	if err != nil {
		return nil, fmt.Errorf("loading label predicate: %w", err)
	}
	if pred == nil {
		return nil, errors.New("label predicate must not be nil")
	}
	return pred, nil
}

func (g *guardedCatalogueDB) prepare(pred *LabelPredicate, spec guardedQuerySpec) (string, []any, error) {
	if g == nil || g.db == nil {
		return "", nil, errors.New("catalogue repository is not initialized")
	}
	if pred == nil {
		return "", nil, errors.New("label predicate is required")
	}
	if len(spec.predicates) == 0 {
		return "", nil, errors.New("catalogue query requires at least one label predicate clause")
	}
	if spec.limit < 0 || spec.offset < 0 {
		return "", nil, fmt.Errorf("invalid pagination limit=%d offset=%d", spec.limit, spec.offset)
	}

	whereParts := slices.Clone(spec.whereParts)
	args := slices.Clone(spec.args)

	for _, p := range spec.predicates {
		if p.fragmentFn == nil {
			return "", nil, errors.New("catalogue query predicate fragment builder must not be nil")
		}
		frag, err := p.fragmentFn(pred, p.alias, len(args)+1)
		if err != nil {
			return "", nil, fmt.Errorf("building %s label predicate: %w", p.alias, err)
		}
		whereParts = append(whereParts, "("+frag.SQL+")")
		args = append(args, frag.Args...)
	}

	query := spec.selectFrom + " WHERE " + strings.Join(whereParts, " AND ") + spec.orderBy
	if spec.limit > 0 {
		args = append(args, spec.limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if spec.offset > 0 {
		args = append(args, spec.offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	return query, args, nil
}

func (g *guardedCatalogueDB) query(ctx context.Context, pred *LabelPredicate, spec guardedQuerySpec) (pgx.Rows, error) {
	query, args, err := g.prepare(pred, spec)
	if err != nil {
		return nil, err
	}
	rows, err := g.db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPgError(err)
	}
	return rows, nil
}

func (g *guardedCatalogueDB) queryRow(ctx context.Context, pred *LabelPredicate, spec guardedQuerySpec) (pgx.Row, error) {
	query, args, err := g.prepare(pred, spec)
	if err != nil {
		return nil, err
	}
	return g.db.QueryRow(ctx, query, args...), nil
}

func queryOne[T any](
	ctx context.Context,
	g *guardedCatalogueDB,
	pred *LabelPredicate,
	spec guardedQuerySpec,
	scanFn func(pgx.Row) (*T, error),
) (*T, error) {
	row, err := g.queryRow(ctx, pred, spec)
	if err != nil {
		return nil, err
	}
	return scanFn(row)
}

func queryList[T any](
	ctx context.Context,
	g *guardedCatalogueDB,
	pred *LabelPredicate,
	spec guardedQuerySpec,
	scanFn func(pgx.Row) (*T, error),
) ([]T, error) {
	rows, err := g.query(ctx, pred, spec)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []T{}
	for rows.Next() {
		item, err := scanFn(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return out, nil
}

func queryCount(
	ctx context.Context,
	g *guardedCatalogueDB,
	pred *LabelPredicate,
	spec guardedQuerySpec,
) (int64, error) {
	row, err := g.queryRow(ctx, pred, spec)
	if err != nil {
		return 0, err
	}
	return scanCountRow(row)
}

// CatalogueRepository implements storage.CatalogueRepository backed by PostgreSQL,
// enforcing LabelPredicate visibility on every read and count path (ADR-0006 §5).
type CatalogueRepository struct {
	guard guardedCatalogueDB
}

// NewCatalogueRepository creates a new CatalogueRepository using the provided database handle.
func NewCatalogueRepository(db DBTX) *CatalogueRepository {
	return &CatalogueRepository{guard: guardedCatalogueDB{db: db}}
}

func (r *CatalogueRepository) loadPredicate(ctx context.Context, scope storage.CatalogueReadScope) (*LabelPredicate, error) {
	if r == nil {
		return nil, errors.New("catalogue repository is not initialized")
	}
	return r.guard.loadPredicate(ctx, scope)
}

// GetSeriesByID retrieves a single series if visible under the principal's label predicate.
func (r *CatalogueRepository) GetSeriesByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
		whereParts: []string{"s.id = $1"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
	}, scanSeriesRow)
}

// ListSeries returns series visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) ListSeries(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	spec := buildSeriesQuerySpec(selectSeriesColumns, filter, " ORDER BY s.title_sort ASC, s.id ASC", true)
	return queryList(ctx, &r.guard, pred, spec, scanSeriesRow)
}

// CountSeries returns the count of series visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) CountSeries(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	spec := buildSeriesQuerySpec("SELECT COUNT(*) FROM series s", filter, "", false)
	return queryCount(ctx, &r.guard, pred, spec)
}

// GetVolumeByID retrieves a single volume if visible under the principal's label predicate,
// populating its authorized Memberships and Assets and clearing PreferredAssetID when unauthorized.
func (r *CatalogueRepository) GetVolumeByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	vol, err := r.getVolumeWithPredicate(ctx, pred, id)
	if err != nil {
		return nil, err
	}
	vols := []domain.Volume{*vol}
	if err := r.populateVolumeAssociationsWithPredicate(ctx, pred, vols); err != nil {
		return nil, err
	}
	return &vols[0], nil
}

func (r *CatalogueRepository) getVolumeWithPredicate(ctx context.Context, pred *LabelPredicate, id string) (*domain.Volume, error) {
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectVolumeColumns,
		whereParts: []string{"v.id = $1"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "v", fragmentFn: (*LabelPredicate).VolumeFragment}},
	}, scanVolumeRow)
}

// ListVolumes returns volumes visible under the principal's label predicate matching filter,
// populating each volume's authorized Memberships and Assets and clearing PreferredAssetID when unauthorized.
func (r *CatalogueRepository) ListVolumes(ctx context.Context, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter) ([]domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	orderBy := " ORDER BY v.created_at ASC, v.id ASC"
	if filter.SeriesID != "" {
		orderBy = " ORDER BY sm_f.position ASC NULLS LAST, v.number ASC NULLS LAST, v.id ASC"
	}
	spec := buildVolumeQuerySpec(selectVolumeColumns, filter, orderBy, true)
	out, err := queryList(ctx, &r.guard, pred, spec, scanVolumeRow)
	if err != nil {
		return nil, err
	}
	if len(out) > 0 {
		if err := r.populateVolumeAssociationsWithPredicate(ctx, pred, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *CatalogueRepository) populateVolumeAssociationsWithPredicate(ctx context.Context, pred *LabelPredicate, volumes []domain.Volume) error {
	if len(volumes) == 0 {
		return nil
	}
	volumeIDs := make([]string, len(volumes))
	for i := range volumes {
		volumeIDs[i] = volumes[i].ID
	}

	membershipsByVol, err := r.listAuthorizedMembershipsByVolumeIDs(ctx, pred, volumeIDs)
	if err != nil {
		return err
	}
	assetsByVol, err := r.listAuthorizedAssetsByVolumeIDs(ctx, pred, volumeIDs)
	if err != nil {
		return err
	}

	for i := range volumes {
		v := &volumes[i]
		v.Memberships = membershipsByVol[v.ID]
		if v.Memberships == nil {
			v.Memberships = []domain.SeriesMembership{}
		}
		v.Assets = assetsByVol[v.ID]
		if v.Assets == nil {
			v.Assets = []domain.MediaAsset{}
		}
		if v.PreferredAssetID != nil && !hasAuthorizedAssetID(v.Assets, *v.PreferredAssetID) {
			v.PreferredAssetID = nil
		}
	}
	return nil
}

func hasAuthorizedAssetID(assets []domain.MediaAsset, assetID string) bool {
	for i := range assets {
		if assets[i].ID == assetID {
			return true
		}
	}
	return false
}

func (r *CatalogueRepository) listAuthorizedMembershipsByVolumeIDs(
	ctx context.Context,
	pred *LabelPredicate,
	volumeIDs []string,
) (map[string][]domain.SeriesMembership, error) {
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: selectSeriesMembershipColumns,
		whereParts: []string{"sm.volume_id = ANY($1::text[])"},
		args:       []any{volumeIDs},
		predicates: []predicateClause{{alias: "s_m", fragmentFn: (*LabelPredicate).SeriesFragment}},
		orderBy:    " ORDER BY sm.volume_id ASC, sm.is_primary DESC, sm.series_id ASC",
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byVol := make(map[string][]domain.SeriesMembership, len(volumeIDs))
	for rows.Next() {
		item, err := scanSeriesMembershipRow(rows)
		if err != nil {
			return nil, err
		}
		byVol[item.VolumeID] = append(byVol[item.VolumeID], item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return byVol, nil
}

func (r *CatalogueRepository) listAuthorizedAssetsByVolumeIDs(
	ctx context.Context,
	pred *LabelPredicate,
	volumeIDs []string,
) (map[string][]domain.MediaAsset, error) {
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: selectAssetColumns,
		whereParts: []string{"a.volume_id = ANY($1::text[])"},
		args:       []any{volumeIDs},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
		orderBy:    " ORDER BY a.volume_id ASC, a.created_at ASC, a.id ASC",
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byVol := make(map[string][]domain.MediaAsset, len(volumeIDs))
	for rows.Next() {
		item, err := scanAssetRow(rows)
		if err != nil {
			return nil, err
		}
		byVol[item.VolumeID] = append(byVol[item.VolumeID], *item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return byVol, nil
}

// CountVolumes returns the count of volumes visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) CountVolumes(ctx context.Context, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	spec := buildVolumeQuerySpec("SELECT COUNT(*) FROM volumes v", filter, "", false)
	return queryCount(ctx, &r.guard, pred, spec)
}

// GetAssetByID retrieves a single media asset if visible under the principal's label predicate.
func (r *CatalogueRepository) GetAssetByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.MediaAsset, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectAssetColumns,
		whereParts: []string{"a.id = $1"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
	}, scanAssetRow)
}

// ListAssetsByVolume returns all visible media asset manifestations for volumeID.
func (r *CatalogueRepository) ListAssetsByVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) ([]domain.MediaAsset, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryList(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectAssetColumns,
		whereParts: []string{"a.volume_id = $1"},
		args:       []any{volumeID},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
		orderBy:    " ORDER BY a.created_at ASC, a.id ASC",
	}, scanAssetRow)
}

// CountAssetsByVolume returns the count of visible media asset manifestations for volumeID.
func (r *CatalogueRepository) CountAssetsByVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	return queryCount(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: "SELECT COUNT(*) FROM media_assets a",
		whereParts: []string{"a.volume_id = $1"},
		args:       []any{volumeID},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
	})
}

// GetAuthorizedAssetFile retrieves the root and asset path projection for assetID
// only when the asset, its parent volume, and its series satisfy the label predicate.
func (r *CatalogueRepository) GetAuthorizedAssetFile(ctx context.Context, scope storage.CatalogueReadScope, assetID string) (*storage.AuthorizedAssetFile, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectAuthorizedAssetFileColumns,
		whereParts: []string{"a.id = $1"},
		args:       []any{assetID},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
	}, scanAuthorizedAssetFileRow)
}

// SelectPreferredAssetFileForVolume resolves the preferred readable asset file projection for volumeID
// per ADR-0018 S5 (explicit preference -> user's progress locator -> oldest eligible available asset),
// strictly constrained by the principal's label predicate. Trashed items (marked_for_deletion) are always
// excluded from preferred readable selection even when scope.IncludeTrashed is set on an admin trash view.
func (r *CatalogueRepository) SelectPreferredAssetFileForVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) (*storage.AuthorizedAssetFile, error) {
	readableScope := scope
	readableScope.IncludeTrashed = false
	pred, err := r.loadPredicate(ctx, readableScope)
	if err != nil {
		return nil, err
	}
	vol, err := r.getVolumeWithPredicate(ctx, pred, volumeID)
	if err != nil {
		return nil, err
	}
	candidates, filesByID, err := r.listAuthorizedAssetCandidatesWithPredicate(ctx, pred, volumeID)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, storage.ErrNotFound
	}

	locatorAssetID := ""
	if userID := principalUserID(scope); userID != "" {
		prog, err := r.getReadingProgressWithPredicate(ctx, pred, userID, volumeID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if err == nil && prog != nil {
			locatorAssetID = prog.Locator.AssetID
		}
	}

	vol.Assets = candidates
	chosen, ok := vol.SelectPreferredAsset(locatorAssetID, func(a domain.MediaAsset) bool {
		_, permitted := filesByID[a.ID]
		return permitted
	})
	if !ok || chosen == nil {
		return nil, storage.ErrNotFound
	}
	file := filesByID[chosen.ID]
	return &file, nil
}

func (r *CatalogueRepository) listAuthorizedAssetCandidatesWithPredicate(
	ctx context.Context,
	pred *LabelPredicate,
	volumeID string,
) ([]domain.MediaAsset, map[string]storage.AuthorizedAssetFile, error) {
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: selectAuthorizedAssetCandidatesColumns,
		whereParts: []string{"a.volume_id = $1"},
		args:       []any{volumeID},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
		orderBy:    " ORDER BY a.created_at ASC, a.id ASC",
	})
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var assets []domain.MediaAsset
	filesByID := make(map[string]storage.AuthorizedAssetFile)
	for rows.Next() {
		asset, file, err := scanAuthorizedAssetCandidateRow(rows)
		if err != nil {
			return nil, nil, err
		}
		assets = append(assets, asset)
		filesByID[asset.ID] = file
	}
	if err := rows.Err(); err != nil {
		return nil, nil, mapPgError(err)
	}
	return assets, filesByID, nil
}

// GetReadingProgress retrieves the requesting user's reading progress for volumeID
// when the volume is visible under the principal's label predicate.
func (r *CatalogueRepository) GetReadingProgress(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) (*domain.ReadingProgress, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return r.getReadingProgressWithPredicate(ctx, pred, principalUserID(scope), volumeID)
}

func (r *CatalogueRepository) getReadingProgressWithPredicate(
	ctx context.Context,
	pred *LabelPredicate,
	userID string,
	volumeID string,
) (*domain.ReadingProgress, error) {
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectReadingProgressColumns,
		whereParts: []string{"rp.user_id = $1", "rp.volume_id = $2"},
		args:       []any{userID, volumeID},
		predicates: []predicateClause{{alias: "rp", fragmentFn: (*LabelPredicate).ReadingProgressFragment}},
	}, scanReadingProgressRow)
}

// ListReadingProgress returns the requesting user's reading progress entries
// for volumes visible under the principal's label predicate.
func (r *CatalogueRepository) ListReadingProgress(ctx context.Context, scope storage.CatalogueReadScope, filter storage.ReadingProgressReadFilter) ([]domain.ReadingProgress, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	spec := buildProgressQuerySpec(selectReadingProgressColumns, principalUserID(scope), filter, " ORDER BY rp.updated_at DESC, rp.volume_id ASC", true)
	return queryList(ctx, &r.guard, pred, spec, scanReadingProgressRow)
}

// CountReadingProgress returns the count of the requesting user's reading progress entries
// for volumes visible under the principal's label predicate.
func (r *CatalogueRepository) CountReadingProgress(ctx context.Context, scope storage.CatalogueReadScope, filter storage.ReadingProgressReadFilter) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	spec := buildProgressQuerySpec("SELECT COUNT(*) FROM reading_progress rp", principalUserID(scope), filter, "", false)
	return queryCount(ctx, &r.guard, pred, spec)
}

func scanCountRow(row pgx.Row) (int64, error) {
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, mapPgError(err)
	}
	return count, nil
}

func principalUserID(scope storage.CatalogueReadScope) string {
	if scope.Principal == nil {
		return ""
	}
	return scope.Principal.User.ID
}

func escapeILIKE(term string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	)
	return "%" + replacer.Replace(term) + "%"
}

func buildSeriesQuerySpec(selectFrom string, filter storage.SeriesReadFilter, orderBy string, withPagination bool) guardedQuerySpec {
	var whereParts []string
	var args []any

	if filter.LibraryID != "" {
		args = append(args, filter.LibraryID)
		whereParts = append(whereParts, fmt.Sprintf("s.library_id = $%d", len(args)))
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		args = append(args, escapeILIKE(search))
		p := len(args)
		whereParts = append(whereParts, fmt.Sprintf(
			"(s.title ILIKE $%d ESCAPE '\\' OR COALESCE(s.romaji_title, '') ILIKE $%d ESCAPE '\\' OR COALESCE(s.original_title, '') ILIKE $%d ESCAPE '\\')",
			p, p, p,
		))
	}

	spec := guardedQuerySpec{
		selectFrom: selectFrom,
		whereParts: whereParts,
		args:       args,
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
		orderBy:    orderBy,
	}
	if withPagination {
		spec.limit = filter.Limit
		spec.offset = filter.Offset
	}
	return spec
}

func buildVolumeQuerySpec(baseSelect string, filter storage.VolumeReadFilter, orderBy string, withPagination bool) guardedQuerySpec {
	var whereParts []string
	var args []any
	predicates := []predicateClause{{alias: "v", fragmentFn: (*LabelPredicate).VolumeFragment}}

	if filter.SeriesID != "" {
		baseSelect += " JOIN series_memberships sm_f ON sm_f.volume_id = v.id JOIN series s_f ON s_f.id = sm_f.series_id"
		args = append(args, filter.SeriesID)
		whereParts = append(whereParts, fmt.Sprintf("sm_f.series_id = $%d", len(args)))
		// Guard the series membership join so hidden series cannot be enumerated via cross-series volumes.
		predicates = append(predicates, predicateClause{alias: "s_f", fragmentFn: (*LabelPredicate).SeriesFragment})
	}
	if filter.LibraryID != "" {
		args = append(args, filter.LibraryID)
		whereParts = append(whereParts, fmt.Sprintf("v.library_id = $%d", len(args)))
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		args = append(args, escapeILIKE(search))
		whereParts = append(whereParts, fmt.Sprintf("COALESCE(v.title, '') ILIKE $%d ESCAPE '\\'", len(args)))
	}

	spec := guardedQuerySpec{
		selectFrom: baseSelect,
		whereParts: whereParts,
		args:       args,
		predicates: predicates,
		orderBy:    orderBy,
	}
	if withPagination {
		spec.limit = filter.Limit
		spec.offset = filter.Offset
	}
	return spec
}

func buildProgressQuerySpec(selectFrom, userID string, filter storage.ReadingProgressReadFilter, orderBy string, withPagination bool) guardedQuerySpec {
	args := []any{userID}
	whereParts := []string{"rp.user_id = $1"}

	if filter.Completed != nil {
		args = append(args, *filter.Completed)
		whereParts = append(whereParts, fmt.Sprintf("rp.completed = $%d", len(args)))
	}

	spec := guardedQuerySpec{
		selectFrom: selectFrom,
		whereParts: whereParts,
		args:       args,
		predicates: []predicateClause{{alias: "rp", fragmentFn: (*LabelPredicate).ReadingProgressFragment}},
		orderBy:    orderBy,
	}
	if withPagination {
		spec.limit = filter.Limit
		spec.offset = filter.Offset
	}
	return spec
}

func scanSeriesRow(row pgx.Row) (*domain.Series, error) {
	var (
		s             domain.Series
		status        string
		coverPolicy   *string
		lockedBytes   []byte
		externalBytes []byte
		totalVolumes  pgtype.Int4
		releaseYear   pgtype.Int4
	)
	err := row.Scan(
		&s.ID,
		&s.LibraryID,
		&s.Title,
		&s.TitleSort,
		&s.Language,
		&s.OriginalTitle,
		&s.RomajiTitle,
		&s.Franchise,
		&s.EditionName,
		&s.Publisher,
		&s.Imprint,
		&status,
		&s.Summary,
		&s.AgeRating,
		&s.Genres,
		&s.Tags,
		&coverPolicy,
		&s.CoverVolumeID,
		&s.CoverImageDigest,
		&lockedBytes,
		&externalBytes,
		&totalVolumes,
		&releaseYear,
		&s.CreatedAt,
		&s.UpdatedAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}
	s.Status = domain.SeriesStatus(status)
	if coverPolicy != nil {
		cp := domain.CoverPolicy(*coverPolicy)
		s.CoverPolicy = &cp
	}
	if totalVolumes.Valid {
		tv := int(totalVolumes.Int32)
		s.TotalVolumes = &tv
	}
	if releaseYear.Valid {
		ry := int(releaseYear.Int32)
		s.ReleaseYear = &ry
	}
	s.LockedFields, err = decodeFieldLocks(lockedBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding series locked_fields: %w", err)
	}
	s.ExternalIDs, err = decodeJSONMap(externalBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding series external_ids: %w", err)
	}
	return &s, nil
}

func scanVolumeRow(row pgx.Row) (*domain.Volume, error) {
	var (
		v                  domain.Volume
		kind               string
		number             pgtype.Float8
		chapterStart       pgtype.Float8
		chapterEnd         pgtype.Float8
		chapterRangeSource *string
		releaseDate        pgtype.Date
		pageCount          int32
		externalBytes      []byte
		lockedBytes        []byte
	)
	err := row.Scan(
		&v.ID,
		&v.LibraryID,
		&kind,
		&number,
		&chapterStart,
		&chapterEnd,
		&chapterRangeSource,
		&v.Title,
		&v.Summary,
		&releaseDate,
		&pageCount,
		&v.PreferredAssetID,
		&externalBytes,
		&lockedBytes,
		&v.CreatedAt,
		&v.UpdatedAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}
	v.Kind = domain.VolumeKind(kind)
	if number.Valid {
		n := number.Float64
		v.Number = &n
	}
	if chapterStart.Valid {
		cs := chapterStart.Float64
		v.ChapterStart = &cs
	}
	if chapterEnd.Valid {
		ce := chapterEnd.Float64
		v.ChapterEnd = &ce
	}
	if chapterRangeSource != nil {
		crs := domain.ChapterRangeSource(*chapterRangeSource)
		v.ChapterRangeSource = &crs
	}
	if releaseDate.Valid {
		rd := releaseDate.Time
		v.ReleaseDate = &rd
	}
	v.PageCount = int(pageCount)
	v.ExternalIDs, err = decodeJSONMap(externalBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding volume external_ids: %w", err)
	}
	v.LockedFields, err = decodeFieldLocks(lockedBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding volume locked_fields: %w", err)
	}
	return &v, nil
}

func scanSeriesMembershipRow(row pgx.Row) (domain.SeriesMembership, error) {
	var (
		m           domain.SeriesMembership
		kind        string
		position    pgtype.Float8
		source      string
		lockedBytes []byte
	)
	err := row.Scan(
		&m.VolumeID,
		&m.SeriesID,
		&m.IsPrimary,
		&kind,
		&position,
		&source,
		&lockedBytes,
	)
	if err != nil {
		return domain.SeriesMembership{}, mapPgError(err)
	}
	m.Kind = domain.MembershipKind(kind)
	if position.Valid {
		p := position.Float64
		m.Position = &p
	}
	m.Source = domain.MembershipSource(source)
	m.LockedFields, err = decodeFieldLocks(lockedBytes)
	if err != nil {
		return domain.SeriesMembership{}, fmt.Errorf("decoding series_membership locked_fields: %w", err)
	}
	return m, nil
}

func scanAssetRow(row pgx.Row) (*domain.MediaAsset, error) {
	var (
		a                domain.MediaAsset
		format           string
		pageCount        int32
		coverPageIndex   int32
		suppressed       []int32
		diagnosticsBytes []byte
	)
	err := row.Scan(
		&a.ID,
		&a.VolumeID,
		&a.LibraryRootID,
		&a.RelPathBytes,
		&a.RelPathDisplay,
		&a.RelPathMatch,
		&a.FileSize,
		&a.FileMtime,
		&format,
		&pageCount,
		&coverPageIndex,
		&suppressed,
		&diagnosticsBytes,
		&a.MissingSince,
		&a.RetiredAt,
		&a.CreatedAt,
		&a.UpdatedAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}
	a.Format = domain.MediaFormat(format)
	a.PageCount = int(pageCount)
	a.CoverPageIndex = int(coverPageIndex)
	a.SuppressedPages = int32SliceToInt(suppressed)
	a.Diagnostics, err = decodeAssetDiagnostics(diagnosticsBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding asset diagnostics: %w", err)
	}
	return &a, nil
}

func scanAuthorizedAssetFileRow(row pgx.Row) (*storage.AuthorizedAssetFile, error) {
	var (
		f              storage.AuthorizedAssetFile
		format         string
		pageCount      int32
		coverPageIndex int32
		suppressed     []int32
	)
	err := row.Scan(
		&f.AssetID,
		&f.VolumeID,
		&f.LibraryID,
		&f.LibraryRootID,
		&f.RootPathBytes,
		&f.RootPathDisplay,
		&f.RootState,
		&f.RelPathBytes,
		&f.RelPathDisplay,
		&f.FileSize,
		&format,
		&pageCount,
		&coverPageIndex,
		&suppressed,
		&f.MissingSince,
		&f.RetiredAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}
	f.Format = domain.MediaFormat(format)
	f.PageCount = int(pageCount)
	f.CoverPageIndex = int(coverPageIndex)
	f.SuppressedPages = int32SliceToInt(suppressed)
	return &f, nil
}

func scanAuthorizedAssetCandidateRow(row pgx.Row) (domain.MediaAsset, storage.AuthorizedAssetFile, error) {
	var (
		a                domain.MediaAsset
		f                storage.AuthorizedAssetFile
		format           string
		pageCount        int32
		coverPageIndex   int32
		suppressed       []int32
		diagnosticsBytes []byte
	)
	err := row.Scan(
		&a.ID,
		&a.VolumeID,
		&f.LibraryID,
		&a.LibraryRootID,
		&f.RootPathBytes,
		&f.RootPathDisplay,
		&f.RootState,
		&a.RelPathBytes,
		&a.RelPathDisplay,
		&a.RelPathMatch,
		&a.FileSize,
		&a.FileMtime,
		&format,
		&pageCount,
		&coverPageIndex,
		&suppressed,
		&diagnosticsBytes,
		&a.MissingSince,
		&a.RetiredAt,
		&a.CreatedAt,
		&a.UpdatedAt,
	)
	if err != nil {
		return domain.MediaAsset{}, storage.AuthorizedAssetFile{}, mapPgError(err)
	}
	a.Format = domain.MediaFormat(format)
	a.PageCount = int(pageCount)
	a.CoverPageIndex = int(coverPageIndex)
	a.SuppressedPages = int32SliceToInt(suppressed)
	a.Diagnostics, err = decodeAssetDiagnostics(diagnosticsBytes)
	if err != nil {
		return domain.MediaAsset{}, storage.AuthorizedAssetFile{}, fmt.Errorf("decoding asset diagnostics: %w", err)
	}

	f.AssetID = a.ID
	f.VolumeID = a.VolumeID
	f.LibraryRootID = a.LibraryRootID
	f.RelPathBytes = a.RelPathBytes
	f.RelPathDisplay = a.RelPathDisplay
	f.FileSize = a.FileSize
	f.Format = a.Format
	f.PageCount = a.PageCount
	f.CoverPageIndex = a.CoverPageIndex
	f.SuppressedPages = a.SuppressedPages
	f.MissingSince = a.MissingSince
	f.RetiredAt = a.RetiredAt
	return a, f, nil
}

func scanReadingProgressRow(row pgx.Row) (*domain.ReadingProgress, error) {
	var (
		rp           domain.ReadingProgress
		locatorBytes []byte
		totalProg    float32
	)
	err := row.Scan(
		&rp.UserID,
		&rp.VolumeID,
		&locatorBytes,
		&totalProg,
		&rp.Device,
		&rp.Source,
		&rp.Completed,
		&rp.UpdatedAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}
	if len(locatorBytes) > 0 && string(locatorBytes) != "null" {
		if err := json.Unmarshal(locatorBytes, &rp.Locator); err != nil {
			return nil, fmt.Errorf("decoding reading_progress locator: %w", err)
		}
	}
	rp.TotalProgression = float64(totalProg)
	return &rp, nil
}

func decodeFieldLocks(raw []byte) (domain.FieldLocks, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return domain.FieldLocks{}, nil
	}
	var out domain.FieldLocks
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = domain.FieldLocks{}
	}
	return out, nil
}

func decodeJSONMap(raw []byte) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func decodeAssetDiagnostics(raw []byte) (domain.AssetDiagnostics, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return domain.AssetDiagnostics{}, nil
	}
	var out domain.AssetDiagnostics
	if err := json.Unmarshal(raw, &out); err != nil {
		return domain.AssetDiagnostics{}, err
	}
	return out, nil
}

func int32SliceToInt(in []int32) []int {
	if len(in) == 0 {
		return []int{}
	}
	out := make([]int, len(in))
	for i, v := range in {
		out[i] = int(v)
	}
	return out
}
