package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

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

	selectVolumePagedColumnsWithSeries = `
		SELECT v.id, v.library_id, v.kind, v.number, v.chapter_start, v.chapter_end,
		       v.chapter_range_source, v.title, v.summary, v.release_date, v.page_count,
		       v.preferred_asset_id, v.external_ids, v.locked_fields, v.created_at, v.updated_at,
		       sm_f.position AS membership_position
		FROM volumes v`

	selectVolumePagedColumnsNoSeries = `
		SELECT v.id, v.library_id, v.kind, v.number, v.chapter_start, v.chapter_end,
		       v.chapter_range_source, v.title, v.summary, v.release_date, v.page_count,
		       v.preferred_asset_id, v.external_ids, v.locked_fields, v.created_at, v.updated_at,
		       NULL::double precision AS membership_position
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
		       a.file_size, a.format, COALESCE(d.kind, ''), COALESCE(d.value, ''),
		       a.page_count, a.cover_page_index, a.suppressed_pages,
		       a.missing_since, a.retired_at
		FROM media_assets a
		JOIN library_roots r ON r.id = a.library_root_id
		LEFT JOIN asset_digests d ON d.asset_id = a.id AND d.kind = 'XXH3_64_PARTIAL' AND d.is_current = TRUE`

	selectReadingProgressColumns = `
		SELECT rp.user_id, rp.volume_id, rp.locator, rp.total_progression,
		       rp.device, rp.source, rp.completed, rp.updated_at
		FROM reading_progress rp`

	selectAuthorizedAssetCandidatesColumns = `
		SELECT a.id, a.volume_id, r.library_id, a.library_root_id,
		       r.path_bytes, r.path_display, r.state,
		       a.rel_path_bytes, a.rel_path_display, a.rel_path_match,
		       a.file_size, a.file_mtime, a.format, COALESCE(d.kind, ''), COALESCE(d.value, ''),
		       a.page_count, a.cover_page_index, a.suppressed_pages, a.diagnostics,
		       a.missing_since, a.retired_at, a.created_at, a.updated_at
		FROM media_assets a
		JOIN library_roots r ON r.id = a.library_root_id
		LEFT JOIN asset_digests d ON d.asset_id = a.id AND d.kind = 'XXH3_64_PARTIAL' AND d.is_current = TRUE`

	selectSeriesMembershipColumns = `
		SELECT sm.volume_id, sm.series_id, sm.is_primary, sm.kind, sm.position,
		       sm.source, sm.locked_fields
		FROM series_memberships sm
		JOIN series s_m ON s_m.id = sm.series_id`

	selectFolderBrowseEntries = `
		SELECT
			entry.is_folder AS is_folder,
			entry.child_name_bytes AS child_name_bytes,
			entry.entry_rel_path_bytes AS entry_rel_path_bytes,
			entry.asset_id AS asset_id,
			MAX(CASE WHEN NOT entry.is_folder THEN a.volume_id END) AS volume_id,
			MAX(CASE WHEN NOT entry.is_folder THEN a.library_root_id END) AS library_root_id,
			MAX(CASE WHEN NOT entry.is_folder THEN a.file_size END) AS file_size,
			MAX(CASE WHEN NOT entry.is_folder THEN a.format END) AS format,
			MAX(CASE WHEN NOT entry.is_folder THEN a.page_count END) AS page_count
		FROM media_assets a
		JOIN library_roots r ON r.id = a.library_root_id
		CROSS JOIN LATERAL (
			SELECT
				(p.slash_pos > 0) AS is_folder,
				CASE
					WHEN p.slash_pos > 0 THEN substring(a.rel_path_bytes FROM $2::int FOR p.slash_pos - 1)
					ELSE substring(a.rel_path_bytes FROM $2::int)
				END AS child_name_bytes,
				CASE
					WHEN p.slash_pos > 0 THEN substring(a.rel_path_bytes FROM 1 FOR $3::int + p.slash_pos - 1)
					ELSE a.rel_path_bytes
				END AS entry_rel_path_bytes,
				CASE
					WHEN p.slash_pos = 0 THEN a.id
					ELSE NULL::text
				END AS asset_id
			FROM (SELECT position('/'::bytea IN substring(a.rel_path_bytes FROM $2::int)) AS slash_pos) p
		) entry`
)

var seriesPagedColumns = []string{
	"id",
	"library_id",
	"title",
	"title_sort",
	"language",
	"original_title",
	"romaji_title",
	"franchise",
	"edition_name",
	"publisher",
	"imprint",
	"status",
	"summary",
	"age_rating",
	"genres",
	"tags",
	"cover_policy",
	"cover_volume_id",
	"cover_image_digest",
	"locked_fields",
	"external_ids",
	"total_volumes",
	"release_year",
	"created_at",
	"updated_at",
}

var volumePagedColumns = []string{
	"id",
	"library_id",
	"kind",
	"number",
	"chapter_start",
	"chapter_end",
	"chapter_range_source",
	"title",
	"summary",
	"release_date",
	"page_count",
	"preferred_asset_id",
	"external_ids",
	"locked_fields",
	"created_at",
	"updated_at",
	"membership_position",
}

var folderBrowseColumns = []string{
	"is_folder",
	"child_name_bytes",
	"entry_rel_path_bytes",
	"asset_id",
	"volume_id",
	"library_root_id",
	"file_size",
	"format",
	"page_count",
}

// predicateClause binds a table alias in the query to a LabelPredicate SQL fragment generator.
type predicateClause struct {
	alias      string
	fragmentFn func(*LabelPredicate, string, int) (SQLFragment, error)
}

// guardedPagedTotalSpec wraps a guarded grouped/filtered entry query into a single
// snapshot statement that returns total_count alongside the ordered, paginated page rows,
// preserving total_count even when zero entries exist or offset >= total_count.
type guardedPagedTotalSpec struct {
	columns []string
	orderBy string
}

// guardedQuerySpec describes a parameterized SQL query executed through guardedCatalogueDB.
type guardedQuerySpec struct {
	selectFrom string
	whereParts []string
	args       []any
	predicates []predicateClause
	groupBy    string
	orderBy    string
	limit      int
	offset     int
	pagedTotal *guardedPagedTotalSpec
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

	if spec.pagedTotal != nil {
		if spec.limit <= 0 {
			return "", nil, fmt.Errorf("paged total query requires positive limit, got %d", spec.limit)
		}
		if len(spec.pagedTotal.columns) == 0 || strings.TrimSpace(spec.pagedTotal.orderBy) == "" {
			return "", nil, errors.New("paged total query requires columns and orderBy")
		}
		baseQuery := spec.selectFrom + " WHERE " + strings.Join(whereParts, " AND ") + spec.groupBy
		args = append(args, spec.limit)
		limitParam := len(args)
		offsetParam := 0
		if spec.offset > 0 {
			args = append(args, spec.offset)
			offsetParam = len(args)
		}
		return wrapPagedTotalQuery(baseQuery, spec.pagedTotal, limitParam, offsetParam), args, nil
	}

	query := spec.selectFrom + " WHERE " + strings.Join(whereParts, " AND ") + spec.groupBy + spec.orderBy
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

func wrapPagedTotalQuery(baseQuery string, paged *guardedPagedTotalSpec, limitParam, offsetParam int) string {
	eCols := make([]string, len(paged.columns))
	pCols := make([]string, len(paged.columns))
	for i, col := range paged.columns {
		eCols[i] = "e." + col
		pCols[i] = "p." + col
	}
	offsetClause := ""
	if offsetParam > 0 {
		offsetClause = fmt.Sprintf(" OFFSET $%d", offsetParam)
	}
	return fmt.Sprintf(`WITH entries AS (%s),
total AS (SELECT COUNT(*)::bigint AS total_count FROM entries),
paged AS (
	SELECT ROW_NUMBER() OVER (ORDER BY %s) AS _row_ord, %s
	FROM entries e
	ORDER BY %s
	LIMIT $%d%s
)
SELECT t.total_count, (p._row_ord IS NOT NULL) AS has_row, %s
FROM total t
LEFT JOIN paged p ON TRUE
ORDER BY p._row_ord ASC NULLS LAST`,
		baseQuery,
		paged.orderBy, strings.Join(eCols, ", "),
		paged.orderBy, limitParam, offsetClause,
		strings.Join(pCols, ", "),
	)
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

func queryScopedOne[T any](
	ctx context.Context,
	r *CatalogueRepository,
	scope storage.CatalogueReadScope,
	spec guardedQuerySpec,
	scanFn func(pgx.Row) (*T, error),
) (*T, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryOne(ctx, &r.guard, pred, spec, scanFn)
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

func queryPagedTotal[T any](
	ctx context.Context,
	g *guardedCatalogueDB,
	pred *LabelPredicate,
	spec guardedQuerySpec,
	scanFn func(pgx.Row) (int64, bool, *T, error),
) ([]T, int64, error) {
	rows, err := g.query(ctx, pred, spec)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var total int64
	out := []T{}
	for rows.Next() {
		rowTotal, hasRow, item, err := scanFn(rows)
		if err != nil {
			return nil, 0, err
		}
		total = rowTotal
		if hasRow {
			out = append(out, *item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, mapPgError(err)
	}
	return out, total, nil
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
	return queryScopedOne(ctx, r, scope, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
		whereParts: []string{"s.id = $1"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
	}, scanSeriesRow)
}

func (r *CatalogueRepository) getSeriesWithPredicate(ctx context.Context, pred *LabelPredicate, id string) (*domain.Series, error) {
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

// ListSeriesPaged returns one server-paginated slice of visible series together with
// total_count computed in one guarded snapshot statement, capping page size at MaxCataloguePageLimit.
func (r *CatalogueRepository) ListSeriesPaged(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) (*storage.SeriesPageResult, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	limit, err := normalizeCataloguePageFilter(filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	spec := buildSeriesQuerySpec(selectSeriesColumns, filter, "", false)
	spec.limit = limit
	spec.offset = filter.Offset
	spec.pagedTotal = &guardedPagedTotalSpec{
		columns: seriesPagedColumns,
		orderBy: "e.title_sort ASC, e.id ASC",
	}
	items, total, err := queryPagedTotal(ctx, &r.guard, pred, spec, scanSeriesPagedRow)
	if err != nil {
		return nil, err
	}
	return &storage.SeriesPageResult{
		Items:      items,
		TotalCount: total,
		Limit:      limit,
		Offset:     filter.Offset,
	}, nil
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

// ListVolumesPaged returns one server-paginated slice of visible volumes together with
// total_count computed in one guarded snapshot statement, populating authorized Memberships
// and Assets and capping page size at MaxCataloguePageLimit.
func (r *CatalogueRepository) ListVolumesPaged(ctx context.Context, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter) (*storage.VolumePageResult, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	limit, err := normalizeCataloguePageFilter(filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	baseSelect := selectVolumePagedColumnsNoSeries
	orderBy := "e.created_at ASC, e.id ASC"
	if filter.SeriesID != "" {
		baseSelect = selectVolumePagedColumnsWithSeries
		orderBy = "e.membership_position ASC NULLS LAST, e.number ASC NULLS LAST, e.id ASC"
	}
	spec := buildVolumeQuerySpec(baseSelect, filter, "", false)
	spec.limit = limit
	spec.offset = filter.Offset
	spec.pagedTotal = &guardedPagedTotalSpec{
		columns: volumePagedColumns,
		orderBy: orderBy,
	}
	out, total, err := queryPagedTotal(ctx, &r.guard, pred, spec, scanVolumePagedRow)
	if err != nil {
		return nil, err
	}
	if len(out) > 0 {
		if err := r.populateVolumeAssociationsWithPredicate(ctx, pred, out); err != nil {
			return nil, err
		}
	}
	return &storage.VolumePageResult{
		Items:      out,
		TotalCount: total,
		Limit:      limit,
		Offset:     filter.Offset,
	}, nil
}

func normalizeCataloguePageFilter(limit, offset int) (int, error) {
	if limit < 0 || offset < 0 || offset > math.MaxInt32 {
		return 0, fmt.Errorf("invalid pagination limit=%d offset=%d", limit, offset)
	}
	if limit == 0 {
		return storage.DefaultCataloguePageLimit, nil
	}
	if limit > storage.MaxCataloguePageLimit {
		return storage.MaxCataloguePageLimit, nil
	}
	return limit, nil
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
		whereParts: []string{"a.volume_id = ANY($1::text[])", "a.missing_since IS NULL", "a.retired_at IS NULL"},
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

// GetAssetByID retrieves a single available media asset if visible under the principal's label predicate.
func (r *CatalogueRepository) GetAssetByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.MediaAsset, error) {
	return queryScopedOne(ctx, r, scope, guardedQuerySpec{
		selectFrom: selectAssetColumns,
		whereParts: []string{"a.id = $1", "a.missing_since IS NULL", "a.retired_at IS NULL"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
	}, scanAssetRow)
}

// ListAssetsByVolume returns all visible, available media asset manifestations for volumeID.
func (r *CatalogueRepository) ListAssetsByVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) ([]domain.MediaAsset, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryList(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectAssetColumns,
		whereParts: []string{"a.volume_id = $1", "a.missing_since IS NULL", "a.retired_at IS NULL"},
		args:       []any{volumeID},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
		orderBy:    " ORDER BY a.created_at ASC, a.id ASC",
	}, scanAssetRow)
}

// CountAssetsByVolume returns the count of visible, available media asset manifestations for volumeID.
func (r *CatalogueRepository) CountAssetsByVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	return queryCount(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: "SELECT COUNT(*) FROM media_assets a",
		whereParts: []string{"a.volume_id = $1", "a.missing_since IS NULL", "a.retired_at IS NULL"},
		args:       []any{volumeID},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
	})
}

// GetAuthorizedAssetFile retrieves the root and asset path projection for available assetID
// only when the asset, its parent volume, and its series satisfy the label predicate.
func (r *CatalogueRepository) GetAuthorizedAssetFile(ctx context.Context, scope storage.CatalogueReadScope, assetID string) (*storage.AuthorizedAssetFile, error) {
	return queryScopedOne(ctx, r, scope, guardedQuerySpec{
		selectFrom: selectAuthorizedAssetFileColumns,
		whereParts: []string{"a.id = $1", "a.missing_since IS NULL", "a.retired_at IS NULL"},
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

// ListAuthorizedVolumeAssetFiles resolves all visible, available, non-trashed asset file
// projections for volumeID ranked in preference order (preferred_asset_id -> user locator ->
// oldest created_at, id). Trashed items are always excluded even when scope.IncludeTrashed is true.
func (r *CatalogueRepository) ListAuthorizedVolumeAssetFiles(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) ([]storage.AuthorizedAssetFile, error) {
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
	filesByVol, err := r.listAuthorizedAssetCandidatesByVolumeIDsWithPredicate(ctx, pred, []string{volumeID})
	if err != nil {
		return nil, err
	}
	orderedFiles := filesByVol[volumeID]
	if len(orderedFiles) == 0 {
		return nil, storage.ErrNotFound
	}
	locatorByVol, err := r.listLocatorAssetIDsByVolumeIDsWithPredicate(ctx, pred, principalUserID(scope), []string{volumeID})
	if err != nil {
		return nil, err
	}
	return rankAuthorizedVolumeAssetFiles(vol.PreferredAssetID, locatorByVol[volumeID], orderedFiles), nil
}

// ListSeriesCoverCandidates returns up to limit visible, non-trashed volumes of seriesID
// in FIRST order (sm.position, v.number, v.id) starting at offset, paired with each volume's
// preference-ranked visible, available, non-trashed asset files. Returns storage.ErrNotFound
// when seriesID itself is missing, restricted, or trashed.
func (r *CatalogueRepository) ListSeriesCoverCandidates(
	ctx context.Context,
	scope storage.CatalogueReadScope,
	seriesID string,
	limit, offset int,
) ([]storage.SeriesCoverCandidate, error) {
	readableScope := scope
	readableScope.IncludeTrashed = false
	pred, err := r.loadPredicate(ctx, readableScope)
	if err != nil {
		return nil, err
	}
	normLimit, err := normalizeCataloguePageFilter(limit, offset)
	if err != nil {
		return nil, err
	}
	if _, err := r.getSeriesWithPredicate(ctx, pred, seriesID); err != nil {
		return nil, err
	}

	vols, err := r.listSeriesCoverVolumesPageWithPredicate(ctx, pred, seriesID, normLimit, offset)
	if err != nil || len(vols) == 0 {
		return nil, err
	}

	volumeIDs := make([]string, len(vols))
	for i := range vols {
		volumeIDs[i] = vols[i].id
	}
	filesByVol, err := r.listAuthorizedAssetCandidatesByVolumeIDsWithPredicate(ctx, pred, volumeIDs)
	if err != nil {
		return nil, err
	}
	locatorByVol, err := r.listLocatorAssetIDsByVolumeIDsWithPredicate(ctx, pred, principalUserID(scope), volumeIDs)
	if err != nil {
		return nil, err
	}

	out := make([]storage.SeriesCoverCandidate, 0, len(vols))
	for _, v := range vols {
		ranked := rankAuthorizedVolumeAssetFiles(v.preferredAssetID, locatorByVol[v.id], filesByVol[v.id])
		if len(ranked) == 0 {
			continue
		}
		out = append(out, storage.SeriesCoverCandidate{
			VolumeID: v.id,
			Files:    ranked,
		})
	}
	return out, nil
}

type seriesCoverVolumeRow struct {
	id               string
	preferredAssetID *string
}

func (r *CatalogueRepository) listSeriesCoverVolumesPageWithPredicate(
	ctx context.Context,
	pred *LabelPredicate,
	seriesID string,
	limit, offset int,
) ([]seriesCoverVolumeRow, error) {
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: `
			SELECT v.id, v.preferred_asset_id
			FROM volumes v
			JOIN series_memberships sm_f ON sm_f.volume_id = v.id
			JOIN series s_f ON s_f.id = sm_f.series_id
			JOIN media_assets a_c ON a_c.volume_id = v.id`,
		whereParts: []string{"sm_f.series_id = $1", "a_c.missing_since IS NULL", "a_c.retired_at IS NULL"},
		args:       []any{seriesID},
		predicates: []predicateClause{
			{alias: "v", fragmentFn: (*LabelPredicate).VolumeFragment},
			{alias: "s_f", fragmentFn: (*LabelPredicate).SeriesFragment},
			{alias: "a_c", fragmentFn: (*LabelPredicate).AssetFragment},
		},
		groupBy: " GROUP BY v.id, v.preferred_asset_id, sm_f.position, v.number",
		orderBy: " ORDER BY sm_f.position ASC NULLS LAST, v.number ASC NULLS LAST, v.id ASC",
		limit:   limit,
		offset:  offset,
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []seriesCoverVolumeRow
	for rows.Next() {
		var row seriesCoverVolumeRow
		if err := rows.Scan(&row.id, &row.preferredAssetID); err != nil {
			return nil, mapPgError(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return out, nil
}

func (r *CatalogueRepository) listAuthorizedAssetCandidatesByVolumeIDsWithPredicate(
	ctx context.Context,
	pred *LabelPredicate,
	volumeIDs []string,
) (map[string][]storage.AuthorizedAssetFile, error) {
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: selectAuthorizedAssetCandidatesColumns,
		whereParts: []string{"a.volume_id = ANY($1::text[])", "a.missing_since IS NULL", "a.retired_at IS NULL"},
		args:       []any{volumeIDs},
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
		orderBy:    " ORDER BY a.volume_id ASC, a.created_at ASC, a.id ASC",
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byVol := make(map[string][]storage.AuthorizedAssetFile, len(volumeIDs))
	for rows.Next() {
		_, file, err := scanAuthorizedAssetCandidateRow(rows)
		if err != nil {
			return nil, err
		}
		byVol[file.VolumeID] = append(byVol[file.VolumeID], file)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return byVol, nil
}

func (r *CatalogueRepository) listLocatorAssetIDsByVolumeIDsWithPredicate(
	ctx context.Context,
	pred *LabelPredicate,
	userID string,
	volumeIDs []string,
) (map[string]string, error) {
	if userID == "" || len(volumeIDs) == 0 {
		return nil, nil
	}
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: "SELECT rp.volume_id, COALESCE(rp.locator->>'asset_id', '') FROM reading_progress rp",
		whereParts: []string{"rp.user_id = $1", "rp.volume_id = ANY($2::text[])"},
		args:       []any{userID, volumeIDs},
		predicates: []predicateClause{{alias: "rp", fragmentFn: (*LabelPredicate).ReadingProgressFragment}},
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string, len(volumeIDs))
	for rows.Next() {
		var volID, assetID string
		if err := rows.Scan(&volID, &assetID); err != nil {
			return nil, mapPgError(err)
		}
		if assetID != "" {
			out[volID] = assetID
		}
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return out, nil
}

func rankAuthorizedVolumeAssetFiles(
	preferredAssetID *string,
	locatorAssetID string,
	orderedFiles []storage.AuthorizedAssetFile,
) []storage.AuthorizedAssetFile {
	if len(orderedFiles) == 0 {
		return nil
	}
	ranked := make([]storage.AuthorizedAssetFile, 0, len(orderedFiles))
	added := make(map[string]bool, len(orderedFiles))
	addByID := func(id string) {
		if id == "" || added[id] {
			return
		}
		for i := range orderedFiles {
			if orderedFiles[i].AssetID == id {
				ranked = append(ranked, orderedFiles[i])
				added[id] = true
				return
			}
		}
	}
	if preferredAssetID != nil {
		addByID(*preferredAssetID)
	}
	addByID(locatorAssetID)
	for i := range orderedFiles {
		if !added[orderedFiles[i].AssetID] {
			ranked = append(ranked, orderedFiles[i])
		}
	}
	return ranked
}

func (r *CatalogueRepository) listAuthorizedAssetCandidatesWithPredicate(
	ctx context.Context,
	pred *LabelPredicate,
	volumeID string,
) ([]domain.MediaAsset, map[string]storage.AuthorizedAssetFile, error) {
	rows, err := r.guard.query(ctx, pred, guardedQuerySpec{
		selectFrom: selectAuthorizedAssetCandidatesColumns,
		whereParts: []string{"a.volume_id = $1", "a.missing_since IS NULL", "a.retired_at IS NULL"},
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

// BrowseFolder returns a server-paginated slice of immediate child folders and available
// file manifestations under filter.FolderPathBytes across all roots of filter.LibraryID,
// enforcing LabelPredicate visibility and capping page size at storage.MaxFolderBrowseLimit.
// Both TotalCount and Entries are computed in one guarded snapshot statement.
func (r *CatalogueRepository) BrowseFolder(ctx context.Context, scope storage.CatalogueReadScope, filter storage.FolderBrowseFilter) (*storage.FolderBrowseResult, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	folderPath, limit, err := normalizeFolderBrowseFilter(filter)
	if err != nil {
		return nil, err
	}

	whereParts, args := buildFolderBrowseWhereAndArgs(filter.LibraryID, folderPath.Bytes)
	entries, total, err := queryPagedTotal(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectFolderBrowseEntries,
		whereParts: whereParts,
		args:       args,
		predicates: []predicateClause{{alias: "a", fragmentFn: (*LabelPredicate).AssetFragment}},
		groupBy:    " GROUP BY entry.is_folder, entry.child_name_bytes, entry.entry_rel_path_bytes, entry.asset_id",
		limit:      limit,
		offset:     filter.Offset,
		pagedTotal: &guardedPagedTotalSpec{
			columns: folderBrowseColumns,
			orderBy: "e.is_folder DESC, e.child_name_bytes ASC, e.asset_id ASC NULLS FIRST",
		},
	}, scanFolderBrowseRow)
	if err != nil {
		return nil, err
	}

	return &storage.FolderBrowseResult{
		LibraryID:  filter.LibraryID,
		FolderPath: folderPath,
		Entries:    entries,
		TotalCount: total,
		Limit:      limit,
		Offset:     filter.Offset,
	}, nil
}

func normalizeFolderBrowseFilter(filter storage.FolderBrowseFilter) (domain.PathIdentity, int, error) {
	if strings.TrimSpace(filter.LibraryID) == "" {
		return domain.PathIdentity{}, 0, domain.ErrEmptyLibraryID
	}
	var folderPath domain.PathIdentity
	if len(filter.FolderPathBytes) > 0 {
		var err error
		folderPath, err = domain.NewRelativePathIdentity(filter.FolderPathBytes)
		if err != nil {
			return domain.PathIdentity{}, 0, err
		}
	}
	if filter.Limit < 0 || filter.Offset < 0 || filter.Offset > math.MaxInt32 {
		return domain.PathIdentity{}, 0, fmt.Errorf("invalid pagination limit=%d offset=%d", filter.Limit, filter.Offset)
	}
	limit := filter.Limit
	if limit == 0 {
		limit = storage.DefaultFolderBrowseLimit
	} else if limit > storage.MaxFolderBrowseLimit {
		limit = storage.MaxFolderBrowseLimit
	}
	return folderPath, limit, nil
}

func buildFolderBrowseWhereAndArgs(libraryID string, prefix []byte) ([]string, []any) {
	remStart := int32(1)
	needleLen := int32(0)
	args := []any{libraryID, remStart, needleLen}
	whereParts := []string{
		"r.library_id = $1",
		"a.missing_since IS NULL",
		"a.retired_at IS NULL",
		"octet_length(a.rel_path_bytes) > $3::int",
		"position('/'::bytea IN substring(a.rel_path_bytes FROM $2::int)) <> 1",
	}

	if len(prefix) > 0 {
		needle := make([]byte, len(prefix)+1)
		copy(needle, prefix)
		needle[len(prefix)] = '/'

		upper := make([]byte, len(prefix)+1)
		copy(upper, prefix)
		upper[len(prefix)] = '0'

		needleLen = int32(len(needle))
		remStart = needleLen + 1
		args[1] = remStart
		args[2] = needleLen
		args = append(args, needle, upper)
		whereParts = append(whereParts,
			"a.rel_path_bytes >= $4",
			"a.rel_path_bytes < $5",
		)
	}

	return whereParts, args
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

func buildDomainSeriesFromScanned(
	s *domain.Series,
	status string,
	coverPolicy *string,
	lockedBytes []byte,
	externalBytes []byte,
	totalVolumes pgtype.Int4,
	releaseYear pgtype.Int4,
) error {
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
	var err error
	s.LockedFields, err = decodeFieldLocks(lockedBytes)
	if err != nil {
		return fmt.Errorf("decoding series locked_fields: %w", err)
	}
	s.ExternalIDs, err = decodeJSONMap(externalBytes)
	if err != nil {
		return fmt.Errorf("decoding series external_ids: %w", err)
	}
	if s.Genres == nil {
		s.Genres = []string{}
	}
	if s.Tags == nil {
		s.Tags = []string{}
	}
	return nil
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
	if err := buildDomainSeriesFromScanned(&s, status, coverPolicy, lockedBytes, externalBytes, totalVolumes, releaseYear); err != nil {
		return nil, err
	}
	return &s, nil
}

func scanSeriesPagedRow(row pgx.Row) (int64, bool, *domain.Series, error) {
	var (
		totalCount    int64
		hasRow        bool
		id            *string
		libraryID     *string
		title         *string
		titleSort     *string
		status        *string
		createdAt     *time.Time
		updatedAt     *time.Time
		s             domain.Series
		coverPolicy   *string
		lockedBytes   []byte
		externalBytes []byte
		totalVolumes  pgtype.Int4
		releaseYear   pgtype.Int4
	)
	err := row.Scan(
		&totalCount,
		&hasRow,
		&id,
		&libraryID,
		&title,
		&titleSort,
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
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return 0, false, nil, mapPgError(err)
	}
	if !hasRow {
		return totalCount, false, nil, nil
	}
	if id == nil || libraryID == nil || title == nil || titleSort == nil || status == nil || createdAt == nil || updatedAt == nil {
		return 0, false, nil, errors.New("paged series row missing required series metadata")
	}
	s.ID = *id
	s.LibraryID = *libraryID
	s.Title = *title
	s.TitleSort = *titleSort
	s.CreatedAt = *createdAt
	s.UpdatedAt = *updatedAt
	if err := buildDomainSeriesFromScanned(&s, *status, coverPolicy, lockedBytes, externalBytes, totalVolumes, releaseYear); err != nil {
		return 0, false, nil, err
	}
	return totalCount, true, &s, nil
}

func buildDomainVolumeFromScanned(
	v *domain.Volume,
	kind string,
	number pgtype.Float8,
	chapterStart pgtype.Float8,
	chapterEnd pgtype.Float8,
	chapterRangeSource *string,
	releaseDate pgtype.Date,
	pageCount int32,
	externalBytes []byte,
	lockedBytes []byte,
) error {
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
	var err error
	v.ExternalIDs, err = decodeJSONMap(externalBytes)
	if err != nil {
		return fmt.Errorf("decoding volume external_ids: %w", err)
	}
	v.LockedFields, err = decodeFieldLocks(lockedBytes)
	if err != nil {
		return fmt.Errorf("decoding volume locked_fields: %w", err)
	}
	return nil
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
	if err := buildDomainVolumeFromScanned(&v, kind, number, chapterStart, chapterEnd, chapterRangeSource, releaseDate, pageCount, externalBytes, lockedBytes); err != nil {
		return nil, err
	}
	return &v, nil
}

func scanVolumePagedRow(row pgx.Row) (int64, bool, *domain.Volume, error) {
	var (
		totalCount         int64
		hasRow             bool
		id                 *string
		libraryID          *string
		kind               *string
		number             pgtype.Float8
		chapterStart       pgtype.Float8
		chapterEnd         pgtype.Float8
		chapterRangeSource *string
		releaseDate        pgtype.Date
		pageCount          *int32
		externalBytes      []byte
		lockedBytes        []byte
		createdAt          *time.Time
		updatedAt          *time.Time
		membershipPos      pgtype.Float8
		v                  domain.Volume
	)
	err := row.Scan(
		&totalCount,
		&hasRow,
		&id,
		&libraryID,
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
		&createdAt,
		&updatedAt,
		&membershipPos,
	)
	if err != nil {
		return 0, false, nil, mapPgError(err)
	}
	if !hasRow {
		return totalCount, false, nil, nil
	}
	if id == nil || libraryID == nil || kind == nil || pageCount == nil || createdAt == nil || updatedAt == nil {
		return 0, false, nil, errors.New("paged volume row missing required volume metadata")
	}
	v.ID = *id
	v.LibraryID = *libraryID
	v.CreatedAt = *createdAt
	v.UpdatedAt = *updatedAt
	if err := buildDomainVolumeFromScanned(&v, *kind, number, chapterStart, chapterEnd, chapterRangeSource, releaseDate, *pageCount, externalBytes, lockedBytes); err != nil {
		return 0, false, nil, err
	}
	return totalCount, true, &v, nil
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
		digestKind     string
		digest         string
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
		&digestKind,
		&digest,
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
	f.DigestKind = domain.DigestKind(digestKind)
	f.Digest = digest
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
		digestKind       string
		digest           string
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
		&digestKind,
		&digest,
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
	if digestKind != "" && digest != "" {
		a.Digests = []domain.AssetDigest{{
			AssetID:   a.ID,
			Kind:      domain.DigestKind(digestKind),
			Value:     digest,
			IsCurrent: true,
		}}
	}

	f.AssetID = a.ID
	f.VolumeID = a.VolumeID
	f.LibraryRootID = a.LibraryRootID
	f.RelPathBytes = a.RelPathBytes
	f.RelPathDisplay = a.RelPathDisplay
	f.FileSize = a.FileSize
	f.Format = a.Format
	f.DigestKind = domain.DigestKind(digestKind)
	f.Digest = digest
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
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected trailing data in JSON map")
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

func scanFolderBrowseRow(row pgx.Row) (int64, bool, *storage.FolderEntry, error) {
	var (
		totalCount     int64
		hasRow         bool
		isFolder       *bool
		childNameBytes []byte
		relPathBytes   []byte
		assetID        *string
		volumeID       *string
		libraryRootID  *string
		fileSize       *int64
		format         *string
		pageCount      *int32
	)
	err := row.Scan(
		&totalCount,
		&hasRow,
		&isFolder,
		&childNameBytes,
		&relPathBytes,
		&assetID,
		&volumeID,
		&libraryRootID,
		&fileSize,
		&format,
		&pageCount,
	)
	if err != nil {
		return 0, false, nil, mapPgError(err)
	}
	if !hasRow {
		return totalCount, false, nil, nil
	}
	if isFolder == nil {
		return 0, false, nil, errors.New("folder entry row missing is_folder")
	}

	entry := &storage.FolderEntry{
		Name:    domain.NewPathIdentity(childNameBytes),
		RelPath: domain.NewPathIdentity(relPathBytes),
	}
	if *isFolder {
		entry.Kind = storage.FolderEntryKindFolder
		return totalCount, true, entry, nil
	}
	if assetID == nil || volumeID == nil || libraryRootID == nil || fileSize == nil || format == nil || pageCount == nil {
		return 0, false, nil, errors.New("folder file entry missing required asset metadata")
	}
	entry.Kind = storage.FolderEntryKindFile
	entry.AssetID = *assetID
	entry.VolumeID = *volumeID
	entry.LibraryRootID = *libraryRootID
	entry.FileSize = *fileSize
	entry.Format = domain.MediaFormat(*format)
	entry.PageCount = int(*pageCount)
	return totalCount, true, entry, nil
}
