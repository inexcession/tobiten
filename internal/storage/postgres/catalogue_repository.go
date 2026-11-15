package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

// CatalogueRepository implements storage.CatalogueRepository backed by PostgreSQL,
// enforcing LabelPredicate visibility on every read and count path (ADR-0006 §5).
type CatalogueRepository struct {
	db DBTX
}

// NewCatalogueRepository creates a new CatalogueRepository using the provided database handle.
func NewCatalogueRepository(db DBTX) *CatalogueRepository {
	return &CatalogueRepository{db: db}
}

func (r *CatalogueRepository) loadPredicate(ctx context.Context, scope storage.CatalogueReadScope) (*LabelPredicate, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("catalogue repository is not initialized")
	}
	pred, err := LoadLabelPredicate(ctx, r.db, scope.Principal, scope.GroupPolicies, LabelPredicateOptions{
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

func getByIDWithPredicate[T any](
	ctx context.Context,
	r *CatalogueRepository,
	scope storage.CatalogueReadScope,
	id string,
	selectCols string,
	alias string,
	fragmentFn func(*LabelPredicate, string, int) (SQLFragment, error),
	scanFn func(pgx.Row) (*T, error),
) (*T, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	frag, err := fragmentFn(pred, alias, 2)
	if err != nil {
		return nil, fmt.Errorf("building %s label predicate: %w", alias, err)
	}
	query := selectCols + fmt.Sprintf(" WHERE %s.id = $1 AND (%s)", alias, frag.SQL)
	args := append([]any{id}, frag.Args...)
	return scanFn(r.db.QueryRow(ctx, query, args...))
}

// GetSeriesByID retrieves a single series if visible under the principal's label predicate.
func (r *CatalogueRepository) GetSeriesByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Series, error) {
	return getByIDWithPredicate(ctx, r, scope, id, selectSeriesColumns, "s", (*LabelPredicate).SeriesFragment, scanSeriesRow)
}

// ListSeries returns series visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) ListSeries(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	whereClause, args, err := buildSeriesFilterWhere(pred, filter)
	if err != nil {
		return nil, err
	}
	query := selectSeriesColumns + whereClause + " ORDER BY s.title_sort ASC, s.id ASC"
	query, args, err = appendPagination(query, args, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	out := []domain.Series{}
	for rows.Next() {
		item, err := scanSeriesRow(rows)
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

// CountSeries returns the count of series visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) CountSeries(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	whereClause, args, err := buildSeriesFilterWhere(pred, filter)
	if err != nil {
		return 0, err
	}
	return r.queryCountWithSQL(ctx, "SELECT COUNT(*) FROM series s"+whereClause, args...)
}

// GetVolumeByID retrieves a single volume if visible under the principal's label predicate.
func (r *CatalogueRepository) GetVolumeByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return r.getVolumeWithPredicate(ctx, pred, id)
}

func (r *CatalogueRepository) getVolumeWithPredicate(ctx context.Context, pred *LabelPredicate, id string) (*domain.Volume, error) {
	frag, err := pred.VolumeFragment("v", 2)
	if err != nil {
		return nil, fmt.Errorf("building volume label predicate: %w", err)
	}
	query := selectVolumeColumns + " WHERE v.id = $1 AND (" + frag.SQL + ")"
	args := append([]any{id}, frag.Args...)
	return scanVolumeRow(r.db.QueryRow(ctx, query, args...))
}

// ListVolumes returns volumes visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) ListVolumes(ctx context.Context, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter) ([]domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	joinClause, whereClause, args, err := buildVolumeFilterQuery(pred, filter)
	if err != nil {
		return nil, err
	}
	orderBy := " ORDER BY v.created_at ASC, v.id ASC"
	if filter.SeriesID != "" {
		orderBy = " ORDER BY sm_f.position ASC NULLS LAST, v.number ASC NULLS LAST, v.id ASC"
	}
	query := selectVolumeColumns + joinClause + whereClause + orderBy
	query, args, err = appendPagination(query, args, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	out := []domain.Volume{}
	for rows.Next() {
		item, err := scanVolumeRow(rows)
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

// CountVolumes returns the count of volumes visible under the principal's label predicate matching filter.
func (r *CatalogueRepository) CountVolumes(ctx context.Context, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	joinClause, whereClause, args, err := buildVolumeFilterQuery(pred, filter)
	if err != nil {
		return 0, err
	}
	return r.queryCountWithSQL(ctx, "SELECT COUNT(*) FROM volumes v"+joinClause+whereClause, args...)
}

// GetAssetByID retrieves a single media asset if visible under the principal's label predicate.
func (r *CatalogueRepository) GetAssetByID(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.MediaAsset, error) {
	return getByIDWithPredicate(ctx, r, scope, id, selectAssetColumns, "a", (*LabelPredicate).AssetFragment, scanAssetRow)
}

// ListAssetsByVolume returns all visible media asset manifestations for volumeID.
func (r *CatalogueRepository) ListAssetsByVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) ([]domain.MediaAsset, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	frag, err := pred.AssetFragment("a", 2)
	if err != nil {
		return nil, fmt.Errorf("building asset label predicate: %w", err)
	}
	query := selectAssetColumns + " WHERE a.volume_id = $1 AND (" + frag.SQL + ") ORDER BY a.created_at ASC, a.id ASC"
	args := append([]any{volumeID}, frag.Args...)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	out := []domain.MediaAsset{}
	for rows.Next() {
		item, err := scanAssetRow(rows)
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

// CountAssetsByVolume returns the count of visible media asset manifestations for volumeID.
func (r *CatalogueRepository) CountAssetsByVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	frag, err := pred.AssetFragment("a", 2)
	if err != nil {
		return 0, fmt.Errorf("building asset label predicate: %w", err)
	}
	query := "SELECT COUNT(*) FROM media_assets a WHERE a.volume_id = $1 AND (" + frag.SQL + ")"
	args := append([]any{volumeID}, frag.Args...)
	return r.queryCountWithSQL(ctx, query, args...)
}

// GetAuthorizedAssetFile retrieves the root and asset path projection for assetID
// only when the asset, its parent volume, and its series satisfy the label predicate.
func (r *CatalogueRepository) GetAuthorizedAssetFile(ctx context.Context, scope storage.CatalogueReadScope, assetID string) (*storage.AuthorizedAssetFile, error) {
	return getByIDWithPredicate(ctx, r, scope, assetID, selectAuthorizedAssetFileColumns, "a", (*LabelPredicate).AssetFragment, scanAuthorizedAssetFileRow)
}

// SelectPreferredAssetFileForVolume resolves the preferred readable asset file projection for volumeID
// per ADR-0018 S5 (explicit preference -> user's progress locator -> oldest eligible available asset),
// strictly constrained by the principal's label predicate.
func (r *CatalogueRepository) SelectPreferredAssetFileForVolume(ctx context.Context, scope storage.CatalogueReadScope, volumeID string) (*storage.AuthorizedAssetFile, error) {
	pred, err := r.loadPredicate(ctx, scope)
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
	frag, err := pred.AssetFragment("a", 2)
	if err != nil {
		return nil, nil, fmt.Errorf("building asset label predicate: %w", err)
	}
	query := `
		SELECT a.id, a.volume_id, r.library_id, a.library_root_id,
		       r.path_bytes, r.path_display, r.state,
		       a.rel_path_bytes, a.rel_path_display, a.rel_path_match,
		       a.file_size, a.file_mtime, a.format, a.page_count,
		       a.cover_page_index, a.suppressed_pages, a.diagnostics,
		       a.missing_since, a.retired_at, a.created_at, a.updated_at
		FROM media_assets a
		JOIN library_roots r ON r.id = a.library_root_id
		WHERE a.volume_id = $1 AND (` + frag.SQL + `)
		ORDER BY a.created_at ASC, a.id ASC`
	args := append([]any{volumeID}, frag.Args...)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, mapPgError(err)
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
	frag, err := pred.ReadingProgressFragment("rp", 3)
	if err != nil {
		return nil, fmt.Errorf("building reading_progress label predicate: %w", err)
	}
	query := selectReadingProgressColumns + " WHERE rp.user_id = $1 AND rp.volume_id = $2 AND (" + frag.SQL + ")"
	args := append([]any{userID, volumeID}, frag.Args...)
	return scanReadingProgressRow(r.db.QueryRow(ctx, query, args...))
}

// ListReadingProgress returns the requesting user's reading progress entries
// for volumes visible under the principal's label predicate.
func (r *CatalogueRepository) ListReadingProgress(ctx context.Context, scope storage.CatalogueReadScope, filter storage.ReadingProgressReadFilter) ([]domain.ReadingProgress, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	whereClause, args, err := buildProgressFilterWhere(pred, principalUserID(scope), filter)
	if err != nil {
		return nil, err
	}
	query := selectReadingProgressColumns + whereClause + " ORDER BY rp.updated_at DESC, rp.volume_id ASC"
	query, args, err = appendPagination(query, args, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	out := []domain.ReadingProgress{}
	for rows.Next() {
		item, err := scanReadingProgressRow(rows)
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

// CountReadingProgress returns the count of the requesting user's reading progress entries
// for volumes visible under the principal's label predicate.
func (r *CatalogueRepository) CountReadingProgress(ctx context.Context, scope storage.CatalogueReadScope, filter storage.ReadingProgressReadFilter) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	whereClause, args, err := buildProgressFilterWhere(pred, principalUserID(scope), filter)
	if err != nil {
		return 0, err
	}
	return r.queryCountWithSQL(ctx, "SELECT COUNT(*) FROM reading_progress rp"+whereClause, args...)
}

func (r *CatalogueRepository) queryCountWithSQL(ctx context.Context, query string, args ...any) (int64, error) {
	var count int64
	if err := r.db.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return 0, mapPgError(err)
	}
	return count, nil
}

func principalUserID(scope storage.CatalogueReadScope) string {
	if scope.Principal == nil {
		return ""
	}
	return strings.TrimSpace(scope.Principal.User.ID)
}

func escapeILIKE(term string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	)
	return "%" + replacer.Replace(term) + "%"
}

func appendPagination(query string, args []any, limit, offset int) (string, []any, error) {
	if limit < 0 || offset < 0 {
		return "", nil, fmt.Errorf("invalid pagination limit=%d offset=%d", limit, offset)
	}
	if limit > 0 {
		args = append(args, limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if offset > 0 {
		args = append(args, offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	return query, args, nil
}

func buildSeriesFilterWhere(pred *LabelPredicate, filter storage.SeriesReadFilter) (string, []any, error) {
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

	frag, err := pred.SeriesFragment("s", len(args)+1)
	if err != nil {
		return "", nil, fmt.Errorf("building series label predicate: %w", err)
	}
	whereParts = append(whereParts, "("+frag.SQL+")")
	args = append(args, frag.Args...)

	return " WHERE " + strings.Join(whereParts, " AND "), args, nil
}

func buildVolumeFilterQuery(pred *LabelPredicate, filter storage.VolumeReadFilter) (string, string, []any, error) {
	var joinClause string
	var whereParts []string
	var args []any

	if filter.SeriesID != "" {
		joinClause = " JOIN series_memberships sm_f ON sm_f.volume_id = v.id JOIN series s_f ON s_f.id = sm_f.series_id"
		args = append(args, filter.SeriesID)
		whereParts = append(whereParts, fmt.Sprintf("sm_f.series_id = $%d", len(args)))

		// Guard series membership filter itself so hidden series cannot be enumerated via cross-series volumes.
		sFrag, err := pred.SeriesFragment("s_f", len(args)+1)
		if err != nil {
			return "", "", nil, fmt.Errorf("building series filter label predicate: %w", err)
		}
		whereParts = append(whereParts, "("+sFrag.SQL+")")
		args = append(args, sFrag.Args...)
	}
	if filter.LibraryID != "" {
		args = append(args, filter.LibraryID)
		whereParts = append(whereParts, fmt.Sprintf("v.library_id = $%d", len(args)))
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		args = append(args, escapeILIKE(search))
		whereParts = append(whereParts, fmt.Sprintf("COALESCE(v.title, '') ILIKE $%d ESCAPE '\\'", len(args)))
	}

	vFrag, err := pred.VolumeFragment("v", len(args)+1)
	if err != nil {
		return "", "", nil, fmt.Errorf("building volume label predicate: %w", err)
	}
	whereParts = append(whereParts, "("+vFrag.SQL+")")
	args = append(args, vFrag.Args...)

	return joinClause, " WHERE " + strings.Join(whereParts, " AND "), args, nil
}

func buildProgressFilterWhere(pred *LabelPredicate, userID string, filter storage.ReadingProgressReadFilter) (string, []any, error) {
	args := []any{userID}
	whereParts := []string{"rp.user_id = $1"}

	if filter.Completed != nil {
		args = append(args, *filter.Completed)
		whereParts = append(whereParts, fmt.Sprintf("rp.completed = $%d", len(args)))
	}

	frag, err := pred.ReadingProgressFragment("rp", len(args)+1)
	if err != nil {
		return "", nil, fmt.Errorf("building reading_progress label predicate: %w", err)
	}
	whereParts = append(whereParts, "("+frag.SQL+")")
	args = append(args, frag.Args...)

	return " WHERE " + strings.Join(whereParts, " AND "), args, nil
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
