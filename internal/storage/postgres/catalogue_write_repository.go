package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

var _ storage.CatalogueWriteRepository = (*CatalogueWriteRepository)(nil)

// CatalogueWriteRepository implements storage.CatalogueWriteRepository backed by PostgreSQL and sqlc.
// It is kept separate from CatalogueRepository so system-level writes and unfiltered root scans
// remain distinct from label-enforced principal reads.
type CatalogueWriteRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
}

// NewCatalogueWriteRepository creates a new CatalogueWriteRepository using the provided database handle.
func NewCatalogueWriteRepository(db DBTX) *CatalogueWriteRepository {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	var scope *txScope
	if extTx, ok := db.(pgx.Tx); ok {
		scope = newCallerTxScope(extTx)
	}
	return &CatalogueWriteRepository{
		q:     New(db),
		tx:    beginner,
		scope: scope,
	}
}

func (r *CatalogueWriteRepository) runWriteTx(ctx context.Context, label string, fn func(q *Queries) error) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	return runTx(ctx, r.tx, r.scope, label, func(child *txScope) error {
		return fn(New(child.tx))
	})
}

// CreateSeries persists a new series record.
func (r *CatalogueWriteRepository) CreateSeries(ctx context.Context, series *domain.Series) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	params, err := buildCreateSeriesParams(series)
	if err != nil {
		return err
	}

	row, err := r.q.CreateSeries(ctx, params)
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	out, err := seriesModelToDomain(row)
	if err != nil {
		return err
	}
	*series = out
	return nil
}

// GetSeriesByID retrieves a series by ID for system operations.
func (r *CatalogueWriteRepository) GetSeriesByID(ctx context.Context, id string) (*domain.Series, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	return r.fetchSeriesByID(ctx, id, r.q.GetSeriesByIDForWrite)
}

// GetSeriesByIDForUpdate retrieves and locks a series row FOR UPDATE within an
// active transaction.
func (r *CatalogueWriteRepository) GetSeriesByIDForUpdate(ctx context.Context, id string) (*domain.Series, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	return r.fetchSeriesByID(ctx, id, r.q.GetSeriesByIDForWriteForUpdate)
}

func (r *CatalogueWriteRepository) fetchSeriesByID(
	ctx context.Context,
	id string,
	query func(context.Context, string) (Series, error),
) (*domain.Series, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptySeriesID
	}
	row, err := query(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := seriesModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// GetSeriesByResolverKey retrieves a series by library ID and canonical resolver key.
func (r *CatalogueWriteRepository) GetSeriesByResolverKey(ctx context.Context, libraryID, resolverKey string) (*domain.Series, error) {
	if err := validateSeriesResolverKeyArgs(r, libraryID, resolverKey); err != nil {
		return nil, err
	}
	row, err := r.q.GetSeriesByResolverKey(ctx, GetSeriesByResolverKeyParams{
		LibraryID:       libraryID,
		ResolverKeyHash: hashStringSHA256(resolverKey),
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	if row.ResolverKey == nil || *row.ResolverKey != resolverKey {
		return nil, storage.ErrResolverKeyHashCollision
	}
	out, err := seriesModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func validateSeriesResolverKeyArgs(r *CatalogueWriteRepository, libraryID, resolverKey string) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	if strings.TrimSpace(libraryID) == "" {
		return domain.ErrEmptyLibraryID
	}
	if strings.TrimSpace(resolverKey) == "" {
		return domain.ErrEmptyResolverKey
	}
	return nil
}

// AcquireSeriesVariantLock acquires a transaction-scoped advisory lock on
// (seriesID, kind, normalized number) so concurrent indexers serialize zero-match
// volume lookup through volume and membership creation without taking a series row
// lock before volume row locks.
func (r *CatalogueWriteRepository) AcquireSeriesVariantLock(
	ctx context.Context,
	seriesID string,
	kind domain.VolumeKind,
	number float64,
) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	if r.scope == nil {
		return storage.ErrTransactionRequired
	}
	if strings.TrimSpace(seriesID) == "" {
		return domain.ErrEmptySeriesID
	}
	if !kind.IsValid() {
		return domain.ErrInvalidVolumeKind
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return errors.New("volume number must be finite")
	}
	if number == 0 {
		number = 0
	}
	variantKey := fmt.Sprintf("%s:%s:%016x", seriesID, kind, math.Float64bits(number))
	return mapPgError(r.q.AcquireSeriesVariantLock(ctx, variantKey))
}

// FindOrCreateSeriesByResolverKey resolves or atomically creates a series scoped
// by (series.LibraryID, *series.ResolverKey), returning created=true when newly
// inserted or created=false with the existing row when already present.
func (r *CatalogueWriteRepository) FindOrCreateSeriesByResolverKey(ctx context.Context, series *domain.Series) (bool, error) {
	if r == nil {
		return false, errors.New("catalogue write repository is required")
	}
	if series == nil {
		return false, errors.New("series cannot be nil")
	}
	if strings.TrimSpace(series.LibraryID) == "" {
		return false, domain.ErrEmptyLibraryID
	}
	if series.ResolverKey == nil || strings.TrimSpace(*series.ResolverKey) == "" {
		return false, domain.ErrEmptyResolverKey
	}

	var (
		out     domain.Series
		created bool
	)
	err := r.runWriteTx(ctx, "find or create series by resolver key", func(q *Queries) error {
		var txErr error
		out, created, txErr = findOrCreateSeriesByResolverKeyInTx(ctx, q, series)
		return txErr
	})
	if err != nil {
		return false, err
	}
	*series = out
	return created, nil
}

func findOrCreateSeriesByResolverKeyInTx(
	ctx context.Context,
	q *Queries,
	series *domain.Series,
) (domain.Series, bool, error) {
	resolverKey := *series.ResolverKey
	keyHash := hashStringSHA256(resolverKey)

	lookupExact := func() (domain.Series, error) {
		row, err := q.GetSeriesByResolverKey(ctx, GetSeriesByResolverKeyParams{
			LibraryID:       series.LibraryID,
			ResolverKeyHash: keyHash,
		})
		if err != nil {
			return domain.Series{}, err
		}
		if row.ResolverKey == nil || *row.ResolverKey != resolverKey {
			return domain.Series{}, storage.ErrResolverKeyHashCollision
		}
		return seriesModelToDomain(row)
	}

	if existing, err := lookupExact(); err == nil {
		return existing, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Series{}, false, mapPgError(err)
	}

	for range maxFindOrCreateAttempts {
		attempt := *series
		params, prepErr := buildCreateSeriesParams(&attempt)
		if prepErr != nil {
			return domain.Series{}, false, prepErr
		}
		inserted, insErr := q.FindOrInsertSeriesByResolverKey(ctx, FindOrInsertSeriesByResolverKeyParams(params))
		if insErr == nil {
			out, convErr := seriesModelToDomain(inserted)
			return out, true, convErr
		}
		if !errors.Is(insErr, pgx.ErrNoRows) {
			return domain.Series{}, false, mapCatalogueWriteError(insErr)
		}

		existing, selErr := lookupExact()
		if selErr == nil {
			return existing, false, nil
		}
		if !errors.Is(selErr, pgx.ErrNoRows) {
			return domain.Series{}, false, mapPgError(selErr)
		}

		if isoErr := ensureReadCommittedIsolation(ctx, q); isoErr != nil {
			return domain.Series{}, false, isoErr
		}
	}
	return domain.Series{}, false, storage.ErrNotFound
}

func hashStringSHA256(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

func seriesResolverKeyHash(key *string) []byte {
	if key == nil {
		return nil
	}
	return hashStringSHA256(*key)
}

func ensureReadCommittedIsolation(ctx context.Context, q *Queries) error {
	iso, isoErr := q.GetTransactionIsolation(ctx)
	if isoErr != nil {
		return mapPgError(isoErr)
	}
	if !strings.EqualFold(strings.TrimSpace(iso), "read committed") {
		return fmt.Errorf("%w: got %q", storage.ErrTransactionIsolation, iso)
	}
	return nil
}

func buildCreateSeriesParams(series *domain.Series) (CreateSeriesParams, error) {
	candidate, lockedBytes, externalBytes, totalVols, releaseYear, err := prepareSeriesWrite(series, true)
	if err != nil {
		return CreateSeriesParams{}, err
	}
	return CreateSeriesParams{
		ID:               candidate.ID,
		LibraryID:        candidate.LibraryID,
		ResolverKey:      cloneStringPtr(candidate.ResolverKey),
		ResolverKeyHash:  seriesResolverKeyHash(candidate.ResolverKey),
		Title:            candidate.Title,
		TitleSort:        candidate.TitleSort,
		Language:         cloneStringPtr(candidate.Language),
		OriginalTitle:    cloneStringPtr(candidate.OriginalTitle),
		RomajiTitle:      cloneStringPtr(candidate.RomajiTitle),
		Franchise:        cloneStringPtr(candidate.Franchise),
		EditionName:      cloneStringPtr(candidate.EditionName),
		Publisher:        cloneStringPtr(candidate.Publisher),
		Imprint:          cloneStringPtr(candidate.Imprint),
		Status:           string(candidate.Status),
		Summary:          cloneStringPtr(candidate.Summary),
		AgeRating:        cloneStringPtr(candidate.AgeRating),
		Genres:           candidate.Genres,
		Tags:             candidate.Tags,
		CoverPolicy:      coverPolicyPtrToStringPtr(candidate.CoverPolicy),
		CoverVolumeID:    cloneStringPtr(candidate.CoverVolumeID),
		CoverImageDigest: cloneStringPtr(candidate.CoverImageDigest),
		LockedFields:     lockedBytes,
		ExternalIds:      externalBytes,
		TotalVolumes:     totalVols,
		ReleaseYear:      releaseYear,
		CreatedAt:        candidate.CreatedAt,
		UpdatedAt:        candidate.UpdatedAt,
	}, nil
}

// UpdateSeries persists changes to an existing series record.
func (r *CatalogueWriteRepository) UpdateSeries(ctx context.Context, series *domain.Series) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	candidate, lockedBytes, externalBytes, totalVols, releaseYear, err := prepareSeriesWrite(series, false)
	if err != nil {
		return err
	}

	row, err := r.q.UpdateSeries(ctx, UpdateSeriesParams{
		ID:               candidate.ID,
		LibraryID:        candidate.LibraryID,
		ResolverKey:      cloneStringPtr(candidate.ResolverKey),
		ResolverKeyHash:  seriesResolverKeyHash(candidate.ResolverKey),
		Title:            candidate.Title,
		TitleSort:        candidate.TitleSort,
		Language:         cloneStringPtr(candidate.Language),
		OriginalTitle:    cloneStringPtr(candidate.OriginalTitle),
		RomajiTitle:      cloneStringPtr(candidate.RomajiTitle),
		Franchise:        cloneStringPtr(candidate.Franchise),
		EditionName:      cloneStringPtr(candidate.EditionName),
		Publisher:        cloneStringPtr(candidate.Publisher),
		Imprint:          cloneStringPtr(candidate.Imprint),
		Status:           string(candidate.Status),
		Summary:          cloneStringPtr(candidate.Summary),
		AgeRating:        cloneStringPtr(candidate.AgeRating),
		Genres:           candidate.Genres,
		Tags:             candidate.Tags,
		CoverPolicy:      coverPolicyPtrToStringPtr(candidate.CoverPolicy),
		CoverVolumeID:    cloneStringPtr(candidate.CoverVolumeID),
		CoverImageDigest: cloneStringPtr(candidate.CoverImageDigest),
		LockedFields:     lockedBytes,
		ExternalIds:      externalBytes,
		TotalVolumes:     totalVols,
		ReleaseYear:      releaseYear,
		UpdatedAt:        candidate.UpdatedAt,
	})
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	out, err := seriesModelToDomain(row)
	if err != nil {
		return err
	}
	*series = out
	return nil
}

// CreateVolume persists a new volume record. Memberships and assets are persisted separately.
func (r *CatalogueWriteRepository) CreateVolume(ctx context.Context, volume *domain.Volume) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	candidate, externalBytes, lockedBytes, pageCount, err := prepareVolumeWrite(volume, true)
	if err != nil {
		return err
	}

	row, err := r.q.CreateVolume(ctx, CreateVolumeParams{
		ID:                 candidate.ID,
		LibraryID:          candidate.LibraryID,
		Kind:               string(candidate.Kind),
		Number:             optFloat64ToFloat8(candidate.Number),
		ChapterStart:       optFloat64ToFloat8(candidate.ChapterStart),
		ChapterEnd:         optFloat64ToFloat8(candidate.ChapterEnd),
		ChapterRangeSource: chapterRangeSourcePtrToStringPtr(candidate.ChapterRangeSource),
		Title:              cloneStringPtr(candidate.Title),
		Summary:            cloneStringPtr(candidate.Summary),
		ReleaseDate:        optTimeToPgDate(candidate.ReleaseDate),
		PageCount:          pageCount,
		PreferredAssetID:   cloneStringPtr(candidate.PreferredAssetID),
		ExternalIds:        externalBytes,
		LockedFields:       lockedBytes,
		CreatedAt:          candidate.CreatedAt,
		UpdatedAt:          candidate.UpdatedAt,
	})
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	out, err := volumeModelToDomain(row)
	if err != nil {
		return err
	}
	*volume = out
	return nil
}

// GetVolumeByID retrieves a volume by ID for system operations.
func (r *CatalogueWriteRepository) GetVolumeByID(ctx context.Context, id string) (*domain.Volume, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	return r.fetchVolumeByID(ctx, id, r.q.GetVolumeByIDForWrite)
}

// GetVolumeByIDForUpdate retrieves and locks a volume row FOR UPDATE within an
// active transaction so concurrent curator edits and field locks are observed
// before merging or updating.
func (r *CatalogueWriteRepository) GetVolumeByIDForUpdate(ctx context.Context, id string) (*domain.Volume, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	return r.fetchVolumeByID(ctx, id, r.q.GetVolumeByIDForWriteForUpdate)
}

func (r *CatalogueWriteRepository) fetchVolumeByID(
	ctx context.Context,
	id string,
	query func(context.Context, string) (Volume, error),
) (*domain.Volume, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyVolumeID
	}
	row, err := query(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := volumeModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// FindVolumesByPrimarySeriesKindNumber returns volumes in libraryID whose primary
// membership targets seriesID and whose kind and number match, locking them FOR UPDATE
// and rechecking the primary membership and tuple under the acquired lock.
func (r *CatalogueWriteRepository) FindVolumesByPrimarySeriesKindNumber(
	ctx context.Context,
	libraryID, seriesID string,
	kind domain.VolumeKind,
	number float64,
) ([]domain.Volume, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	if strings.TrimSpace(libraryID) == "" {
		return nil, domain.ErrEmptyLibraryID
	}
	if strings.TrimSpace(seriesID) == "" {
		return nil, domain.ErrEmptySeriesID
	}
	if !kind.IsValid() {
		return nil, domain.ErrInvalidVolumeKind
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil, errors.New("volume number must be finite")
	}
	if number == 0 {
		number = 0
	}

	numParam := pgtype.Float8{Float64: number, Valid: true}
	candidateIDs, err := r.q.FindVolumesByPrimarySeriesKindNumberForUpdate(ctx, FindVolumesByPrimarySeriesKindNumberForUpdateParams{
		SeriesID:  seriesID,
		LibraryID: libraryID,
		Kind:      string(kind),
		Number:    numParam,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	if len(candidateIDs) == 0 {
		return nil, nil
	}

	rows, err := r.q.GetVerifiedVolumesByPrimarySeriesKindNumberForUpdate(ctx, GetVerifiedVolumesByPrimarySeriesKindNumberForUpdateParams{
		SeriesID:  seriesID,
		LibraryID: libraryID,
		Kind:      string(kind),
		Number:    numParam,
		VolumeIds: candidateIDs,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	out := make([]domain.Volume, len(rows))
	for i, row := range rows {
		vol, convErr := volumeModelToDomain(row)
		if convErr != nil {
			return nil, convErr
		}
		out[i] = vol
	}
	return out, nil
}

// UpdateVolume persists changes to an existing volume record.
func (r *CatalogueWriteRepository) UpdateVolume(ctx context.Context, volume *domain.Volume) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	candidate, externalBytes, lockedBytes, pageCount, err := prepareVolumeWrite(volume, false)
	if err != nil {
		return err
	}

	row, err := r.q.UpdateVolume(ctx, UpdateVolumeParams{
		ID:                 candidate.ID,
		LibraryID:          candidate.LibraryID,
		Kind:               string(candidate.Kind),
		Number:             optFloat64ToFloat8(candidate.Number),
		ChapterStart:       optFloat64ToFloat8(candidate.ChapterStart),
		ChapterEnd:         optFloat64ToFloat8(candidate.ChapterEnd),
		ChapterRangeSource: chapterRangeSourcePtrToStringPtr(candidate.ChapterRangeSource),
		Title:              cloneStringPtr(candidate.Title),
		Summary:            cloneStringPtr(candidate.Summary),
		ReleaseDate:        optTimeToPgDate(candidate.ReleaseDate),
		PageCount:          pageCount,
		PreferredAssetID:   cloneStringPtr(candidate.PreferredAssetID),
		ExternalIds:        externalBytes,
		LockedFields:       lockedBytes,
		UpdatedAt:          candidate.UpdatedAt,
	})
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	out, err := volumeModelToDomain(row)
	if err != nil {
		return err
	}
	out.Memberships = slices.Clone(volume.Memberships)
	out.Assets = slices.Clone(volume.Assets)
	*volume = out
	return nil
}

// CreateAsset persists a new media asset and any initial digests supplied in asset.Digests.
func (r *CatalogueWriteRepository) CreateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	prepared, err := prepareAssetWrite(asset, true)
	if err != nil {
		return err
	}
	digests, err := prepareInitialAssetDigests(prepared.candidate.ID, prepared.candidate.CreatedAt, prepared.candidate.Digests)
	if err != nil {
		return err
	}

	insert := func(q *Queries) error {
		row, err := q.CreateMediaAsset(ctx, CreateMediaAssetParams{
			ID:              prepared.candidate.ID,
			VolumeID:        prepared.candidate.VolumeID,
			LibraryRootID:   prepared.candidate.LibraryRootID,
			RelPathBytes:    slices.Clone(prepared.candidate.RelPathBytes),
			RelPathDisplay:  prepared.candidate.RelPathDisplay,
			RelPathMatch:    prepared.candidate.RelPathMatch,
			FileSize:        prepared.candidate.FileSize,
			FileMtime:       normalizeOptionalTimeUTC(prepared.candidate.FileMtime),
			Format:          string(prepared.candidate.Format),
			PageCount:       prepared.pageCount,
			CoverPageIndex:  prepared.coverPageIndex,
			SuppressedPages: prepared.suppressedPages,
			Diagnostics:     prepared.diagnosticsBytes,
			MissingSince:    normalizeOptionalTimeUTC(prepared.candidate.MissingSince),
			RetiredAt:       normalizeOptionalTimeUTC(prepared.candidate.RetiredAt),
			CreatedAt:       prepared.candidate.CreatedAt,
			UpdatedAt:       prepared.candidate.UpdatedAt,
		})
		if err != nil {
			return mapCatalogueWriteError(err)
		}

		insertedDigests := make([]domain.AssetDigest, 0, len(digests))
		for _, d := range digests {
			dRow, err := q.InsertAssetDigest(ctx, InsertAssetDigestParams{
				AssetID:    d.AssetID,
				Kind:       string(d.Kind),
				Value:      d.Value,
				IsCurrent:  d.IsCurrent,
				Reason:     string(d.Reason),
				RecordedAt: d.RecordedAt,
			})
			if err != nil {
				return mapCatalogueWriteError(err)
			}
			insertedDigests = append(insertedDigests, assetDigestModelToDomain(dRow))
		}

		out, err := mediaAssetModelToDomain(row)
		if err != nil {
			return err
		}
		if len(insertedDigests) > 0 {
			out.Digests = insertedDigests
		}
		*asset = out
		return nil
	}

	if len(digests) == 0 {
		return insert(r.q)
	}
	return r.runWriteTx(ctx, "create asset", insert)
}

// GetAssetByID retrieves a media asset by ID along with its current and historical digests.
func (r *CatalogueWriteRepository) GetAssetByID(ctx context.Context, id string) (*domain.MediaAsset, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyAssetID
	}
	rows, err := r.q.GetMediaAssetWithDigestsByIDForWrite(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	if len(rows) == 0 {
		return nil, storage.ErrNotFound
	}
	assets, err := mapJoinedAssetRows(rows, func(row GetMediaAssetWithDigestsByIDForWriteRow) joinedAssetDigestRow {
		return joinedAssetDigestRow{
			asset:            row.MediaAsset,
			digestKind:       row.DigestKind,
			digestValue:      row.DigestValue,
			digestIsCurrent:  row.DigestIsCurrent,
			digestReason:     row.DigestReason,
			digestRecordedAt: row.DigestRecordedAt,
		}
	})
	if err != nil {
		return nil, err
	}
	return &assets[0], nil
}

// UpdateAsset persists changes to an existing media asset record.
func (r *CatalogueWriteRepository) UpdateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	prepared, err := prepareAssetWrite(asset, false)
	if err != nil {
		return err
	}

	row, err := r.q.UpdateMediaAsset(ctx, UpdateMediaAssetParams{
		ID:              prepared.candidate.ID,
		VolumeID:        prepared.candidate.VolumeID,
		LibraryRootID:   prepared.candidate.LibraryRootID,
		RelPathBytes:    slices.Clone(prepared.candidate.RelPathBytes),
		RelPathDisplay:  prepared.candidate.RelPathDisplay,
		RelPathMatch:    prepared.candidate.RelPathMatch,
		FileSize:        prepared.candidate.FileSize,
		FileMtime:       normalizeOptionalTimeUTC(prepared.candidate.FileMtime),
		Format:          string(prepared.candidate.Format),
		PageCount:       prepared.pageCount,
		CoverPageIndex:  prepared.coverPageIndex,
		SuppressedPages: prepared.suppressedPages,
		Diagnostics:     prepared.diagnosticsBytes,
		MissingSince:    normalizeOptionalTimeUTC(prepared.candidate.MissingSince),
		RetiredAt:       normalizeOptionalTimeUTC(prepared.candidate.RetiredAt),
		UpdatedAt:       prepared.candidate.UpdatedAt,
	})
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	out, err := mediaAssetModelToDomain(row)
	if err != nil {
		return err
	}
	out.Digests = slices.Clone(asset.Digests)
	*asset = out
	return nil
}

// UpdateAssetLocation updates an asset's library root and relative path while preserving its ID,
// volume assignment, digests, and reading progress.
func (r *CatalogueWriteRepository) UpdateAssetLocation(
	ctx context.Context,
	assetID string,
	libraryRootID string,
	relPath domain.PathIdentity,
	updatedAt time.Time,
) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	if strings.TrimSpace(assetID) == "" {
		return domain.ErrEmptyAssetID
	}
	if strings.TrimSpace(libraryRootID) == "" {
		return domain.ErrEmptyLibraryRootID
	}
	resolved, err := resolveAssetPathIdentity(relPath)
	if err != nil {
		return err
	}
	ts := normalizeTimestampOrNow(updatedAt)

	_, err = r.q.UpdateMediaAssetLocation(ctx, UpdateMediaAssetLocationParams{
		ID:              assetID,
		LibraryRootID:   libraryRootID,
		RelPathBytes:    slices.Clone(resolved.Bytes),
		RelPathDisplay:  resolved.Display,
		RelPathMatch:    resolved.Match,
		UpdatedAt:       ts,
		InvalidUtf8Path: resolved.InvalidUTF8,
	})
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	return nil
}

// ListAssetsByRoot returns all media assets belonging to libraryRootID along with their current digests.
func (r *CatalogueWriteRepository) ListAssetsByRoot(ctx context.Context, libraryRootID string) ([]domain.MediaAsset, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	if strings.TrimSpace(libraryRootID) == "" {
		return nil, domain.ErrEmptyLibraryRootID
	}
	rows, err := r.q.ListMediaAssetsWithCurrentDigestsByRoot(ctx, libraryRootID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return mapJoinedAssetRows(rows, func(row ListMediaAssetsWithCurrentDigestsByRootRow) joinedAssetDigestRow {
		return joinedAssetDigestRow{
			asset:            row.MediaAsset,
			digestKind:       row.DigestKind,
			digestValue:      row.DigestValue,
			digestIsCurrent:  row.DigestIsCurrent,
			digestReason:     row.DigestReason,
			digestRecordedAt: row.DigestRecordedAt,
		}
	})
}

// RecordAssetDigest records a new current digest for (digest.AssetID, digest.Kind),
// demoting any prior current digest of that kind to is_current = FALSE.
func (r *CatalogueWriteRepository) RecordAssetDigest(ctx context.Context, digest *domain.AssetDigest) error {
	if r == nil {
		return errors.New("catalogue write repository is required")
	}
	if digest == nil {
		return errors.New("asset digest cannot be nil")
	}
	candidate := *digest
	if err := candidate.Validate(); err != nil {
		return err
	}
	candidate.RecordedAt = normalizeTimestampOrNow(candidate.RecordedAt)
	candidate.IsCurrent = true

	return r.runWriteTx(ctx, "record asset digest", func(q *Queries) error {
		if _, err := q.LockMediaAssetForUpdate(ctx, candidate.AssetID); err != nil {
			return mapPgError(err)
		}
		if err := q.DemoteCurrentAssetDigests(ctx, DemoteCurrentAssetDigestsParams{
			AssetID: candidate.AssetID,
			Kind:    string(candidate.Kind),
		}); err != nil {
			return mapPgError(err)
		}
		row, err := q.UpsertCurrentAssetDigest(ctx, UpsertCurrentAssetDigestParams{
			AssetID:    candidate.AssetID,
			Kind:       string(candidate.Kind),
			Value:      candidate.Value,
			Reason:     string(candidate.Reason),
			RecordedAt: candidate.RecordedAt,
		})
		if err != nil {
			return mapCatalogueWriteError(err)
		}
		*digest = assetDigestModelToDomain(row)
		return nil
	})
}

// FindAssetsByDigest looks up media assets within libraryID matching (kind, value)
// across both current and historical asset_digests rows.
func (r *CatalogueWriteRepository) FindAssetsByDigest(
	ctx context.Context,
	libraryID string,
	kind domain.DigestKind,
	value string,
) ([]domain.MediaAsset, error) {
	if r == nil {
		return nil, errors.New("catalogue write repository is required")
	}
	if strings.TrimSpace(libraryID) == "" {
		return nil, domain.ErrEmptyLibraryID
	}
	if strings.TrimSpace(string(kind)) == "" {
		return nil, domain.ErrInvalidDigestKind
	}
	if strings.TrimSpace(value) == "" {
		return nil, domain.ErrEmptyDigestValue
	}

	rows, err := r.q.FindMediaAssetsWithDigestsByDigestInLibrary(ctx, FindMediaAssetsWithDigestsByDigestInLibraryParams{
		LibraryID: libraryID,
		Kind:      string(kind),
		Value:     value,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	return mapJoinedAssetRows(rows, func(row FindMediaAssetsWithDigestsByDigestInLibraryRow) joinedAssetDigestRow {
		return joinedAssetDigestRow{
			asset:            row.MediaAsset,
			digestKind:       row.DigestKind,
			digestValue:      row.DigestValue,
			digestIsCurrent:  row.DigestIsCurrent,
			digestReason:     row.DigestReason,
			digestRecordedAt: row.DigestRecordedAt,
		}
	})
}

type joinedAssetDigestRow struct {
	asset            MediaAsset
	digestKind       *string
	digestValue      *string
	digestIsCurrent  pgtype.Bool
	digestReason     *string
	digestRecordedAt *time.Time
}

func mapJoinedAssetRows[R any](rows []R, convert func(R) joinedAssetDigestRow) ([]domain.MediaAsset, error) {
	joined := make([]joinedAssetDigestRow, len(rows))
	for i, row := range rows {
		joined[i] = convert(row)
	}
	return foldJoinedAssetDigests(joined)
}

func foldJoinedAssetDigests(rows []joinedAssetDigestRow) ([]domain.MediaAsset, error) {
	out := make([]domain.MediaAsset, 0, len(rows))
	for _, r := range rows {
		if len(out) == 0 || out[len(out)-1].ID != r.asset.ID {
			asset, err := mediaAssetModelToDomain(r.asset)
			if err != nil {
				return nil, err
			}
			out = append(out, asset)
		}
		if r.digestKind != nil && r.digestValue != nil && r.digestIsCurrent.Valid && r.digestReason != nil && r.digestRecordedAt != nil {
			out[len(out)-1].Digests = append(out[len(out)-1].Digests, assetDigestModelToDomain(AssetDigest{
				AssetID:    r.asset.ID,
				Kind:       *r.digestKind,
				Value:      *r.digestValue,
				IsCurrent:  r.digestIsCurrent.Bool,
				Reason:     *r.digestReason,
				RecordedAt: *r.digestRecordedAt,
			}))
		}
	}
	return out, nil
}

func prepareSeriesWrite(series *domain.Series, isCreate bool) (domain.Series, []byte, []byte, pgtype.Int4, pgtype.Int4, error) {
	if series == nil {
		return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, errors.New("series cannot be nil")
	}
	candidate := *series
	if isCreate && candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, fmt.Errorf("generating series ID: %w", err)
		}
		candidate.ID = id
	}
	if candidate.TitleSort == "" {
		candidate.TitleSort = candidate.Title
	}
	if candidate.Status == "" {
		candidate.Status = domain.SeriesStatusOngoing
	}
	if candidate.Genres == nil {
		candidate.Genres = []string{}
	} else {
		candidate.Genres = slices.Clone(candidate.Genres)
	}
	if candidate.Tags == nil {
		candidate.Tags = []string{}
	} else {
		candidate.Tags = slices.Clone(candidate.Tags)
	}
	if err := candidate.Validate(); err != nil {
		return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, err
	}
	candidate.CreatedAt, candidate.UpdatedAt = normalizeWriteTimestamps(candidate.CreatedAt, candidate.UpdatedAt, isCreate)

	lockedBytes, err := encodeJSONOrEmptyObject(candidate.LockedFields)
	if err != nil {
		return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, fmt.Errorf("encoding series locked_fields: %w", err)
	}
	externalBytes, err := encodeJSONOrEmptyObject(candidate.ExternalIDs)
	if err != nil {
		return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, fmt.Errorf("encoding series external_ids: %w", err)
	}
	totalVols, err := optIntToInt4("total_volumes", candidate.TotalVolumes)
	if err != nil {
		return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, err
	}
	releaseYear, err := optIntToInt4("release_year", candidate.ReleaseYear)
	if err != nil {
		return domain.Series{}, nil, nil, pgtype.Int4{}, pgtype.Int4{}, err
	}
	return candidate, lockedBytes, externalBytes, totalVols, releaseYear, nil
}

func prepareVolumeWrite(volume *domain.Volume, isCreate bool) (domain.Volume, []byte, []byte, int32, error) {
	if volume == nil {
		return domain.Volume{}, nil, nil, 0, errors.New("volume cannot be nil")
	}
	candidate := *volume
	if isCreate && candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return domain.Volume{}, nil, nil, 0, fmt.Errorf("generating volume ID: %w", err)
		}
		candidate.ID = id
	}
	if candidate.Kind == "" {
		candidate.Kind = domain.VolumeKindVolume
	}
	if err := candidate.Validate(); err != nil {
		return domain.Volume{}, nil, nil, 0, err
	}
	if err := validateVolumeFloats(candidate); err != nil {
		return domain.Volume{}, nil, nil, 0, err
	}
	if candidate.PageCount > math.MaxInt32 {
		return domain.Volume{}, nil, nil, 0, fmt.Errorf("volume page_count %d exceeds int32 range", candidate.PageCount)
	}
	candidate.CreatedAt, candidate.UpdatedAt = normalizeWriteTimestamps(candidate.CreatedAt, candidate.UpdatedAt, isCreate)

	externalBytes, err := encodeJSONOrEmptyObject(candidate.ExternalIDs)
	if err != nil {
		return domain.Volume{}, nil, nil, 0, fmt.Errorf("encoding volume external_ids: %w", err)
	}
	lockedBytes, err := encodeJSONOrEmptyObject(candidate.LockedFields)
	if err != nil {
		return domain.Volume{}, nil, nil, 0, fmt.Errorf("encoding volume locked_fields: %w", err)
	}
	return candidate, externalBytes, lockedBytes, int32(candidate.PageCount), nil
}

type preparedAssetWrite struct {
	candidate        domain.MediaAsset
	pageCount        int32
	coverPageIndex   int32
	suppressedPages  []int32
	diagnosticsBytes []byte
}

func prepareAssetWrite(asset *domain.MediaAsset, isCreate bool) (preparedAssetWrite, error) {
	if asset == nil {
		return preparedAssetWrite{}, errors.New("media asset cannot be nil")
	}
	candidate := *asset
	if isCreate && candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return preparedAssetWrite{}, fmt.Errorf("generating media asset ID: %w", err)
		}
		candidate.ID = id
	}
	resolvedPath, err := resolveAssetPathIdentity(domain.PathIdentity{
		Bytes:   candidate.RelPathBytes,
		Display: candidate.RelPathDisplay,
		Match:   candidate.RelPathMatch,
	})
	if err != nil {
		return preparedAssetWrite{}, err
	}
	candidate.RelPathBytes = resolvedPath.Bytes
	candidate.RelPathDisplay = resolvedPath.Display
	candidate.RelPathMatch = resolvedPath.Match
	candidate.Diagnostics.InvalidUTF8Path = resolvedPath.InvalidUTF8

	if err := candidate.Validate(); err != nil {
		return preparedAssetWrite{}, err
	}
	if candidate.PageCount > math.MaxInt32 {
		return preparedAssetWrite{}, fmt.Errorf("asset page_count %d exceeds int32 range", candidate.PageCount)
	}
	if candidate.CoverPageIndex > math.MaxInt32 {
		return preparedAssetWrite{}, fmt.Errorf("asset cover_page_index %d exceeds int32 range", candidate.CoverPageIndex)
	}
	suppressed, err := intSliceToInt32(candidate.SuppressedPages)
	if err != nil {
		return preparedAssetWrite{}, err
	}

	candidate.CreatedAt, candidate.UpdatedAt = normalizeWriteTimestamps(candidate.CreatedAt, candidate.UpdatedAt, isCreate)
	candidate.Diagnostics.ImageCodecs = slices.Clone(candidate.Diagnostics.ImageCodecs)
	diagnosticsBytes, err := json.Marshal(candidate.Diagnostics)
	if err != nil {
		return preparedAssetWrite{}, fmt.Errorf("encoding asset diagnostics: %w", err)
	}

	return preparedAssetWrite{
		candidate:        candidate,
		pageCount:        int32(candidate.PageCount),
		coverPageIndex:   int32(candidate.CoverPageIndex),
		suppressedPages:  suppressed,
		diagnosticsBytes: diagnosticsBytes,
	}, nil
}

func prepareInitialAssetDigests(assetID string, defaultRecordedAt time.Time, digests []domain.AssetDigest) ([]domain.AssetDigest, error) {
	if len(digests) == 0 {
		return nil, nil
	}
	out := make([]domain.AssetDigest, len(digests))
	for i, d := range digests {
		if d.AssetID == "" {
			d.AssetID = assetID
		} else if d.AssetID != assetID {
			return nil, fmt.Errorf("asset digest asset_id %q does not match asset %q", d.AssetID, assetID)
		}
		if d.RecordedAt.IsZero() {
			d.RecordedAt = defaultRecordedAt
		} else {
			d.RecordedAt = d.RecordedAt.UTC()
		}
		if err := d.Validate(); err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func resolveAssetPathIdentity(p domain.PathIdentity) (domain.PathIdentity, error) {
	if err := domain.ValidateRelativePathBytes(p.Bytes); err != nil {
		return domain.PathIdentity{}, err
	}
	expected := domain.NewPathIdentity(p.Bytes)
	if p.Display == "" && p.Match == "" {
		return expected, nil
	}
	if p.Display != expected.Display || p.Match != expected.Match {
		return domain.PathIdentity{}, domain.ErrInvalidPathIdentity
	}
	return expected, nil
}

func validateVolumeFloats(v domain.Volume) error {
	if v.Number != nil && (math.IsNaN(*v.Number) || math.IsInf(*v.Number, 0)) {
		return errors.New("volume number must be finite")
	}
	if v.ChapterStart != nil && (math.IsNaN(*v.ChapterStart) || math.IsInf(*v.ChapterStart, 0)) {
		return domain.ErrInvalidChapterRange
	}
	if v.ChapterEnd != nil && (math.IsNaN(*v.ChapterEnd) || math.IsInf(*v.ChapterEnd, 0)) {
		return domain.ErrInvalidChapterRange
	}
	return nil
}

func normalizeWriteTimestamps(createdAt, updatedAt time.Time, isCreate bool) (time.Time, time.Time) {
	now := time.Now().UTC()
	if createdAt.IsZero() {
		createdAt = now
	} else {
		createdAt = createdAt.UTC()
	}
	if updatedAt.IsZero() {
		if isCreate {
			updatedAt = createdAt
		} else {
			updatedAt = now
		}
	} else {
		updatedAt = updatedAt.UTC()
	}
	return createdAt, updatedAt
}

func normalizeTimestampOrNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t.UTC()
}

func encodeJSONOrEmptyObject[M ~map[string]V, V any](m M) ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

func optIntToInt4(field string, v *int) (pgtype.Int4, error) {
	if v == nil {
		return pgtype.Int4{}, nil
	}
	if *v < math.MinInt32 || *v > math.MaxInt32 {
		return pgtype.Int4{}, fmt.Errorf("%s %d exceeds int32 range", field, *v)
	}
	return pgtype.Int4{Int32: int32(*v), Valid: true}, nil
}

func optFloat64ToFloat8(v *float64) pgtype.Float8 {
	if v == nil {
		return pgtype.Float8{}
	}
	val := *v
	if val == 0 {
		val = 0
	}
	return pgtype.Float8{Float64: val, Valid: true}
}

func optTimeToPgDate(t *time.Time) pgtype.Date {
	if t == nil {
		return pgtype.Date{}
	}
	utc := t.UTC()
	return pgtype.Date{
		Time:  time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC),
		Valid: true,
	}
}

func intSliceToInt32(in []int) ([]int32, error) {
	if len(in) == 0 {
		return []int32{}, nil
	}
	out := make([]int32, len(in))
	for i, v := range in {
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("suppressed page index %d exceeds int32 range", v)
		}
		out[i] = int32(v)
	}
	return out, nil
}

func coverPolicyPtrToStringPtr(p *domain.CoverPolicy) *string {
	if p == nil {
		return nil
	}
	s := string(*p)
	return &s
}

func chapterRangeSourcePtrToStringPtr(s *domain.ChapterRangeSource) *string {
	if s == nil {
		return nil
	}
	val := string(*s)
	return &val
}

func seriesModelToDomain(row Series) (domain.Series, error) {
	lockedFields, err := decodeFieldLocks(row.LockedFields)
	if err != nil {
		return domain.Series{}, fmt.Errorf("decoding series locked_fields: %w", err)
	}
	externalIDs, err := decodeJSONMap(row.ExternalIds)
	if err != nil {
		return domain.Series{}, fmt.Errorf("decoding series external_ids: %w", err)
	}

	var coverPolicy *domain.CoverPolicy
	if row.CoverPolicy != nil {
		cp := domain.CoverPolicy(*row.CoverPolicy)
		coverPolicy = &cp
	}
	var totalVolumes *int
	if row.TotalVolumes.Valid {
		tv := int(row.TotalVolumes.Int32)
		totalVolumes = &tv
	}
	var releaseYear *int
	if row.ReleaseYear.Valid {
		ry := int(row.ReleaseYear.Int32)
		releaseYear = &ry
	}

	genres := slices.Clone(row.Genres)
	if genres == nil {
		genres = []string{}
	}
	tags := slices.Clone(row.Tags)
	if tags == nil {
		tags = []string{}
	}

	return domain.Series{
		ID:               row.ID,
		LibraryID:        row.LibraryID,
		ResolverKey:      cloneStringPtr(row.ResolverKey),
		Title:            row.Title,
		TitleSort:        row.TitleSort,
		Language:         cloneStringPtr(row.Language),
		OriginalTitle:    cloneStringPtr(row.OriginalTitle),
		RomajiTitle:      cloneStringPtr(row.RomajiTitle),
		Franchise:        cloneStringPtr(row.Franchise),
		EditionName:      cloneStringPtr(row.EditionName),
		Publisher:        cloneStringPtr(row.Publisher),
		Imprint:          cloneStringPtr(row.Imprint),
		Status:           domain.SeriesStatus(row.Status),
		Summary:          cloneStringPtr(row.Summary),
		AgeRating:        cloneStringPtr(row.AgeRating),
		Genres:           genres,
		Tags:             tags,
		CoverPolicy:      coverPolicy,
		CoverVolumeID:    cloneStringPtr(row.CoverVolumeID),
		CoverImageDigest: cloneStringPtr(row.CoverImageDigest),
		LockedFields:     lockedFields,
		ExternalIDs:      externalIDs,
		TotalVolumes:     totalVolumes,
		ReleaseYear:      releaseYear,
		CreatedAt:        row.CreatedAt.UTC(),
		UpdatedAt:        row.UpdatedAt.UTC(),
	}, nil
}

func volumeModelToDomain(row Volume) (domain.Volume, error) {
	externalIDs, err := decodeJSONMap(row.ExternalIds)
	if err != nil {
		return domain.Volume{}, fmt.Errorf("decoding volume external_ids: %w", err)
	}
	lockedFields, err := decodeFieldLocks(row.LockedFields)
	if err != nil {
		return domain.Volume{}, fmt.Errorf("decoding volume locked_fields: %w", err)
	}

	var number, chapterStart, chapterEnd *float64
	if row.Number.Valid {
		n := row.Number.Float64
		number = &n
	}
	if row.ChapterStart.Valid {
		cs := row.ChapterStart.Float64
		chapterStart = &cs
	}
	if row.ChapterEnd.Valid {
		ce := row.ChapterEnd.Float64
		chapterEnd = &ce
	}
	var chapterRangeSource *domain.ChapterRangeSource
	if row.ChapterRangeSource != nil {
		crs := domain.ChapterRangeSource(*row.ChapterRangeSource)
		chapterRangeSource = &crs
	}
	var releaseDate *time.Time
	if row.ReleaseDate.Valid {
		rd := row.ReleaseDate.Time.UTC()
		releaseDate = &rd
	}

	return domain.Volume{
		ID:                 row.ID,
		LibraryID:          row.LibraryID,
		Kind:               domain.VolumeKind(row.Kind),
		Number:             number,
		ChapterStart:       chapterStart,
		ChapterEnd:         chapterEnd,
		ChapterRangeSource: chapterRangeSource,
		Title:              cloneStringPtr(row.Title),
		Summary:            cloneStringPtr(row.Summary),
		ReleaseDate:        releaseDate,
		PageCount:          int(row.PageCount),
		PreferredAssetID:   cloneStringPtr(row.PreferredAssetID),
		ExternalIDs:        externalIDs,
		LockedFields:       lockedFields,
		CreatedAt:          row.CreatedAt.UTC(),
		UpdatedAt:          row.UpdatedAt.UTC(),
	}, nil
}

func mediaAssetModelToDomain(row MediaAsset) (domain.MediaAsset, error) {
	diagnostics, err := decodeAssetDiagnostics(row.Diagnostics)
	if err != nil {
		return domain.MediaAsset{}, fmt.Errorf("decoding asset diagnostics: %w", err)
	}
	return domain.MediaAsset{
		ID:              row.ID,
		VolumeID:        row.VolumeID,
		LibraryRootID:   row.LibraryRootID,
		RelPathBytes:    slices.Clone(row.RelPathBytes),
		RelPathDisplay:  row.RelPathDisplay,
		RelPathMatch:    row.RelPathMatch,
		FileSize:        row.FileSize,
		FileMtime:       normalizeOptionalTimeUTC(row.FileMtime),
		Format:          domain.MediaFormat(row.Format),
		PageCount:       int(row.PageCount),
		CoverPageIndex:  int(row.CoverPageIndex),
		SuppressedPages: int32SliceToInt(row.SuppressedPages),
		Diagnostics:     diagnostics,
		MissingSince:    normalizeOptionalTimeUTC(row.MissingSince),
		RetiredAt:       normalizeOptionalTimeUTC(row.RetiredAt),
		CreatedAt:       row.CreatedAt.UTC(),
		UpdatedAt:       row.UpdatedAt.UTC(),
	}, nil
}

func assetDigestModelToDomain(row AssetDigest) domain.AssetDigest {
	return domain.AssetDigest{
		AssetID:    row.AssetID,
		Kind:       domain.DigestKind(row.Kind),
		Value:      row.Value,
		IsCurrent:  row.IsCurrent,
		Reason:     domain.DigestReason(row.Reason),
		RecordedAt: row.RecordedAt.UTC(),
	}
}

func mapCatalogueWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return storage.ErrNotFound
	}
	return mapPgError(err)
}
