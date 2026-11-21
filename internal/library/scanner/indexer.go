package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/library/resolver"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
	"github.com/inexcession/tobiten/internal/storage"
)

// IndexerOptions configures an Indexer instance.
type IndexerOptions struct {
	Outbox *events.Outbox
	Now    func() time.Time
}

// IndexFileInput supplies the inspected file metadata and target library root
// for transactional catalogue indexing (M1-T10.5, M1B-D3, M1B-D5).
type IndexFileInput struct {
	LibraryID     string
	LibraryRootID string
	Inspected     InspectedFile
	// TargetVolumeID optionally supplies independent identity evidence (M1B-D5)
	// identifying a specific existing volume in the same library to attach to,
	// bypassing standalone/unnumbered/ambiguous non-grouping.
	TargetVolumeID string
}

// IndexFileResult describes the committed catalogue records and outbox events
// produced by indexing one inspected file.
type IndexFileResult struct {
	Series               *domain.Series
	SeriesCreated        bool
	Volume               domain.Volume
	VolumeCreated        bool
	Memberships          []domain.SeriesMembership
	Asset                domain.MediaAsset
	Resolution           resolver.Result
	AddedEvent           domain.DomainEvent
	InvalidUTF8PathEvent *domain.DomainEvent
}

// Indexer persists inspected files into the library catalogue inside one
// database transaction per file, composing series resolution, volume grouping,
// membership reconciliation, asset/digest creation, ComicInfo credit/entity
// import, and transactional outbox events.
type Indexer struct {
	tx     storage.Transactor
	outbox *events.Outbox
	now    func() time.Time
}

// NewIndexer constructs an Indexer backed by tx.
func NewIndexer(tx storage.Transactor, opts IndexerOptions) (*Indexer, error) {
	if tx == nil {
		return nil, errors.New("scanner: transactor is required")
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	outbox := opts.Outbox
	if outbox == nil {
		outbox = events.NewOutbox(events.WithClock(nowFn))
	}
	return &Indexer{
		tx:     tx,
		outbox: outbox,
		now:    nowFn,
	}, nil
}

// IndexFile resolves and indexes one inspected file in a single transaction.
func (idx *Indexer) IndexFile(ctx context.Context, in IndexFileInput) (*IndexFileResult, error) {
	if idx == nil || idx.tx == nil {
		return nil, errors.New("scanner: indexer is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	relPath, err := validateIndexFileInput(in)
	if err != nil {
		return nil, err
	}

	var result *IndexFileResult
	err = idx.tx.WithTx(ctx, func(tx storage.Tx) error {
		var txErr error
		result, txErr = idx.indexFileWithValidatedPath(ctx, tx, in, relPath)
		return txErr
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func validateIndexFileInput(in IndexFileInput) (domain.PathIdentity, error) {
	if strings.TrimSpace(in.LibraryID) == "" {
		return domain.PathIdentity{}, domain.ErrEmptyLibraryID
	}
	if strings.TrimSpace(in.LibraryRootID) == "" {
		return domain.PathIdentity{}, domain.ErrEmptyLibraryRootID
	}
	relPath, err := normalizeInspectedRelPath(in.Inspected.RelPath)
	if err != nil {
		return domain.PathIdentity{}, err
	}
	if !in.Inspected.Format.IsValid() {
		return domain.PathIdentity{}, domain.ErrInvalidMediaFormat
	}
	if strings.TrimSpace(string(in.Inspected.DigestKind)) == "" {
		return domain.PathIdentity{}, domain.ErrInvalidDigestKind
	}
	if strings.TrimSpace(in.Inspected.Digest) == "" {
		return domain.PathIdentity{}, domain.ErrEmptyDigestValue
	}
	if in.Inspected.Size < 0 {
		return domain.PathIdentity{}, errors.New("scanner: asset file_size cannot be negative")
	}
	if in.Inspected.PageCount < 0 {
		return domain.PathIdentity{}, errors.New("scanner: asset page_count cannot be negative")
	}
	if in.Inspected.CoverPageIndex < 0 {
		return domain.PathIdentity{}, errors.New("scanner: asset cover_page_index cannot be negative")
	}
	return relPath, nil
}

func normalizeInspectedRelPath(p domain.PathIdentity) (domain.PathIdentity, error) {
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

func (idx *Indexer) indexFileWithValidatedPath(
	ctx context.Context,
	tx storage.Tx,
	in IndexFileInput,
	relPath domain.PathIdentity,
) (*IndexFileResult, error) {
	now := idx.now().UTC()
	lib, root, err := loadLibraryAndRoot(ctx, tx, in.LibraryID, in.LibraryRootID)
	if err != nil {
		return nil, err
	}

	res, err := resolver.Resolve(in.Inspected.ComicInfo, relPath, lib.ResolverChain)
	if err != nil {
		return nil, err
	}
	res.Number = normalizeZeroFloatPtr(res.Number)

	cw := tx.CatalogueWrites()
	series, proposedSeries, seriesCreated, err := resolveOrCreateSeries(ctx, cw, lib.ID, res, in.Inspected.ComicInfo, now)
	if err != nil {
		return nil, err
	}

	var seriesID string
	if series != nil {
		seriesID = series.ID
	}
	// Serialize concurrent indexers on the same (series, kind, number) variant from
	// zero-match volume lookup through membership creation so two workers do not create
	// duplicate volumes for a newly seen number.
	if strings.TrimSpace(in.TargetVolumeID) == "" && !res.Standalone && seriesID != "" && res.Number != nil {
		if err := cw.AcquireSeriesVariantLock(ctx, seriesID, res.VolumeKind, *res.Number); err != nil {
			return nil, err
		}
	}
	vol, volCreated, err := resolveOrCreateVolume(ctx, cw, lib.ID, seriesID, res, in.Inspected, in.TargetVolumeID, now)
	if err != nil {
		return nil, err
	}

	// Lock and sync an already-existing series only AFTER resolving/locking the
	// volume so the lock order matches volumes -> series_memberships -> series.
	if !seriesCreated && series != nil {
		series, err = syncExistingSeries(ctx, cw, series.ID, proposedSeries, now)
		if err != nil {
			return nil, err
		}
	}

	memberships, err := syncIndexedVolumeMemberships(ctx, tx, vol.ID, seriesID, res, volCreated)
	if err != nil {
		return nil, err
	}
	vol.Memberships = memberships

	asset, err := createIndexedAsset(ctx, cw, vol.ID, root.ID, relPath, in.Inspected, now)
	if err != nil {
		return nil, err
	}

	if in.Inspected.ComicInfo != nil {
		if err := comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, *in.Inspected.ComicInfo); err != nil {
			return nil, err
		}
	}

	addedEv, invalidEv, err := idx.appendIndexEvents(ctx, tx.Events(), lib.ID, asset)
	if err != nil {
		return nil, err
	}

	return &IndexFileResult{
		Series:               series,
		SeriesCreated:        seriesCreated,
		Volume:               vol,
		VolumeCreated:        volCreated,
		Memberships:          memberships,
		Asset:                asset,
		Resolution:           res,
		AddedEvent:           addedEv,
		InvalidUTF8PathEvent: invalidEv,
	}, nil
}

func loadLibraryAndRoot(
	ctx context.Context,
	tx storage.Tx,
	libraryID, libraryRootID string,
) (*domain.Library, *domain.LibraryRoot, error) {
	libRepo := tx.Libraries()
	if libRepo == nil {
		return nil, nil, errors.New("scanner: library repository is required")
	}
	lib, err := libRepo.GetLibraryByID(ctx, libraryID)
	if err != nil {
		return nil, nil, err
	}
	root, err := libRepo.GetLibraryRootByID(ctx, libraryRootID)
	if err != nil {
		return nil, nil, err
	}
	if root.LibraryID != lib.ID {
		return nil, nil, fmt.Errorf("scanner: library root %q belongs to library %q, expected %q", root.ID, root.LibraryID, lib.ID)
	}
	return lib, root, nil
}

func resolveOrCreateSeries(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	libraryID string,
	res resolver.Result,
	meta *comicinfo.Metadata,
	now time.Time,
) (*domain.Series, domain.Series, bool, error) {
	if res.Standalone || res.Series == nil {
		return nil, domain.Series{}, false, nil
	}
	proposed := buildProposedSeries(libraryID, *res.Series, meta, now)
	series := proposed
	created, err := cw.FindOrCreateSeriesByResolverKey(ctx, &series)
	if err != nil {
		return nil, domain.Series{}, false, err
	}
	return &series, proposed, created, nil
}

func syncExistingSeries(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	seriesID string,
	proposed domain.Series,
	now time.Time,
) (*domain.Series, error) {
	locked, err := cw.GetSeriesByIDForUpdate(ctx, seriesID)
	if err != nil {
		return nil, err
	}
	merged, changed := mergeSeriesForIndex(*locked, proposed, now)
	if !changed {
		return locked, nil
	}
	if err := cw.UpdateSeriesMetadata(ctx, &merged); err != nil {
		return nil, err
	}
	return &merged, nil
}

func resolveOrCreateVolume(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	libraryID, seriesID string,
	res resolver.Result,
	inspected InspectedFile,
	targetVolumeID string,
	now time.Time,
) (domain.Volume, bool, error) {
	proposed := buildProposedVolume(libraryID, res, inspected, now)
	if strings.TrimSpace(targetVolumeID) != "" {
		existing, err := cw.GetVolumeByIDForUpdate(ctx, targetVolumeID)
		if err != nil {
			return domain.Volume{}, false, err
		}
		if existing.LibraryID != libraryID {
			return domain.Volume{}, false, fmt.Errorf("scanner: target volume %q belongs to library %q, expected %q", existing.ID, existing.LibraryID, libraryID)
		}
		return updateMatchedVolume(ctx, cw, *existing, proposed, now)
	}

	if !res.Standalone && seriesID != "" && res.Number != nil {
		matches, err := cw.FindVolumesByPrimarySeriesKindNumber(ctx, libraryID, seriesID, res.VolumeKind, *res.Number)
		if err != nil {
			return domain.Volume{}, false, err
		}
		if len(matches) == 1 {
			return updateMatchedVolume(ctx, cw, matches[0], proposed, now)
		}
	}

	vol := proposed
	if err := cw.CreateVolume(ctx, &vol); err != nil {
		return domain.Volume{}, false, err
	}
	return vol, true, nil
}

func updateMatchedVolume(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	existing domain.Volume,
	proposed domain.Volume,
	now time.Time,
) (domain.Volume, bool, error) {
	merged, changed, err := mergeVolumeForIndex(existing, proposed, now)
	if err != nil {
		return domain.Volume{}, false, err
	}
	if !changed {
		return existing, false, nil
	}
	if err := cw.UpdateVolume(ctx, &merged); err != nil {
		return domain.Volume{}, false, err
	}
	return merged, false, nil
}

func syncIndexedVolumeMemberships(
	ctx context.Context,
	tx storage.Tx,
	volumeID, seriesID string,
	res resolver.Result,
	volCreated bool,
) ([]domain.SeriesMembership, error) {
	if res.Standalone && volCreated {
		return nil, nil
	}
	var desired []domain.SeriesMembership
	if !res.Standalone && seriesID != "" {
		desired = []domain.SeriesMembership{
			{
				VolumeID:  volumeID,
				SeriesID:  seriesID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  normalizeZeroFloatPtr(res.Number),
				Source:    domain.MembershipSourceResolver,
			},
		}
	}
	repo := tx.SeriesMemberships()
	if err := repo.SyncResolverMemberships(ctx, volumeID, desired); err != nil {
		return nil, err
	}
	return repo.ListMembershipsByVolume(ctx, volumeID)
}

func createIndexedAsset(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	volumeID, rootID string,
	relPath domain.PathIdentity,
	inspected InspectedFile,
	now time.Time,
) (domain.MediaAsset, error) {
	var fileMtime *time.Time
	if !inspected.Mtime.IsZero() {
		mt := inspected.Mtime.UTC()
		fileMtime = &mt
	}
	asset := domain.MediaAsset{
		VolumeID:        volumeID,
		LibraryRootID:   rootID,
		RelPathBytes:    slices.Clone(relPath.Bytes),
		RelPathDisplay:  relPath.Display,
		RelPathMatch:    relPath.Match,
		FileSize:        inspected.Size,
		FileMtime:       fileMtime,
		Format:          inspected.Format,
		PageCount:       inspected.PageCount,
		CoverPageIndex:  inspected.CoverPageIndex,
		SuppressedPages: slices.Clone(inspected.SuppressedPages),
		Diagnostics: domain.AssetDiagnostics{
			ImageCodecs:     slices.Clone(inspected.ImageCodecs),
			InvalidUTF8Path: relPath.InvalidUTF8,
		},
		Digests: []domain.AssetDigest{
			{
				Kind:       inspected.DigestKind,
				Value:      inspected.Digest,
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := cw.CreateAsset(ctx, &asset); err != nil {
		return domain.MediaAsset{}, err
	}
	return asset, nil
}

func (idx *Indexer) appendIndexEvents(
	ctx context.Context,
	appender events.Appender,
	libraryID string,
	asset domain.MediaAsset,
) (domain.DomainEvent, *domain.DomainEvent, error) {
	payload := domain.AssetEventPayloadV1{
		AssetID:       asset.ID,
		VolumeID:      asset.VolumeID,
		LibraryID:     libraryID,
		LibraryRootID: asset.LibraryRootID,
		Format:        asset.Format,
		PathDisplay:   asset.RelPathDisplay,
	}
	addedEv, err := idx.outbox.AppendPayload(
		ctx,
		appender,
		domain.EventAssetAdded,
		domain.EventVersion1,
		domain.AggregateTypeAsset,
		asset.ID,
		payload,
	)
	if err != nil {
		return domain.DomainEvent{}, nil, err
	}

	var invalidEv *domain.DomainEvent
	if asset.Diagnostics.InvalidUTF8Path {
		ev, err := idx.outbox.AppendPayload(
			ctx,
			appender,
			domain.EventAssetInvalidUTF8Path,
			domain.EventVersion1,
			domain.AggregateTypeAsset,
			asset.ID,
			payload,
		)
		if err != nil {
			return domain.DomainEvent{}, nil, err
		}
		invalidEv = ev
	}
	return *addedEv, invalidEv, nil
}

func buildProposedSeries(
	libraryID string,
	sr resolver.SeriesResolution,
	meta *comicinfo.Metadata,
	now time.Time,
) domain.Series {
	key := sr.Key
	s := domain.Series{
		LibraryID:   libraryID,
		ResolverKey: &key,
		Title:       sr.Title,
		TitleSort:   sr.TitleSort,
		Status:      domain.SeriesStatusOngoing,
		Genres:      []string{},
		Tags:        []string{},
		ExternalIDs: map[string]any{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if meta == nil {
		return s
	}
	s.Language = optTrimmedStringPtr(meta.LanguageISO)
	s.Publisher = optTrimmedStringPtr(meta.Publisher)
	s.Imprint = optTrimmedStringPtr(meta.Imprint)
	s.AgeRating = optTrimmedStringPtr(meta.AgeRating)
	s.Genres = cleanStringSlice(meta.Genre)
	s.Tags = cleanStringSlice(meta.Tags)
	if meta.Count != nil && *meta.Count >= 0 {
		tv := int(*meta.Count)
		s.TotalVolumes = &tv
	}
	if meta.Year != nil && *meta.Year > 0 {
		ry := int(*meta.Year)
		s.ReleaseYear = &ry
	}
	return s
}

func mergeSeriesForIndex(existing, proposed domain.Series, now time.Time) (domain.Series, bool) {
	out := existing
	out.Genres = slices.Clone(existing.Genres)
	out.Tags = slices.Clone(existing.Tags)
	out.ExternalIDs = maps.Clone(existing.ExternalIDs)
	out.LockedFields = maps.Clone(existing.LockedFields)

	wholeLocked := existing.LockedFields.IsLocked("*")
	unlocked := func(field string) bool {
		return !wholeLocked && !existing.LockedFields.IsLocked(field)
	}

	if unlocked("title") && proposed.Title != "" {
		out.Title = proposed.Title
	}
	if unlocked("title_sort") && proposed.TitleSort != "" {
		out.TitleSort = proposed.TitleSort
	}
	if unlocked("language") && proposed.Language != nil {
		l := *proposed.Language
		out.Language = &l
	}
	if unlocked("publisher") && proposed.Publisher != nil {
		p := *proposed.Publisher
		out.Publisher = &p
	}
	if unlocked("imprint") && proposed.Imprint != nil {
		imp := *proposed.Imprint
		out.Imprint = &imp
	}
	if unlocked("age_rating") && proposed.AgeRating != nil {
		ar := *proposed.AgeRating
		out.AgeRating = &ar
	}
	if unlocked("genres") && len(proposed.Genres) > 0 {
		out.Genres = slices.Clone(proposed.Genres)
	}
	if unlocked("tags") && len(proposed.Tags) > 0 {
		out.Tags = slices.Clone(proposed.Tags)
	}
	if unlocked("total_volumes") && proposed.TotalVolumes != nil {
		tv := *proposed.TotalVolumes
		out.TotalVolumes = &tv
	}
	if unlocked("release_year") && existing.ReleaseYear == nil && proposed.ReleaseYear != nil {
		ry := *proposed.ReleaseYear
		out.ReleaseYear = &ry
	}

	if indexedSeriesEqual(existing, out) {
		return existing, false
	}
	out.UpdatedAt = now
	return out, true
}

func buildProposedVolume(
	libraryID string,
	res resolver.Result,
	inspected InspectedFile,
	now time.Time,
) domain.Volume {
	kind := res.VolumeKind
	if kind == "" {
		kind = domain.VolumeKindVolume
	}
	v := domain.Volume{
		LibraryID:   libraryID,
		Kind:        kind,
		Number:      normalizeZeroFloatPtr(res.Number),
		PageCount:   inspected.PageCount,
		ExternalIDs: map[string]any{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if inspected.ComicInfo == nil {
		return v
	}
	meta := inspected.ComicInfo
	v.Title = optTrimmedStringPtr(meta.Title)
	v.Summary = optTrimmedStringPtr(meta.Summary)
	v.ReleaseDate = comicInfoReleaseDate(meta)
	if gtin := strings.TrimSpace(meta.GTIN); gtin != "" {
		v.ExternalIDs["gtin"] = gtin
	}
	if web := strings.TrimSpace(meta.Web); web != "" {
		v.ExternalIDs["web"] = web
	}
	return v
}

func mergeVolumeForIndex(existing, proposed domain.Volume, now time.Time) (domain.Volume, bool, error) {
	out := existing
	out.ExternalIDs = maps.Clone(existing.ExternalIDs)
	out.LockedFields = maps.Clone(existing.LockedFields)

	wholeLocked := existing.LockedFields.IsLocked("*")
	unlocked := func(field string) bool {
		return !wholeLocked && !existing.LockedFields.IsLocked(field)
	}

	if unlocked("kind") && proposed.Kind != "" {
		out.Kind = proposed.Kind
	}
	if unlocked("number") && proposed.Number != nil {
		n := *proposed.Number
		out.Number = &n
	}
	if unlocked("title") && proposed.Title != nil {
		t := *proposed.Title
		out.Title = &t
	}
	if unlocked("summary") && proposed.Summary != nil {
		s := *proposed.Summary
		out.Summary = &s
	}
	if unlocked("release_date") && proposed.ReleaseDate != nil {
		d := *proposed.ReleaseDate
		out.ReleaseDate = &d
	}
	if unlocked("page_count") {
		out.PageCount = proposed.PageCount
	}
	if unlocked("external_ids") && len(proposed.ExternalIDs) > 0 {
		if out.ExternalIDs == nil {
			out.ExternalIDs = make(map[string]any, len(proposed.ExternalIDs))
		}
		maps.Copy(out.ExternalIDs, proposed.ExternalIDs)
	}

	same, err := indexedVolumesEqual(existing, out)
	if err != nil {
		return domain.Volume{}, false, err
	}
	if same {
		return existing, false, nil
	}
	out.UpdatedAt = now
	return out, true, nil
}

func indexedSeriesEqual(a, b domain.Series) bool {
	return a.Title == b.Title &&
		a.TitleSort == b.TitleSort &&
		optStringEqual(a.Language, b.Language) &&
		optStringEqual(a.Publisher, b.Publisher) &&
		optStringEqual(a.Imprint, b.Imprint) &&
		optStringEqual(a.AgeRating, b.AgeRating) &&
		slices.Equal(a.Genres, b.Genres) &&
		slices.Equal(a.Tags, b.Tags) &&
		optIntEqual(a.TotalVolumes, b.TotalVolumes) &&
		optIntEqual(a.ReleaseYear, b.ReleaseYear)
}

func indexedVolumesEqual(a, b domain.Volume) (bool, error) {
	if a.Kind != b.Kind ||
		!optFloatEqual(a.Number, b.Number) ||
		!optStringEqual(a.Title, b.Title) ||
		!optStringEqual(a.Summary, b.Summary) ||
		!optDateEqual(a.ReleaseDate, b.ReleaseDate) ||
		a.PageCount != b.PageCount {
		return false, nil
	}
	return jsonMapsEqual(a.ExternalIDs, b.ExternalIDs)
}

func comicInfoReleaseDate(meta *comicinfo.Metadata) *time.Time {
	if meta == nil || meta.Year == nil {
		return nil
	}
	month := 1
	if meta.Month != nil {
		month = int(*meta.Month)
	}
	day := 1
	if meta.Day != nil {
		day = int(*meta.Day)
	}
	d := time.Date(int(*meta.Year), time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return &d
}

func optTrimmedStringPtr(s string) *string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func cleanStringSlice(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func normalizeZeroFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	c := *v
	if c == 0 {
		c = 0
	}
	return &c
}

func optStringEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func optIntEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func optFloatEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func optDateEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.UTC().Format(time.DateOnly) == b.UTC().Format(time.DateOnly)
}

func jsonMapsEqual(a, b map[string]any) (bool, error) {
	ab, err := encodeJSONObject(a)
	if err != nil {
		return false, err
	}
	bb, err := encodeJSONObject(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ab, bb), nil
}

func encodeJSONObject(m map[string]any) ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}
