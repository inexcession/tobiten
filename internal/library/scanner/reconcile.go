package scanner

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/resolver"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
	"github.com/inexcession/tobiten/internal/storage"
)

type assetLocation struct {
	rootID       string
	relPathBytes string
}

type digestKey struct {
	kind  domain.DigestKind
	value string
}

type observedFile struct {
	rootID    string
	inspected InspectedFile
}

type observedChanged struct {
	rootID    string
	existing  domain.MediaAsset
	inspected InspectedFile
}

type libraryWalkObservations struct {
	seenUnchanged map[assetLocation]struct{}
	seenSkipped   map[assetLocation]struct{}
	changed       []observedChanged
	deferredNew   []observedFile
}

func newLibraryWalkObservations() *libraryWalkObservations {
	return &libraryWalkObservations{
		seenUnchanged: make(map[assetLocation]struct{}),
		seenSkipped:   make(map[assetLocation]struct{}),
	}
}

type candidateObservation struct {
	loc       assetLocation
	rootID    string
	inspected InspectedFile
}

type plannedMove struct {
	Asset        domain.MediaAsset
	TargetRootID string
	Inspected    InspectedFile
	obsIdx       int
}

type plannedModify struct {
	Existing  domain.MediaAsset
	RootID    string
	Inspected InspectedFile
}

type reconciliationPlan struct {
	MoveComponents [][]plannedMove
	Modifies       []plannedModify
	MtimeRefreshes []plannedModify
	ToIndex        []observedFile
}

func (s *Scanner) reconcileLibraryPass(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	preWalkAssets []domain.MediaAsset,
	obs *libraryWalkObservations,
	res *ScanResult,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	_, currentRoots, err := s.loadLibraryAndRoots(ctx, libraryID)
	if err != nil {
		return err
	}
	if !rootSnapshotPathsMatch(checkedRoots, currentRoots) || !allRootsOnline(currentRoots) {
		return ErrStalePreCheck
	}

	plan := planReconciliation(preWalkAssets, obs)
	if err := s.executeMoves(ctx, libraryID, plan.MoveComponents, res); err != nil {
		return err
	}
	if err := s.executeModifies(ctx, libraryID, plan.Modifies, res); err != nil {
		return err
	}
	if err := s.executeMtimeRefreshes(ctx, plan.MtimeRefreshes); err != nil {
		return err
	}
	return s.executeDeferredIndexes(ctx, libraryID, plan.ToIndex, res)
}

func planReconciliation(preWalkAssets []domain.MediaAsset, obs *libraryWalkObservations) reconciliationPlan {
	existingByLoc := make(map[assetLocation]domain.MediaAsset, len(preWalkAssets))
	activeAssets := make([]domain.MediaAsset, 0, len(preWalkAssets))
	for _, asset := range preWalkAssets {
		loc := assetLocation{
			rootID:       asset.LibraryRootID,
			relPathBytes: string(asset.RelPathBytes),
		}
		existingByLoc[loc] = asset
		if asset.MissingSince == nil {
			activeAssets = append(activeAssets, asset)
		}
	}

	confirmedInPlace := make(map[string]struct{})
	var mtimeRefreshes []plannedModify
	var candidates []candidateObservation

	for _, ch := range obs.changed {
		if ch.existing.MissingSince != nil {
			continue
		}
		loc := assetLocation{
			rootID:       ch.rootID,
			relPathBytes: string(ch.inspected.RelPath.Bytes),
		}
		if cur, ok := ch.existing.CurrentDigest(ch.inspected.DigestKind); ok && cur.Value == ch.inspected.Digest {
			confirmedInPlace[ch.existing.ID] = struct{}{}
			if ch.existing.FileSize != ch.inspected.Size || !equalOptionalNormalizedMtime(ch.existing.FileMtime, ch.inspected.Mtime) {
				mtimeRefreshes = append(mtimeRefreshes, plannedModify{
					Existing:  ch.existing,
					RootID:    ch.rootID,
					Inspected: ch.inspected,
				})
			}
			continue
		}
		candidates = append(candidates, candidateObservation{
			loc:       loc,
			rootID:    ch.rootID,
			inspected: ch.inspected,
		})
	}

	for _, d := range obs.deferredNew {
		candidates = append(candidates, candidateObservation{
			loc: assetLocation{
				rootID:       d.rootID,
				relPathBytes: string(d.inspected.RelPath.Bytes),
			},
			rootID:    d.rootID,
			inspected: d.inspected,
		})
	}

	unconfirmed := collectUnconfirmedAssets(activeAssets, obs, confirmedInPlace)
	slices.SortFunc(candidates, compareCandidateObservation)

	proposedMoves, matchedObs := matchMovesFixedPoint(unconfirmed, candidates, existingByLoc)
	modifies, toIndex := classifyUnmatchedCandidates(candidates, matchedObs, existingByLoc, proposedMoves)

	moves := make([]plannedMove, 0, len(proposedMoves))
	for _, mv := range proposedMoves {
		moves = append(moves, mv)
	}
	slices.SortFunc(moves, func(a, b plannedMove) int {
		return cmp.Compare(a.Asset.ID, b.Asset.ID)
	})

	return reconciliationPlan{
		MoveComponents: partitionMoveComponents(moves),
		Modifies:       modifies,
		MtimeRefreshes: mtimeRefreshes,
		ToIndex:        toIndex,
	}
}

func collectUnconfirmedAssets(
	activeAssets []domain.MediaAsset,
	obs *libraryWalkObservations,
	confirmedInPlace map[string]struct{},
) []domain.MediaAsset {
	unconfirmed := make([]domain.MediaAsset, 0)
	for _, asset := range activeAssets {
		if _, ok := confirmedInPlace[asset.ID]; ok {
			continue
		}
		loc := assetLocation{
			rootID:       asset.LibraryRootID,
			relPathBytes: string(asset.RelPathBytes),
		}
		if _, ok := obs.seenUnchanged[loc]; ok {
			continue
		}
		if _, ok := obs.seenSkipped[loc]; ok {
			continue
		}
		unconfirmed = append(unconfirmed, asset)
	}
	slices.SortFunc(unconfirmed, compareAssetOrderForReconcile)
	return unconfirmed
}

func matchMovesFixedPoint(
	unconfirmed []domain.MediaAsset,
	candidates []candidateObservation,
	existingByLoc map[assetLocation]domain.MediaAsset,
) (map[string]plannedMove, map[int]bool) {
	blockedDest := make(map[assetLocation]struct{})
	for {
		proposedMoves, matchedObs := matchCandidateMoves(unconfirmed, candidates, blockedDest)
		newlyBlocked := findNewlyBlockedDestinations(proposedMoves, existingByLoc, blockedDest)
		if !newlyBlocked {
			return proposedMoves, matchedObs
		}
	}
}

func matchCandidateMoves(
	unconfirmed []domain.MediaAsset,
	candidates []candidateObservation,
	blockedDest map[assetLocation]struct{},
) (map[string]plannedMove, map[int]bool) {
	claimed := make(map[string]bool, len(unconfirmed))
	matchedObs := make(map[int]bool, len(candidates))
	proposed := make(map[string]plannedMove)

	matchPass := func(currentOnly bool) {
		for obsIdx, cand := range candidates {
			if matchedObs[obsIdx] {
				continue
			}
			if _, blocked := blockedDest[cand.loc]; blocked {
				continue
			}
			for _, asset := range unconfirmed {
				if claimed[asset.ID] {
					continue
				}
				oldLoc := assetLocation{
					rootID:       asset.LibraryRootID,
					relPathBytes: string(asset.RelPathBytes),
				}
				if oldLoc == cand.loc {
					continue
				}
				if !assetMatchesDigestTier(asset, cand.inspected.DigestKind, cand.inspected.Digest, currentOnly) {
					continue
				}
				claimed[asset.ID] = true
				matchedObs[obsIdx] = true
				proposed[asset.ID] = plannedMove{
					Asset:        asset,
					TargetRootID: cand.rootID,
					Inspected:    cand.inspected,
					obsIdx:       obsIdx,
				}
				break
			}
		}
	}

	matchPass(true)
	matchPass(false)
	return proposed, matchedObs
}

func findNewlyBlockedDestinations(
	proposedMoves map[string]plannedMove,
	existingByLoc map[assetLocation]domain.MediaAsset,
	blockedDest map[assetLocation]struct{},
) bool {
	newlyBlocked := false
	for _, mv := range proposedMoves {
		dst := assetLocation{
			rootID:       mv.TargetRootID,
			relPathBytes: string(mv.Inspected.RelPath.Bytes),
		}
		occupant, occupied := existingByLoc[dst]
		if !occupied {
			continue
		}
		if occupant.MissingSince != nil {
			if _, already := blockedDest[dst]; !already {
				blockedDest[dst] = struct{}{}
				newlyBlocked = true
			}
			continue
		}
		if _, vacated := proposedMoves[occupant.ID]; !vacated {
			if _, already := blockedDest[dst]; !already {
				blockedDest[dst] = struct{}{}
				newlyBlocked = true
			}
		}
	}
	return newlyBlocked
}

func classifyUnmatchedCandidates(
	candidates []candidateObservation,
	matchedObs map[int]bool,
	existingByLoc map[assetLocation]domain.MediaAsset,
	proposedMoves map[string]plannedMove,
) ([]plannedModify, []observedFile) {
	var modifies []plannedModify
	var toIndex []observedFile

	for obsIdx, cand := range candidates {
		if matchedObs[obsIdx] {
			continue
		}
		occupant, occupied := existingByLoc[cand.loc]
		if !occupied {
			toIndex = append(toIndex, observedFile{
				rootID:    cand.rootID,
				inspected: cand.inspected,
			})
			continue
		}
		if occupant.MissingSince != nil {
			// Leave missing/retired asset paths to M1-T22.2 reconciliation.
			continue
		}
		if _, vacated := proposedMoves[occupant.ID]; vacated {
			toIndex = append(toIndex, observedFile{
				rootID:    cand.rootID,
				inspected: cand.inspected,
			})
			continue
		}
		modifies = append(modifies, plannedModify{
			Existing:  occupant,
			RootID:    cand.rootID,
			Inspected: cand.inspected,
		})
	}
	return modifies, toIndex
}

func findDSURoot(parent []int, x int) int {
	for parent[x] != x {
		parent[x] = parent[parent[x]]
		x = parent[x]
	}
	return x
}

func partitionMoveComponents(moves []plannedMove) [][]plannedMove {
	if len(moves) == 0 {
		return nil
	}
	parent := make([]int, len(moves))
	for i := range parent {
		parent[i] = i
	}
	union := func(a, b int) {
		ra, rb := findDSURoot(parent, a), findDSURoot(parent, b)
		if ra != rb {
			if ra < rb {
				parent[rb] = ra
			} else {
				parent[ra] = rb
			}
		}
	}

	locOwner := make(map[assetLocation]int, len(moves)*2)
	for i, mv := range moves {
		src := assetLocation{rootID: mv.Asset.LibraryRootID, relPathBytes: string(mv.Asset.RelPathBytes)}
		dst := assetLocation{rootID: mv.TargetRootID, relPathBytes: string(mv.Inspected.RelPath.Bytes)}
		for _, loc := range []assetLocation{src, dst} {
			if prev, seen := locOwner[loc]; seen {
				union(prev, i)
			} else {
				locOwner[loc] = i
			}
		}
	}

	grouped := make(map[int][]plannedMove)
	var roots []int
	for i, mv := range moves {
		r := findDSURoot(parent, i)
		if _, exists := grouped[r]; !exists {
			roots = append(roots, r)
		}
		grouped[r] = append(grouped[r], mv)
	}

	components := make([][]plannedMove, 0, len(roots))
	for _, r := range roots {
		comp := grouped[r]
		slices.SortFunc(comp, func(a, b plannedMove) int {
			return cmp.Compare(a.Asset.ID, b.Asset.ID)
		})
		components = append(components, comp)
	}
	slices.SortFunc(components, func(a, b []plannedMove) int {
		return cmp.Compare(a[0].Asset.ID, b[0].Asset.ID)
	})
	return components
}

func (s *Scanner) executeMoves(
	ctx context.Context,
	libraryID string,
	components [][]plannedMove,
	res *ScanResult,
) error {
	for _, comp := range components {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.applyMoveComponent(ctx, libraryID, comp); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			for _, mv := range comp {
				res.recordReconcileSkipped(mv.TargetRootID, SkippedEntry{
					RelPath: mv.Inspected.RelPath,
					Reason:  SkipReasonIndexFailed,
					Err:     err,
				})
			}
			continue
		}
		for _, mv := range comp {
			res.recordReconcileMoved(mv.TargetRootID)
		}
	}
	return nil
}

type pendingMoveEvent struct {
	asset               domain.MediaAsset
	prevDisplay         string
	emitInvalidUTF8Path bool
}

func (s *Scanner) applyMoveComponent(ctx context.Context, libraryID string, comp []plannedMove) error {
	return s.tx.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		appender := tx.Events()
		if cw == nil || appender == nil {
			return errors.New("scanner: catalogue write and event repositories are required")
		}
		if len(comp) > 1 {
			if err := cw.DeferAssetPathUniqueness(ctx); err != nil {
				return err
			}
		}

		lockedAssets, err := lockComponentAssets(ctx, cw, comp)
		if err != nil {
			return err
		}

		now := s.now().UTC()
		eventsToEmit, err := applyComponentAssetUpdates(ctx, cw, comp, lockedAssets, now)
		if err != nil {
			return err
		}

		if len(comp) > 1 {
			if err := cw.CheckAssetPathUniqueness(ctx); err != nil {
				return err
			}
		}

		for _, ev := range eventsToEmit {
			if err := s.appendMoveEvents(ctx, appender, libraryID, ev.asset, ev.prevDisplay, ev.emitInvalidUTF8Path); err != nil {
				return err
			}
		}
		return nil
	})
}

func lockComponentAssets(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	comp []plannedMove,
) ([]domain.MediaAsset, error) {
	locked := make([]domain.MediaAsset, len(comp))
	for i, mv := range comp {
		fresh, err := cw.GetAssetByIDForUpdate(ctx, mv.Asset.ID)
		if err != nil {
			return nil, err
		}
		if fresh.MissingSince != nil ||
			fresh.LibraryRootID != mv.Asset.LibraryRootID ||
			!bytes.Equal(fresh.RelPathBytes, mv.Asset.RelPathBytes) {
			return nil, fmt.Errorf("scanner: asset %q changed concurrently before move", mv.Asset.ID)
		}
		locked[i] = *fresh
	}
	return locked, nil
}

func applyComponentAssetUpdates(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	comp []plannedMove,
	lockedAssets []domain.MediaAsset,
	now time.Time,
) ([]pendingMoveEvent, error) {
	eventsToEmit := make([]pendingMoveEvent, 0, len(comp))
	for i, mv := range comp {
		relPath, err := normalizeInspectedRelPath(mv.Inspected.RelPath)
		if err != nil {
			return nil, err
		}
		fresh := lockedAssets[i]
		prevDisplay := fresh.RelPathDisplay
		wasInvalidUTF8 := fresh.Diagnostics.InvalidUTF8Path

		fresh.LibraryRootID = mv.TargetRootID
		fresh.RelPathBytes = slices.Clone(relPath.Bytes)
		fresh.RelPathDisplay = relPath.Display
		fresh.RelPathMatch = relPath.Match
		fresh.FileSize = mv.Inspected.Size
		fresh.FileMtime = optionalNormalizedMtimePtr(mv.Inspected.Mtime)
		fresh.Diagnostics.InvalidUTF8Path = relPath.InvalidUTF8
		fresh.UpdatedAt = now

		if err := cw.UpdateAsset(ctx, &fresh); err != nil {
			return nil, err
		}
		eventsToEmit = append(eventsToEmit, pendingMoveEvent{
			asset:               fresh,
			prevDisplay:         prevDisplay,
			emitInvalidUTF8Path: !wasInvalidUTF8 && fresh.Diagnostics.InvalidUTF8Path,
		})
	}
	return eventsToEmit, nil
}

func (s *Scanner) appendMoveEvents(
	ctx context.Context,
	appender storage.EventRepository,
	libraryID string,
	asset domain.MediaAsset,
	prevDisplay string,
	emitInvalidUTF8 bool,
) error {
	movedPayload := domain.AssetEventPayloadV1{
		AssetID:             asset.ID,
		VolumeID:            asset.VolumeID,
		LibraryID:           libraryID,
		LibraryRootID:       asset.LibraryRootID,
		Format:              asset.Format,
		PathDisplay:         asset.RelPathDisplay,
		PreviousPathDisplay: prevDisplay,
	}
	if _, err := s.outbox.AppendPayload(
		ctx,
		appender,
		domain.EventAssetMoved,
		domain.EventVersion1,
		domain.AggregateTypeAsset,
		asset.ID,
		movedPayload,
	); err != nil {
		return err
	}
	if emitInvalidUTF8 {
		invalidPayload := domain.AssetEventPayloadV1{
			AssetID:       asset.ID,
			VolumeID:      asset.VolumeID,
			LibraryID:     libraryID,
			LibraryRootID: asset.LibraryRootID,
			Format:        asset.Format,
			PathDisplay:   asset.RelPathDisplay,
		}
		if _, err := s.outbox.AppendPayload(
			ctx,
			appender,
			domain.EventAssetInvalidUTF8Path,
			domain.EventVersion1,
			domain.AggregateTypeAsset,
			asset.ID,
			invalidPayload,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scanner) executeModifies(
	ctx context.Context,
	libraryID string,
	modifies []plannedModify,
	res *ScanResult,
) error {
	for _, mod := range modifies {
		if err := ctx.Err(); err != nil {
			return err
		}
		applied, err := s.applyExternalModify(ctx, libraryID, mod)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			res.recordReconcileSkipped(mod.RootID, SkippedEntry{
				RelPath: mod.Inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
			continue
		}
		if applied {
			res.recordReconcileModified(mod.RootID)
		}
	}
	return nil
}

func (s *Scanner) applyExternalModify(ctx context.Context, libraryID string, mod plannedModify) (bool, error) {
	relPath, err := normalizeInspectedRelPath(mod.Inspected.RelPath)
	if err != nil {
		return false, err
	}
	var applied bool
	err = s.tx.WithTx(ctx, func(tx storage.Tx) error {
		libRepo := tx.Libraries()
		cw := tx.CatalogueWrites()
		appender := tx.Events()
		if libRepo == nil || cw == nil || appender == nil {
			return errors.New("scanner: library, catalogue write, and event repositories are required")
		}

		lib, err := libRepo.GetLibraryByID(ctx, libraryID)
		if err != nil {
			return err
		}
		// Lock order: volumes -> media_assets -> series_memberships -> series -> people/credits/entities -> domain_events.
		vol, err := cw.GetVolumeByIDForUpdate(ctx, mod.Existing.VolumeID)
		if err != nil {
			return err
		}
		asset, err := cw.GetAssetByIDForUpdate(ctx, mod.Existing.ID)
		if err != nil {
			return err
		}
		if asset.MissingSince != nil ||
			asset.VolumeID != vol.ID ||
			asset.LibraryRootID != mod.RootID ||
			!bytes.Equal(asset.RelPathBytes, relPath.Bytes) {
			return fmt.Errorf("scanner: asset %q changed concurrently before modify", mod.Existing.ID)
		}
		if cur, ok := asset.CurrentDigest(mod.Inspected.DigestKind); ok && cur.Value == mod.Inspected.Digest {
			applied = false
			return nil
		}

		now := s.now().UTC()
		updatedAsset, err := s.persistExternalModifyAsset(ctx, cw, *asset, relPath, mod.Inspected, now)
		if err != nil {
			return err
		}
		if err := refreshModifiedCatalogue(ctx, tx, cw, lib, *vol, relPath, mod.Inspected, now); err != nil {
			return err
		}
		if _, err := s.outbox.AppendPayload(
			ctx,
			appender,
			domain.EventAssetModified,
			domain.EventVersion1,
			domain.AggregateTypeAsset,
			updatedAsset.ID,
			domain.AssetEventPayloadV1{
				AssetID:       updatedAsset.ID,
				VolumeID:      updatedAsset.VolumeID,
				LibraryID:     lib.ID,
				LibraryRootID: updatedAsset.LibraryRootID,
				Format:        updatedAsset.Format,
				PathDisplay:   updatedAsset.RelPathDisplay,
			},
		); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (s *Scanner) persistExternalModifyAsset(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	asset domain.MediaAsset,
	relPath domain.PathIdentity,
	inspected InspectedFile,
	now time.Time,
) (domain.MediaAsset, error) {
	newDigest := domain.AssetDigest{
		AssetID:    asset.ID,
		Kind:       inspected.DigestKind,
		Value:      inspected.Digest,
		IsCurrent:  true,
		Reason:     domain.DigestReasonExternalModify,
		RecordedAt: now,
	}
	if err := cw.RecordAssetDigest(ctx, &newDigest); err != nil {
		return domain.MediaAsset{}, err
	}

	updated := asset
	updated.FileSize = inspected.Size
	updated.FileMtime = optionalNormalizedMtimePtr(inspected.Mtime)
	updated.Format = inspected.Format
	updated.PageCount = inspected.PageCount
	updated.CoverPageIndex = inspected.CoverPageIndex
	updated.SuppressedPages = slices.Clone(inspected.SuppressedPages)
	updated.Diagnostics.ImageCodecs = slices.Clone(inspected.ImageCodecs)
	updated.Diagnostics.InvalidUTF8Path = relPath.InvalidUTF8
	updated.UpdatedAt = now
	if err := cw.UpdateAssetContent(ctx, &updated); err != nil {
		return domain.MediaAsset{}, err
	}
	return updated, nil
}

func refreshModifiedCatalogue(
	ctx context.Context,
	tx storage.Tx,
	cw storage.CatalogueWriteRepository,
	lib *domain.Library,
	vol domain.Volume,
	relPath domain.PathIdentity,
	inspected InspectedFile,
	now time.Time,
) error {
	res, err := resolver.Resolve(inspected.ComicInfo, relPath, lib.ResolverChain)
	if err != nil {
		return err
	}
	res.Number = normalizeZeroFloatPtr(res.Number)

	proposedVol := buildProposedVolume(lib.ID, res, inspected, now)
	if _, _, err := updateMatchedVolume(ctx, cw, vol, proposedVol, now); err != nil {
		return err
	}

	if err := reconcileModifiedSeriesAndMemberships(ctx, tx, cw, lib.ID, vol.ID, res, inspected.ComicInfo, now); err != nil {
		return err
	}

	if inspected.ComicInfo != nil {
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, *inspected.ComicInfo)
	}
	if inspected.ComicInfoErr == nil {
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, comicinfo.Metadata{})
	}
	return nil
}

func reconcileModifiedSeriesAndMemberships(
	ctx context.Context,
	tx storage.Tx,
	cw storage.CatalogueWriteRepository,
	libraryID, volumeID string,
	res resolver.Result,
	meta *comicinfo.Metadata,
	now time.Time,
) error {
	membershipRepo := tx.SeriesMemberships()
	if membershipRepo == nil {
		return errors.New("scanner: series membership repository is required")
	}
	existingMemberships, err := membershipRepo.ListMembershipsByVolume(ctx, volumeID)
	if err != nil {
		return err
	}

	var existingPrimary *domain.SeriesMembership
	for i := range existingMemberships {
		if existingMemberships[i].IsPrimary {
			m := existingMemberships[i]
			existingPrimary = &m
			break
		}
	}

	if existingPrimary != nil && isImmutableModifyPrimary(*existingPrimary) {
		return reconcileImmutableModifyPrimary(ctx, tx, cw, libraryID, volumeID, *existingPrimary, res, meta, now)
	}

	series, proposedSeries, seriesCreated, err := resolveOrCreateSeries(ctx, cw, libraryID, res, meta, now)
	if err != nil {
		return err
	}
	var seriesID string
	if series != nil {
		seriesID = series.ID
	}
	if _, err := syncIndexedVolumeMemberships(ctx, tx, volumeID, seriesID, res, false); err != nil {
		return err
	}
	if !seriesCreated && series != nil {
		if _, err := syncExistingSeries(ctx, cw, series.ID, proposedSeries, now); err != nil {
			return err
		}
	}
	return nil
}

func reconcileImmutableModifyPrimary(
	ctx context.Context,
	tx storage.Tx,
	cw storage.CatalogueWriteRepository,
	libraryID, volumeID string,
	primary domain.SeriesMembership,
	res resolver.Result,
	meta *comicinfo.Metadata,
	now time.Time,
) error {
	existingSeries, err := cw.GetSeriesByID(ctx, primary.SeriesID)
	if err != nil {
		return err
	}
	sameSeries := !res.Standalone && res.Series != nil &&
		existingSeries.ResolverKey != nil &&
		*existingSeries.ResolverKey == res.Series.Key
	if !sameSeries {
		return tx.SeriesMemberships().SyncResolverMemberships(ctx, volumeID, nil)
	}

	if _, err := syncIndexedVolumeMemberships(ctx, tx, volumeID, existingSeries.ID, res, false); err != nil {
		return err
	}
	proposedSeries := buildProposedSeries(libraryID, *res.Series, meta, now)
	_, err = syncExistingSeries(ctx, cw, existingSeries.ID, proposedSeries, now)
	return err
}

func isImmutableModifyPrimary(m domain.SeriesMembership) bool {
	if !m.IsPrimary {
		return false
	}
	if m.Source == domain.MembershipSourceManual {
		return true
	}
	return m.LockedFields.IsLocked("*") ||
		m.LockedFields.IsLocked("is_primary") ||
		m.LockedFields.IsLocked("membership") ||
		m.LockedFields.IsLocked("series_id")
}

func (s *Scanner) executeMtimeRefreshes(ctx context.Context, refreshes []plannedModify) error {
	for _, ref := range refreshes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.applyMtimeRefresh(ctx, ref); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			// An mtime refresh is an optimization only; skip silently on transient mismatch.
			continue
		}
	}
	return nil
}

func (s *Scanner) applyMtimeRefresh(ctx context.Context, ref plannedModify) error {
	return s.tx.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		if cw == nil {
			return errors.New("scanner: catalogue write repository is required")
		}
		asset, err := cw.GetAssetByIDForUpdate(ctx, ref.Existing.ID)
		if err != nil {
			return err
		}
		if asset.MissingSince != nil ||
			asset.LibraryRootID != ref.RootID ||
			!bytes.Equal(asset.RelPathBytes, ref.Inspected.RelPath.Bytes) {
			return nil
		}
		cur, ok := asset.CurrentDigest(ref.Inspected.DigestKind)
		if !ok || cur.Value != ref.Inspected.Digest {
			return nil
		}

		updated := *asset
		updated.FileSize = ref.Inspected.Size
		updated.FileMtime = optionalNormalizedMtimePtr(ref.Inspected.Mtime)
		updated.UpdatedAt = s.now().UTC()
		return cw.UpdateAssetContent(ctx, &updated)
	})
}

func (s *Scanner) executeDeferredIndexes(
	ctx context.Context,
	libraryID string,
	toIndex []observedFile,
	res *ScanResult,
) error {
	for _, item := range toIndex {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := s.indexer.IndexFile(ctx, IndexFileInput{
			LibraryID:     libraryID,
			LibraryRootID: item.rootID,
			Inspected:     item.inspected,
		})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			res.recordReconcileSkipped(item.rootID, SkippedEntry{
				RelPath: item.inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
			continue
		}
		res.recordReconcileIndexed(item.rootID)
	}
	return nil
}

func assetMatchesDigestTier(asset domain.MediaAsset, kind domain.DigestKind, value string, currentOnly bool) bool {
	for _, d := range asset.Digests {
		if d.Kind == kind && d.Value == value && d.IsCurrent == currentOnly {
			return true
		}
	}
	return false
}

func compareCandidateObservation(a, b candidateObservation) int {
	if c := cmp.Compare(a.rootID, b.rootID); c != 0 {
		return c
	}
	return bytes.Compare(a.inspected.RelPath.Bytes, b.inspected.RelPath.Bytes)
}

func compareAssetOrderForReconcile(a, b domain.MediaAsset) int {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		return 1
	}
	return cmp.Compare(a.ID, b.ID)
}

func optionalNormalizedMtimePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	mt := normalizeMtime(t)
	return &mt
}

func equalOptionalNormalizedMtime(stored *time.Time, inspected time.Time) bool {
	if inspected.IsZero() {
		return stored == nil
	}
	if stored == nil {
		return false
	}
	return normalizeMtime(*stored).Equal(normalizeMtime(inspected))
}
