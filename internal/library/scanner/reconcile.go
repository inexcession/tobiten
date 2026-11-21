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

// ErrStaleAssetState is returned when a media asset's database state changes
// concurrently between scan planning and a reconciliation write transaction.
var ErrStaleAssetState = errors.New("scanner: media asset state changed concurrently")

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

type libraryWalkObservations struct {
	seenUnchanged map[assetLocation]struct{}
	seenSkipped   map[assetLocation]struct{}
	newFiles      []observedFile
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

func (p reconciliationPlan) hasWrites() bool {
	return len(p.MoveComponents) > 0 ||
		len(p.Modifies) > 0 ||
		len(p.MtimeRefreshes) > 0 ||
		len(p.ToIndex) > 0
}

func lockAndVerifyRootSnapshot(
	ctx context.Context,
	tx storage.Tx,
	libraryID string,
	checkedRoots []RootCheckResult,
) error {
	libRepo := tx.Libraries()
	if libRepo == nil {
		return errors.New("scanner: library repository is required")
	}
	if err := libRepo.LockLibrary(ctx, libraryID); errors.Is(err, storage.ErrNotFound) {
		return ErrStalePreCheck
	} else if err != nil {
		return err
	}
	currentRoots, err := libRepo.ListLibraryRootsForNoKeyUpdate(ctx, libraryID)
	if err != nil {
		return err
	}
	if !rootSnapshotPathsMatch(checkedRoots, currentRoots) || !allRootsOnline(currentRoots) {
		return ErrStalePreCheck
	}
	return nil
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

	plan := planReconciliation(res.Roots, preWalkAssets, obs)
	if !plan.hasWrites() {
		return s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
			return lockAndVerifyRootSnapshot(ctx, tx, libraryID, checkedRoots)
		})
	}

	var writeErrs []error
	if err := s.executeMoves(ctx, libraryID, checkedRoots, plan.MoveComponents, res); err != nil {
		if errors.Is(err, ErrStalePreCheck) || ctx.Err() != nil {
			return err
		}
		writeErrs = append(writeErrs, err)
	}
	if err := s.executeModifies(ctx, libraryID, checkedRoots, plan.Modifies, res); err != nil {
		if errors.Is(err, ErrStalePreCheck) || ctx.Err() != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		writeErrs = append(writeErrs, err)
	}
	if err := s.executeMtimeRefreshes(ctx, libraryID, checkedRoots, plan.MtimeRefreshes, res); err != nil {
		if errors.Is(err, ErrStalePreCheck) || ctx.Err() != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		writeErrs = append(writeErrs, err)
	}
	if err := s.executeDeferredIndexes(ctx, libraryID, checkedRoots, plan.ToIndex, res); err != nil {
		return errors.Join(append(writeErrs, err)...)
	}
	return errors.Join(writeErrs...)
}

func planReconciliation(
	roots []RootScanResult,
	preWalkAssets []domain.MediaAsset,
	obs *libraryWalkObservations,
) reconciliationPlan {
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

	confirmedInPlace, mtimeRefreshes, candidates := collectReconciliationObservations(roots, obs)
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

func collectReconciliationObservations(
	roots []RootScanResult,
	obs *libraryWalkObservations,
) (map[string]struct{}, []plannedModify, []candidateObservation) {
	confirmedInPlace := make(map[string]struct{})
	var mtimeRefreshes []plannedModify
	var candidates []candidateObservation

	for _, rootRes := range roots {
		for _, ch := range rootRes.Changed {
			if ch.Existing.MissingSince != nil {
				continue
			}
			loc := assetLocation{
				rootID:       rootRes.LibraryRootID,
				relPathBytes: string(ch.Inspected.RelPath.Bytes),
			}
			if cur, ok := ch.Existing.CurrentDigest(ch.Inspected.DigestKind); ok && cur.Value == ch.Inspected.Digest {
				confirmedInPlace[ch.Existing.ID] = struct{}{}
				if ch.Existing.FileSize != ch.Inspected.Size || !equalOptionalNormalizedMtime(ch.Existing.FileMtime, ch.Inspected.Mtime) {
					mtimeRefreshes = append(mtimeRefreshes, plannedModify{
						Existing:  ch.Existing,
						RootID:    rootRes.LibraryRootID,
						Inspected: ch.Inspected,
					})
				}
				continue
			}
			candidates = append(candidates, candidateObservation{
				loc:       loc,
				rootID:    rootRes.LibraryRootID,
				inspected: ch.Inspected,
			})
		}
	}

	for _, f := range obs.newFiles {
		candidates = append(candidates, candidateObservation{
			loc: assetLocation{
				rootID:       f.rootID,
				relPathBytes: string(f.inspected.RelPath.Bytes),
			},
			rootID:    f.rootID,
			inspected: f.inspected,
		})
	}
	return confirmedInPlace, mtimeRefreshes, candidates
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

type moveDigestIndex struct {
	unconfirmedLocs    []assetLocation
	samePathHistorical []bool
	currentBuckets     map[digestKey][]int
	historicalBuckets  map[digestKey][]int
}

func buildMoveDigestIndex(unconfirmed []domain.MediaAsset, candidates []candidateObservation) moveDigestIndex {
	candByLoc := make(map[assetLocation]int, len(candidates))
	for obsIdx, cand := range candidates {
		candByLoc[cand.loc] = obsIdx
	}

	idx := moveDigestIndex{
		unconfirmedLocs:    make([]assetLocation, len(unconfirmed)),
		samePathHistorical: make([]bool, len(unconfirmed)),
		currentBuckets:     make(map[digestKey][]int, len(unconfirmed)),
		historicalBuckets:  make(map[digestKey][]int),
	}
	for i, asset := range unconfirmed {
		loc := assetLocation{
			rootID:       asset.LibraryRootID,
			relPathBytes: string(asset.RelPathBytes),
		}
		idx.unconfirmedLocs[i] = loc
		if obsIdx, ok := candByLoc[loc]; ok {
			cand := candidates[obsIdx]
			for _, d := range asset.Digests {
				if !d.IsCurrent && d.Kind == cand.inspected.DigestKind && d.Value == cand.inspected.Digest {
					idx.samePathHistorical[i] = true
					break
				}
			}
		}
		for _, d := range asset.Digests {
			key := digestKey{kind: d.Kind, value: d.Value}
			if d.IsCurrent {
				bucket := idx.currentBuckets[key]
				if len(bucket) == 0 || bucket[len(bucket)-1] != i {
					idx.currentBuckets[key] = append(bucket, i)
				}
				continue
			}
			bucket := idx.historicalBuckets[key]
			if len(bucket) == 0 || bucket[len(bucket)-1] != i {
				idx.historicalBuckets[key] = append(bucket, i)
			}
		}
	}
	return idx
}

func matchMovesFixedPoint(
	unconfirmed []domain.MediaAsset,
	candidates []candidateObservation,
	existingByLoc map[assetLocation]domain.MediaAsset,
) (map[string]plannedMove, []bool) {
	index := buildMoveDigestIndex(unconfirmed, candidates)
	blockedDest := make(map[assetLocation]struct{})
	for {
		proposedMoves, matchedObs := matchCandidateMoves(unconfirmed, candidates, index, blockedDest)
		newlyBlocked := findNewlyBlockedDestinations(proposedMoves, existingByLoc, blockedDest)
		if !newlyBlocked {
			return proposedMoves, matchedObs
		}
	}
}

func matchCandidateMoves(
	unconfirmed []domain.MediaAsset,
	candidates []candidateObservation,
	index moveDigestIndex,
	blockedDest map[assetLocation]struct{},
) (map[string]plannedMove, []bool) {
	claimed := make([]bool, len(unconfirmed))
	matchedObs := make([]bool, len(candidates))
	proposed := make(map[string]plannedMove)

	matchPass := func(buckets map[digestKey][]int) {
		cursors := make(map[digestKey]int, len(buckets))
		for obsIdx, cand := range candidates {
			if matchedObs[obsIdx] {
				continue
			}
			if _, blocked := blockedDest[cand.loc]; blocked {
				continue
			}
			key := digestKey{kind: cand.inspected.DigestKind, value: cand.inspected.Digest}
			bucket := buckets[key]
			pos := cursors[key]
			for pos < len(bucket) && claimed[bucket[pos]] {
				pos++
			}
			cursors[key] = pos

			for scan := pos; scan < len(bucket); scan++ {
				assetIdx := bucket[scan]
				if claimed[assetIdx] || index.unconfirmedLocs[assetIdx] == cand.loc {
					continue
				}
				asset := unconfirmed[assetIdx]
				claimed[assetIdx] = true
				matchedObs[obsIdx] = true
				proposed[asset.ID] = plannedMove{
					Asset:        asset,
					TargetRootID: cand.rootID,
					Inspected:    cand.inspected,
				}
				if scan == pos {
					cursors[key] = pos + 1
				}
				break
			}
		}
	}

	matchPass(index.currentBuckets)
	// When no current-digest move explains an unconfirmed asset's departure,
	// keep a same-path historical observation in place as an external modify
	// rather than letting a copy of that historical digest steal the asset ID.
	for assetIdx, hasHist := range index.samePathHistorical {
		if hasHist && !claimed[assetIdx] {
			claimed[assetIdx] = true
		}
	}
	matchPass(index.historicalBuckets)
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
	matchedObs []bool,
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
	checkedRoots []RootCheckResult,
	components [][]plannedMove,
	res *ScanResult,
) error {
	var writeErrs []error
	for _, comp := range components {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.applyMoveComponent(ctx, libraryID, checkedRoots, comp); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(append(writeErrs, ctxErr)...)
			}
			if errors.Is(err, ErrStalePreCheck) {
				return errors.Join(append(writeErrs, err)...)
			}
			for _, mv := range comp {
				res.recordReconcileSkipped(mv.TargetRootID, SkippedEntry{
					RelPath: mv.Inspected.RelPath,
					Reason:  SkipReasonIndexFailed,
					Err:     err,
				})
			}
			writeErrs = append(writeErrs, err)
			continue
		}
		for _, mv := range comp {
			res.recordReconcileMoved(mv.TargetRootID)
		}
	}
	return errors.Join(writeErrs...)
}

type pendingMoveEvent struct {
	asset               domain.MediaAsset
	prevDisplay         string
	emitInvalidUTF8Path bool
}

func (s *Scanner) applyMoveComponent(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	comp []plannedMove,
) error {
	return s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		// Global lock order:
		// libraries (FOR UPDATE) -> library_roots (id ASC, FOR NO KEY UPDATE) ->
		// volumes (id ASC, FOR UPDATE) -> media_assets (id ASC, FOR UPDATE).
		if err := lockAndVerifyRootSnapshot(ctx, tx, libraryID, checkedRoots); err != nil {
			return err
		}

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

		lockedVolumes, err := lockComponentVolumes(ctx, cw, comp)
		if err != nil {
			return err
		}
		lockedAssets, err := lockComponentAssets(ctx, cw, comp)
		if err != nil {
			return err
		}

		now := s.now().UTC()
		eventsToEmit, err := applyComponentAssetUpdates(ctx, cw, comp, lockedVolumes, lockedAssets, now)
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

func lockComponentVolumes(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	comp []plannedMove,
) (map[string]domain.Volume, error) {
	seen := make(map[string]struct{}, len(comp))
	volIDs := make([]string, 0, len(comp))
	for _, mv := range comp {
		if _, ok := seen[mv.Asset.VolumeID]; ok {
			continue
		}
		seen[mv.Asset.VolumeID] = struct{}{}
		volIDs = append(volIDs, mv.Asset.VolumeID)
	}
	slices.Sort(volIDs)

	volumes := make(map[string]domain.Volume, len(volIDs))
	for _, volID := range volIDs {
		vol, err := cw.GetVolumeByIDForUpdate(ctx, volID)
		if err != nil {
			return nil, err
		}
		volumes[volID] = *vol
	}
	return volumes, nil
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
		if err := verifyLockedAssetSnapshot(*fresh, mv.Asset, "move"); err != nil {
			return nil, err
		}
		locked[i] = *fresh
	}
	return locked, nil
}

func verifyLockedAssetSnapshot(fresh, expected domain.MediaAsset, action string) error {
	if fresh.MissingSince != nil ||
		fresh.VolumeID != expected.VolumeID ||
		fresh.LibraryRootID != expected.LibraryRootID ||
		!bytes.Equal(fresh.RelPathBytes, expected.RelPathBytes) ||
		fresh.FileSize != expected.FileSize ||
		!equalOptionalTimePtr(fresh.FileMtime, expected.FileMtime) ||
		!fresh.UpdatedAt.Equal(expected.UpdatedAt) {
		return fmt.Errorf("%w: asset %q changed concurrently before %s", ErrStaleAssetState, expected.ID, action)
	}
	return nil
}

func applyComponentAssetUpdates(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	comp []plannedMove,
	lockedVolumes map[string]domain.Volume,
	lockedAssets []domain.MediaAsset,
	now time.Time,
) ([]pendingMoveEvent, error) {
	eventsToEmit := make([]pendingMoveEvent, 0, len(comp))
	for i, mv := range comp {
		ev, err := applyMoveToAsset(ctx, cw, mv, lockedVolumes, lockedAssets[i], now)
		if err != nil {
			return nil, err
		}
		eventsToEmit = append(eventsToEmit, ev)
	}
	return eventsToEmit, nil
}

func applyMoveToAsset(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	mv plannedMove,
	lockedVolumes map[string]domain.Volume,
	fresh domain.MediaAsset,
	now time.Time,
) (pendingMoveEvent, error) {
	relPath, err := normalizeInspectedRelPath(mv.Inspected.RelPath)
	if err != nil {
		return pendingMoveEvent{}, err
	}
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

	cur, hasCur := fresh.CurrentDigest(mv.Inspected.DigestKind)
	if !hasCur || cur.Value != mv.Inspected.Digest {
		if err := promoteHistoricalMoveDigest(ctx, cw, &fresh, lockedVolumes, mv.Inspected, now); err != nil {
			return pendingMoveEvent{}, err
		}
	}

	if err := cw.UpdateAsset(ctx, &fresh); err != nil {
		return pendingMoveEvent{}, err
	}
	return pendingMoveEvent{
		asset:               fresh,
		prevDisplay:         prevDisplay,
		emitInvalidUTF8Path: !wasInvalidUTF8 && fresh.Diagnostics.InvalidUTF8Path,
	}, nil
}

func promoteHistoricalMoveDigest(
	ctx context.Context,
	cw storage.CatalogueWriteRepository,
	fresh *domain.MediaAsset,
	lockedVolumes map[string]domain.Volume,
	inspected InspectedFile,
	now time.Time,
) error {
	promoted := domain.AssetDigest{
		AssetID:    fresh.ID,
		Kind:       inspected.DigestKind,
		Value:      inspected.Digest,
		IsCurrent:  true,
		Reason:     domain.DigestReasonScan,
		RecordedAt: now,
	}
	if err := cw.RecordAssetDigest(ctx, &promoted); err != nil {
		return err
	}
	fresh.Format = inspected.Format
	fresh.PageCount = inspected.PageCount
	fresh.CoverPageIndex = inspected.CoverPageIndex
	fresh.SuppressedPages = slices.Clone(inspected.SuppressedPages)
	fresh.Diagnostics.ImageCodecs = slices.Clone(inspected.ImageCodecs)

	vol := lockedVolumes[fresh.VolumeID]
	if shouldSyncMovedVolumePageCount(vol, fresh.ID, inspected.PageCount) {
		vol.PageCount = inspected.PageCount
		vol.UpdatedAt = now
		if err := cw.UpdateVolume(ctx, &vol); err != nil {
			return err
		}
		lockedVolumes[fresh.VolumeID] = vol
	}
	return nil
}

func shouldSyncMovedVolumePageCount(vol domain.Volume, assetID string, pageCount int) bool {
	if vol.PreferredAssetID != nil && *vol.PreferredAssetID != assetID {
		return false
	}
	if vol.LockedFields.IsLocked("*") || vol.LockedFields.IsLocked("page_count") {
		return false
	}
	return vol.PageCount != pageCount
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
	checkedRoots []RootCheckResult,
	modifies []plannedModify,
	res *ScanResult,
) error {
	var writeErrs []error
	for _, mod := range modifies {
		if err := ctx.Err(); err != nil {
			return err
		}
		applied, err := s.applyExternalModify(ctx, libraryID, checkedRoots, mod)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(append(writeErrs, ctxErr)...)
			}
			if errors.Is(err, ErrStalePreCheck) {
				return errors.Join(append(writeErrs, err)...)
			}
			res.recordReconcileSkipped(mod.RootID, SkippedEntry{
				RelPath: mod.Inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
			writeErrs = append(writeErrs, err)
			continue
		}
		if applied {
			res.recordReconcileModified(mod.RootID)
		}
	}
	return errors.Join(writeErrs...)
}

func (s *Scanner) applyExternalModify(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	mod plannedModify,
) (bool, error) {
	relPath, err := normalizeInspectedRelPath(mod.Inspected.RelPath)
	if err != nil {
		return false, err
	}
	var applied bool
	err = s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		// Global lock order:
		// libraries (FOR UPDATE) -> library_roots (id ASC, FOR NO KEY UPDATE) ->
		// volumes (FOR UPDATE) -> media_assets (FOR UPDATE) -> series_memberships ->
		// series -> people/credits/entities -> domain_events.
		if err := lockAndVerifyRootSnapshot(ctx, tx, libraryID, checkedRoots); err != nil {
			return err
		}

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
		vol, err := cw.GetVolumeByIDForUpdate(ctx, mod.Existing.VolumeID)
		if err != nil {
			return err
		}
		asset, err := cw.GetAssetByIDForUpdate(ctx, mod.Existing.ID)
		if err != nil {
			return err
		}
		if err := verifyLockedAssetSnapshot(*asset, mod.Existing, "modify"); err != nil {
			return err
		}
		if asset.VolumeID != vol.ID || asset.LibraryRootID != mod.RootID || !bytes.Equal(asset.RelPathBytes, relPath.Bytes) {
			return fmt.Errorf("%w: asset %q changed concurrently before modify", ErrStaleAssetState, mod.Existing.ID)
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

func (s *Scanner) executeMtimeRefreshes(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	refreshes []plannedModify,
	res *ScanResult,
) error {
	var writeErrs []error
	for _, ref := range refreshes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.applyMtimeRefresh(ctx, libraryID, checkedRoots, ref); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(append(writeErrs, ctxErr)...)
			}
			if errors.Is(err, ErrStalePreCheck) {
				return errors.Join(append(writeErrs, err)...)
			}
			res.recordReconcileSkipped(ref.RootID, SkippedEntry{
				RelPath: ref.Inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
			writeErrs = append(writeErrs, err)
			continue
		}
	}
	return errors.Join(writeErrs...)
}

func (s *Scanner) applyMtimeRefresh(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	ref plannedModify,
) error {
	return s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		if err := lockAndVerifyRootSnapshot(ctx, tx, libraryID, checkedRoots); err != nil {
			return err
		}
		cw := tx.CatalogueWrites()
		if cw == nil {
			return errors.New("scanner: catalogue write repository is required")
		}
		// Maintain volumes -> media_assets lock order with moves and modifies.
		if _, err := cw.GetVolumeByIDForUpdate(ctx, ref.Existing.VolumeID); err != nil {
			return err
		}
		asset, err := cw.GetAssetByIDForUpdate(ctx, ref.Existing.ID)
		if err != nil {
			return err
		}
		if err := verifyLockedAssetSnapshot(*asset, ref.Existing, "mtime refresh"); err != nil {
			return err
		}
		if asset.LibraryRootID != ref.RootID || !bytes.Equal(asset.RelPathBytes, ref.Inspected.RelPath.Bytes) {
			return fmt.Errorf("%w: asset %q changed concurrently before mtime refresh", ErrStaleAssetState, ref.Existing.ID)
		}
		cur, ok := asset.CurrentDigest(ref.Inspected.DigestKind)
		if !ok || cur.Value != ref.Inspected.Digest {
			return fmt.Errorf("%w: asset %q digest changed concurrently before mtime refresh", ErrStaleAssetState, ref.Existing.ID)
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
	checkedRoots []RootCheckResult,
	toIndex []observedFile,
	res *ScanResult,
) error {
	for _, item := range toIndex {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := s.indexer.indexFileWithRootSnapshot(ctx, IndexFileInput{
			LibraryID:     libraryID,
			LibraryRootID: item.rootID,
			Inspected:     item.inspected,
		}, checkedRoots)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, ErrStalePreCheck) {
				return err
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

func equalOptionalTimePtr(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}
