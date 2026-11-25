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

type plannedRestore struct {
	Existing  domain.MediaAsset
	RootID    string
	Inspected InspectedFile
}

type plannedAssetTransition struct {
	Existing domain.MediaAsset
	RootID   string
}

type reconciliationPlan struct {
	MoveComponents               [][]plannedMove
	Modifies                     []plannedModify
	Restores                     []plannedRestore
	MtimeRefreshes               []plannedModify
	ToIndex                      []observedFile
	MarkMissing                  []plannedAssetTransition
	RetireMissing                []plannedAssetTransition
	UnconfirmedRootsWithRemovals map[string]struct{}
}

func (p reconciliationPlan) hasWrites() bool {
	return len(p.MoveComponents) > 0 ||
		len(p.Modifies) > 0 ||
		len(p.Restores) > 0 ||
		len(p.MtimeRefreshes) > 0 ||
		len(p.ToIndex) > 0 ||
		len(p.MarkMissing) > 0 ||
		len(p.RetireMissing) > 0
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
	if !rootSnapshotMatches(checkedRoots, currentRoots) || !allRootsOnline(currentRoots) {
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
	gracePeriod time.Duration,
	probeOpts PreCheckOptions,
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkScanCancel(ctx, checkCancel); err != nil {
		return err
	}

	reconcileNow := s.now().UTC().Truncate(time.Microsecond)
	plan := planReconciliation(res.Roots, preWalkAssets, obs, reconcileNow, gracePeriod)
	if !plan.hasWrites() {
		return s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
			return lockAndVerifyRootSnapshot(ctx, tx, libraryID, checkedRoots)
		})
	}

	if len(plan.UnconfirmedRootsWithRemovals) > 0 {
		if err := s.verifyCheckedRootsAvailable(ctx, checkedRoots, probeOpts); err != nil {
			return err
		}
		markRootsIncomplete(res, plan.UnconfirmedRootsWithRemovals)
		return nil
	}

	var writeErrs []error
	for _, step := range []func() error{
		func() error {
			return s.executeMoves(ctx, libraryID, checkedRoots, plan.MoveComponents, checkCancel, res)
		},
		func() error {
			return s.executeModifies(ctx, libraryID, checkedRoots, plan.Modifies, checkCancel, res)
		},
		func() error {
			return s.executeRestores(ctx, libraryID, checkedRoots, plan.Restores, checkCancel, res)
		},
		func() error {
			return s.executeMtimeRefreshes(ctx, libraryID, checkedRoots, plan.MtimeRefreshes, checkCancel, res)
		},
	} {
		if err := step(); err != nil {
			if errors.Is(err, ErrStalePreCheck) || isScanAbortErr(ctx, err) {
				return errors.Join(append(writeErrs, err)...)
			}
			writeErrs = append(writeErrs, err)
		}
	}
	if err := s.executeDeferredIndexes(ctx, libraryID, checkedRoots, plan.ToIndex, checkCancel, res); err != nil {
		return errors.Join(append(writeErrs, err)...)
	}
	if len(writeErrs) > 0 {
		if err := checkScanCancel(ctx, checkCancel); err != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		return errors.Join(writeErrs...)
	}

	if err := s.verifyCheckedRootsAvailable(ctx, checkedRoots, probeOpts); err != nil {
		return err
	}
	if err := s.executeMarkMissing(ctx, libraryID, checkedRoots, plan.MarkMissing, reconcileNow, checkCancel, res); err != nil {
		if errors.Is(err, ErrStalePreCheck) || isScanAbortErr(ctx, err) {
			return err
		}
		writeErrs = append(writeErrs, err)
	}
	if err := s.executeRetireMissing(ctx, libraryID, checkedRoots, plan.RetireMissing, gracePeriod, reconcileNow, checkCancel, res); err != nil {
		if errors.Is(err, ErrStalePreCheck) || isScanAbortErr(ctx, err) {
			return errors.Join(append(writeErrs, err)...)
		}
		writeErrs = append(writeErrs, err)
	}
	if len(writeErrs) > 0 {
		if err := checkScanCancel(ctx, checkCancel); err != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		return errors.Join(writeErrs...)
	}
	return checkScanCancel(ctx, checkCancel)
}

func planReconciliation(
	roots []RootScanResult,
	preWalkAssets []domain.MediaAsset,
	obs *libraryWalkObservations,
	now time.Time,
	grace time.Duration,
) reconciliationPlan {
	existingByLoc := make(map[assetLocation]domain.MediaAsset, len(preWalkAssets))
	activeAssets := make([]domain.MediaAsset, 0, len(preWalkAssets))
	missingAssets := make([]domain.MediaAsset, 0)
	for _, asset := range preWalkAssets {
		loc := assetLocation{
			rootID:       asset.LibraryRootID,
			relPathBytes: string(asset.RelPathBytes),
		}
		existingByLoc[loc] = asset
		if asset.MissingSince == nil {
			activeAssets = append(activeAssets, asset)
		} else {
			missingAssets = append(missingAssets, asset)
		}
	}

	confirmedInPlace, mtimeRefreshes, samePathRestores, candidates := collectReconciliationObservations(roots, obs)
	unconfirmedActive := collectUnconfirmedAssets(activeAssets, obs, confirmedInPlace)
	unconfirmedMissing := collectUnconfirmedAssets(missingAssets, obs, confirmedInPlace)
	unconfirmed := make([]domain.MediaAsset, 0, len(unconfirmedActive)+len(unconfirmedMissing))
	unconfirmed = append(unconfirmed, unconfirmedActive...)
	unconfirmed = append(unconfirmed, unconfirmedMissing...)
	slices.SortFunc(candidates, compareCandidateObservation)

	proposedMoves, matchedObs := matchMovesFixedPoint(unconfirmed, candidates, existingByLoc)
	modifies, histRestores, toIndex := classifyUnmatchedCandidates(candidates, matchedObs, existingByLoc, proposedMoves)
	samePathRestores = append(samePathRestores, histRestores...)
	slices.SortFunc(samePathRestores, func(a, b plannedRestore) int {
		return cmp.Compare(a.Existing.ID, b.Existing.ID)
	})

	markMissing, retireMissing := collectMissingAndRetiredTransitions(
		unconfirmedActive,
		unconfirmedMissing,
		proposedMoves,
		modifies,
		samePathRestores,
		now,
		grace,
	)

	unconfirmedRoots := findRootsWithZeroConfirmedKnownAssets(
		roots,
		activeAssets,
		unconfirmedActive,
		proposedMoves,
		modifies,
		markMissing,
	)

	moves := make([]plannedMove, 0, len(proposedMoves))
	for _, mv := range proposedMoves {
		moves = append(moves, mv)
	}
	slices.SortFunc(moves, func(a, b plannedMove) int {
		return cmp.Compare(a.Asset.ID, b.Asset.ID)
	})

	return reconciliationPlan{
		MoveComponents:               partitionMoveComponents(moves),
		Modifies:                     modifies,
		Restores:                     samePathRestores,
		MtimeRefreshes:               mtimeRefreshes,
		ToIndex:                      toIndex,
		MarkMissing:                  markMissing,
		RetireMissing:                retireMissing,
		UnconfirmedRootsWithRemovals: unconfirmedRoots,
	}
}

func findRootsWithZeroConfirmedKnownAssets(
	roots []RootScanResult,
	activeAssets []domain.MediaAsset,
	unconfirmedActive []domain.MediaAsset,
	proposedMoves map[string]plannedMove,
	modifies []plannedModify,
	markMissing []plannedAssetTransition,
) map[string]struct{} {
	if len(markMissing) == 0 {
		return nil
	}
	missingByRoot := make(map[string]int, len(roots))
	for _, item := range markMissing {
		missingByRoot[item.RootID]++
	}

	unconfirmedSet := make(map[string]struct{}, len(unconfirmedActive))
	for _, asset := range unconfirmedActive {
		unconfirmedSet[asset.ID] = struct{}{}
	}

	confirmedInRoot := make(map[string]int, len(roots))
	for _, asset := range activeAssets {
		if _, unconfirmed := unconfirmedSet[asset.ID]; !unconfirmed {
			confirmedInRoot[asset.LibraryRootID]++
		}
	}
	for _, mod := range modifies {
		if mod.Existing.MissingSince == nil && mod.RootID == mod.Existing.LibraryRootID {
			confirmedInRoot[mod.RootID]++
		}
	}
	for _, mv := range proposedMoves {
		if mv.Asset.MissingSince == nil && mv.TargetRootID == mv.Asset.LibraryRootID {
			confirmedInRoot[mv.TargetRootID]++
		}
	}

	var unconfirmedRoots map[string]struct{}
	for _, rootRes := range roots {
		if missingByRoot[rootRes.LibraryRootID] == 0 || confirmedInRoot[rootRes.LibraryRootID] > 0 {
			continue
		}
		if unconfirmedRoots == nil {
			unconfirmedRoots = make(map[string]struct{})
		}
		unconfirmedRoots[rootRes.LibraryRootID] = struct{}{}
	}
	return unconfirmedRoots
}

func markRootsIncomplete(res *ScanResult, unconfirmedRoots map[string]struct{}) {
	for i := range res.Roots {
		if _, ok := unconfirmedRoots[res.Roots[i].LibraryRootID]; ok {
			res.Roots[i].Complete = false
		}
	}
	res.Complete = false
	res.ApplyRemovals = false
}

func collectReconciliationObservations(
	roots []RootScanResult,
	obs *libraryWalkObservations,
) (map[string]struct{}, []plannedModify, []plannedRestore, []candidateObservation) {
	confirmedInPlace := make(map[string]struct{})
	var mtimeRefreshes []plannedModify
	var samePathRestores []plannedRestore
	var candidates []candidateObservation

	for _, rootRes := range roots {
		for _, ch := range rootRes.Changed {
			loc := assetLocation{
				rootID:       rootRes.LibraryRootID,
				relPathBytes: string(ch.Inspected.RelPath.Bytes),
			}
			cur, hasCur := ch.Existing.CurrentDigest(ch.Inspected.DigestKind)
			curMatches := hasCur && cur.Value == ch.Inspected.Digest
			if ch.Existing.MissingSince != nil {
				if curMatches {
					confirmedInPlace[ch.Existing.ID] = struct{}{}
					samePathRestores = append(samePathRestores, plannedRestore{
						Existing:  ch.Existing,
						RootID:    rootRes.LibraryRootID,
						Inspected: ch.Inspected,
					})
					continue
				}
				candidates = append(candidates, candidateObservation{
					loc:       loc,
					rootID:    rootRes.LibraryRootID,
					inspected: ch.Inspected,
				})
				continue
			}
			if curMatches {
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
	return confirmedInPlace, mtimeRefreshes, samePathRestores, candidates
}

func collectMissingAndRetiredTransitions(
	unconfirmedActive []domain.MediaAsset,
	unconfirmedMissing []domain.MediaAsset,
	proposedMoves map[string]plannedMove,
	modifies []plannedModify,
	restores []plannedRestore,
	now time.Time,
	grace time.Duration,
) ([]plannedAssetTransition, []plannedAssetTransition) {
	resolvedIDs := make(map[string]struct{}, len(modifies)+len(restores))
	for _, m := range modifies {
		resolvedIDs[m.Existing.ID] = struct{}{}
	}
	for _, r := range restores {
		resolvedIDs[r.Existing.ID] = struct{}{}
	}

	var markMissing []plannedAssetTransition
	for _, asset := range unconfirmedActive {
		if _, moved := proposedMoves[asset.ID]; moved {
			continue
		}
		if _, resolved := resolvedIDs[asset.ID]; resolved {
			continue
		}
		markMissing = append(markMissing, plannedAssetTransition{
			Existing: asset,
			RootID:   asset.LibraryRootID,
		})
	}

	var retireMissing []plannedAssetTransition
	for _, asset := range unconfirmedMissing {
		if _, moved := proposedMoves[asset.ID]; moved {
			continue
		}
		if _, resolved := resolvedIDs[asset.ID]; resolved {
			continue
		}
		if asset.RetiredAt != nil || asset.MissingSince == nil {
			continue
		}
		if !now.Before(asset.MissingSince.UTC().Add(grace)) {
			retireMissing = append(retireMissing, plannedAssetTransition{
				Existing: asset,
				RootID:   asset.LibraryRootID,
			})
		}
	}
	return markMissing, retireMissing
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
	unconfirmedLocs          []assetLocation
	samePathHistorical       []bool
	activeCurrentBuckets     map[digestKey][]int
	missingCurrentBuckets    map[digestKey][]int
	activeHistoricalBuckets  map[digestKey][]int
	missingHistoricalBuckets map[digestKey][]int
}

func buildMoveDigestIndex(unconfirmed []domain.MediaAsset, candidates []candidateObservation) moveDigestIndex {
	candByLoc := make(map[assetLocation]int, len(candidates))
	for obsIdx, cand := range candidates {
		candByLoc[cand.loc] = obsIdx
	}

	idx := moveDigestIndex{
		unconfirmedLocs:          make([]assetLocation, len(unconfirmed)),
		samePathHistorical:       make([]bool, len(unconfirmed)),
		activeCurrentBuckets:     make(map[digestKey][]int, len(unconfirmed)),
		missingCurrentBuckets:    make(map[digestKey][]int),
		activeHistoricalBuckets:  make(map[digestKey][]int),
		missingHistoricalBuckets: make(map[digestKey][]int),
	}
	for i, asset := range unconfirmed {
		loc := assetLocation{
			rootID:       asset.LibraryRootID,
			relPathBytes: string(asset.RelPathBytes),
		}
		idx.unconfirmedLocs[i] = loc
		if obsIdx, ok := candByLoc[loc]; ok {
			cand := candidates[obsIdx]
			if hasMatchingHistoricalDigest(asset, cand.inspected.DigestKind, cand.inspected.Digest) {
				idx.samePathHistorical[i] = true
			}
		}
		isMissing := asset.MissingSince != nil
		for _, d := range asset.Digests {
			key := digestKey{kind: d.Kind, value: d.Value}
			target := selectDigestBucket(idx, isMissing, d.IsCurrent)
			bucket := target[key]
			if len(bucket) == 0 || bucket[len(bucket)-1] != i {
				target[key] = append(bucket, i)
			}
		}
	}
	return idx
}

func selectDigestBucket(idx moveDigestIndex, isMissing, isCurrent bool) map[digestKey][]int {
	if isMissing {
		if isCurrent {
			return idx.missingCurrentBuckets
		}
		return idx.missingHistoricalBuckets
	}
	if isCurrent {
		return idx.activeCurrentBuckets
	}
	return idx.activeHistoricalBuckets
}

func hasMatchingHistoricalDigest(asset domain.MediaAsset, kind domain.DigestKind, value string) bool {
	for _, d := range asset.Digests {
		if !d.IsCurrent && d.Kind == kind && d.Value == value {
			return true
		}
	}
	return false
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

	matchPass(index.activeCurrentBuckets)
	matchPass(index.missingCurrentBuckets)
	// When no current-digest move explains an unconfirmed asset's departure,
	// keep a same-path historical observation in place as an external modify or
	// same-path historical restore rather than letting a copy steal the asset ID.
	for assetIdx, hasHist := range index.samePathHistorical {
		if hasHist && !claimed[assetIdx] {
			claimed[assetIdx] = true
		}
	}
	matchPass(index.activeHistoricalBuckets)
	matchPass(index.missingHistoricalBuckets)
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
) ([]plannedModify, []plannedRestore, []observedFile) {
	var modifies []plannedModify
	var restores []plannedRestore
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
		if _, vacated := proposedMoves[occupant.ID]; vacated {
			toIndex = append(toIndex, observedFile{
				rootID:    cand.rootID,
				inspected: cand.inspected,
			})
			continue
		}
		if occupant.MissingSince != nil && hasMatchingHistoricalDigest(occupant, cand.inspected.DigestKind, cand.inspected.Digest) {
			restores = append(restores, plannedRestore{
				Existing:  occupant,
				RootID:    cand.rootID,
				Inspected: cand.inspected,
			})
			continue
		}
		modifies = append(modifies, plannedModify{
			Existing:  occupant,
			RootID:    cand.rootID,
			Inspected: cand.inspected,
		})
	}
	return modifies, restores, toIndex
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
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	var writeErrs []error
	for _, comp := range components {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		if err := checkScanCancel(ctx, checkCancel); err != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		if err := s.applyMoveComponent(ctx, libraryID, checkedRoots, comp); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(append(writeErrs, ctxErr)...)
			}
			if errors.Is(err, ErrStalePreCheck) || isScanAbortErr(ctx, err) {
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
			if mv.Asset.MissingSince != nil {
				res.recordReconcileRestored(mv.TargetRootID)
			}
		}
	}
	return errors.Join(writeErrs...)
}

type pendingMoveEvent struct {
	asset               domain.MediaAsset
	prevDisplay         string
	emitInvalidUTF8Path bool
	emitRestored        bool
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

		now := s.now().UTC().Truncate(time.Microsecond)
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
			if err := s.appendMoveEvents(ctx, appender, libraryID, ev); err != nil {
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
	if !equalOptionalTimePtr(fresh.MissingSince, expected.MissingSince) ||
		!equalOptionalTimePtr(fresh.RetiredAt, expected.RetiredAt) ||
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
	wasMissing := fresh.MissingSince != nil

	fresh.LibraryRootID = mv.TargetRootID
	fresh.RelPathBytes = slices.Clone(relPath.Bytes)
	fresh.RelPathDisplay = relPath.Display
	fresh.RelPathMatch = relPath.Match
	fresh.FileSize = mv.Inspected.Size
	fresh.FileMtime = optionalNormalizedMtimePtr(mv.Inspected.Mtime)
	fresh.Diagnostics.InvalidUTF8Path = relPath.InvalidUTF8
	fresh.MissingSince = nil
	fresh.RetiredAt = nil
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
		emitRestored:        wasMissing,
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

func (s *Scanner) appendAssetEvent(
	ctx context.Context,
	appender storage.EventRepository,
	kind domain.EventKind,
	libraryID string,
	asset domain.MediaAsset,
) error {
	payload := domain.AssetEventPayloadV1{
		AssetID:       asset.ID,
		VolumeID:      asset.VolumeID,
		LibraryID:     libraryID,
		LibraryRootID: asset.LibraryRootID,
		Format:        asset.Format,
		PathDisplay:   asset.RelPathDisplay,
	}
	_, err := s.outbox.AppendPayload(
		ctx,
		appender,
		kind,
		domain.EventVersion1,
		domain.AggregateTypeAsset,
		asset.ID,
		payload,
	)
	return err
}

func (s *Scanner) appendMoveEvents(
	ctx context.Context,
	appender storage.EventRepository,
	libraryID string,
	ev pendingMoveEvent,
) error {
	movedPayload := domain.AssetEventPayloadV1{
		AssetID:             ev.asset.ID,
		VolumeID:            ev.asset.VolumeID,
		LibraryID:           libraryID,
		LibraryRootID:       ev.asset.LibraryRootID,
		Format:              ev.asset.Format,
		PathDisplay:         ev.asset.RelPathDisplay,
		PreviousPathDisplay: ev.prevDisplay,
	}
	if _, err := s.outbox.AppendPayload(
		ctx,
		appender,
		domain.EventAssetMoved,
		domain.EventVersion1,
		domain.AggregateTypeAsset,
		ev.asset.ID,
		movedPayload,
	); err != nil {
		return err
	}
	if ev.emitRestored {
		if err := s.appendAssetEvent(ctx, appender, domain.EventAssetRestored, libraryID, ev.asset); err != nil {
			return err
		}
	}
	if ev.emitInvalidUTF8Path {
		if err := s.appendAssetEvent(ctx, appender, domain.EventAssetInvalidUTF8Path, libraryID, ev.asset); err != nil {
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
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	return runReconcileStep(
		ctx,
		checkCancel,
		modifies,
		func(mod plannedModify) error {
			applied, wasMissing, err := s.applyExternalModify(ctx, libraryID, checkedRoots, mod)
			if err != nil {
				return err
			}
			if applied {
				res.recordReconcileModified(mod.RootID)
				if wasMissing {
					res.recordReconcileRestored(mod.RootID)
				}
			}
			return nil
		},
		func(mod plannedModify, err error) {
			res.recordReconcileSkipped(mod.RootID, SkippedEntry{
				RelPath: mod.Inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
		},
	)
}

func (s *Scanner) applyExternalModify(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	mod plannedModify,
) (bool, bool, error) {
	relPath, err := normalizeInspectedRelPath(mod.Inspected.RelPath)
	if err != nil {
		return false, false, err
	}
	var (
		applied    bool
		wasMissing bool
	)
	err = s.withLockedAssetSnapshot(ctx, libraryID, checkedRoots, mod.Existing, "modify",
		func(tx storage.Tx, cw storage.CatalogueWriteRepository, vol *domain.Volume, asset *domain.MediaAsset) error {
			libRepo := tx.Libraries()
			appender := tx.Events()
			if libRepo == nil || appender == nil {
				return errors.New("scanner: library and event repositories are required")
			}
			lib, err := libRepo.GetLibraryByID(ctx, libraryID)
			if err != nil {
				return err
			}
			if asset.VolumeID != vol.ID || asset.LibraryRootID != mod.RootID || !bytes.Equal(asset.RelPathBytes, relPath.Bytes) {
				return fmt.Errorf("%w: asset %q changed concurrently before modify", ErrStaleAssetState, mod.Existing.ID)
			}
			if cur, ok := asset.CurrentDigest(mod.Inspected.DigestKind); ok && cur.Value == mod.Inspected.Digest {
				return nil
			}

			now := s.now().UTC().Truncate(time.Microsecond)
			wasMissing = asset.MissingSince != nil
			updatedAsset, err := s.persistExternalModifyAsset(ctx, cw, *asset, relPath, mod.Inspected, now)
			if err != nil {
				return err
			}
			if err := refreshModifiedCatalogue(ctx, tx, cw, lib, *vol, relPath, mod.Inspected, now); err != nil {
				return err
			}
			if err := s.appendExternalModifyEvents(ctx, appender, lib.ID, updatedAsset, wasMissing); err != nil {
				return err
			}
			applied = true
			return nil
		},
	)
	return applied, wasMissing, err
}

func (s *Scanner) appendExternalModifyEvents(
	ctx context.Context,
	appender storage.EventRepository,
	libraryID string,
	asset domain.MediaAsset,
	wasMissing bool,
) error {
	if err := s.appendAssetEvent(ctx, appender, domain.EventAssetModified, libraryID, asset); err != nil {
		return err
	}
	if wasMissing {
		return s.appendAssetEvent(ctx, appender, domain.EventAssetRestored, libraryID, asset)
	}
	return nil
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

	wasMissing := asset.MissingSince != nil
	updated := asset
	updated.FileSize = inspected.Size
	updated.FileMtime = optionalNormalizedMtimePtr(inspected.Mtime)
	updated.Format = inspected.Format
	updated.PageCount = inspected.PageCount
	updated.CoverPageIndex = inspected.CoverPageIndex
	updated.SuppressedPages = slices.Clone(inspected.SuppressedPages)
	updated.Diagnostics.ImageCodecs = slices.Clone(inspected.ImageCodecs)
	updated.Diagnostics.InvalidUTF8Path = relPath.InvalidUTF8
	updated.MissingSince = nil
	updated.RetiredAt = nil
	updated.UpdatedAt = now
	if wasMissing {
		if err := cw.UpdateAsset(ctx, &updated); err != nil {
			return domain.MediaAsset{}, err
		}
		return updated, nil
	}
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

func runReconcileStep[T any](
	ctx context.Context,
	checkCancel func(context.Context) error,
	items []T,
	apply func(T) error,
	onSkip func(T, error),
) error {
	var writeErrs []error
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		if err := checkScanCancel(ctx, checkCancel); err != nil {
			return errors.Join(append(writeErrs, err)...)
		}
		if err := apply(item); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(append(writeErrs, ctxErr)...)
			}
			if errors.Is(err, ErrStalePreCheck) || isScanAbortErr(ctx, err) {
				return errors.Join(append(writeErrs, err)...)
			}
			if onSkip != nil {
				onSkip(item, err)
			}
			writeErrs = append(writeErrs, err)
		}
	}
	return errors.Join(writeErrs...)
}

func (s *Scanner) withLockedAssetSnapshot(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	expected domain.MediaAsset,
	action string,
	fn func(tx storage.Tx, cw storage.CatalogueWriteRepository, vol *domain.Volume, asset *domain.MediaAsset) error,
) error {
	return s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		// Global lock order:
		// libraries (FOR UPDATE) -> library_roots (id ASC, FOR NO KEY UPDATE) ->
		// volumes (FOR UPDATE) -> media_assets (FOR UPDATE).
		if err := lockAndVerifyRootSnapshot(ctx, tx, libraryID, checkedRoots); err != nil {
			return err
		}
		cw := tx.CatalogueWrites()
		if cw == nil {
			return errors.New("scanner: catalogue write repository is required")
		}
		vol, err := cw.GetVolumeByIDForUpdate(ctx, expected.VolumeID)
		if err != nil {
			return err
		}
		asset, err := cw.GetAssetByIDForUpdate(ctx, expected.ID)
		if err != nil {
			return err
		}
		if err := verifyLockedAssetSnapshot(*asset, expected, action); err != nil {
			return err
		}
		return fn(tx, cw, vol, asset)
	})
}

func (s *Scanner) executeRestores(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	restores []plannedRestore,
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	return runReconcileStep(
		ctx,
		checkCancel,
		restores,
		func(r plannedRestore) error {
			if err := s.applySamePathRestore(ctx, libraryID, checkedRoots, r); err != nil {
				return err
			}
			res.recordReconcileRestored(r.RootID)
			return nil
		},
		func(r plannedRestore, err error) {
			res.recordReconcileSkipped(r.RootID, SkippedEntry{
				RelPath: r.Inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
		},
	)
}

func (s *Scanner) applySamePathRestore(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	r plannedRestore,
) error {
	relPath, err := normalizeInspectedRelPath(r.Inspected.RelPath)
	if err != nil {
		return err
	}
	return s.withLockedAssetSnapshot(ctx, libraryID, checkedRoots, r.Existing, "restore",
		func(tx storage.Tx, cw storage.CatalogueWriteRepository, vol *domain.Volume, asset *domain.MediaAsset) error {
			appender := tx.Events()
			if appender == nil {
				return errors.New("scanner: event repository is required")
			}
			if asset.MissingSince == nil || asset.LibraryRootID != r.RootID || !bytes.Equal(asset.RelPathBytes, relPath.Bytes) {
				return fmt.Errorf("%w: asset %q changed concurrently before restore", ErrStaleAssetState, r.Existing.ID)
			}
			now := s.now().UTC().Truncate(time.Microsecond)
			cur, hasCur := asset.CurrentDigest(r.Inspected.DigestKind)
			if !hasCur || cur.Value != r.Inspected.Digest {
				if !hasMatchingHistoricalDigest(*asset, r.Inspected.DigestKind, r.Inspected.Digest) {
					return fmt.Errorf("%w: asset %q digest changed concurrently before restore", ErrStaleAssetState, r.Existing.ID)
				}
				vols := map[string]domain.Volume{vol.ID: *vol}
				if err := promoteHistoricalMoveDigest(ctx, cw, asset, vols, r.Inspected, now); err != nil {
					return err
				}
			}
			asset.FileSize = r.Inspected.Size
			asset.FileMtime = optionalNormalizedMtimePtr(r.Inspected.Mtime)
			asset.MissingSince = nil
			asset.RetiredAt = nil
			asset.UpdatedAt = now
			if err := cw.UpdateAsset(ctx, asset); err != nil {
				return err
			}
			return s.appendAssetEvent(ctx, appender, domain.EventAssetRestored, libraryID, *asset)
		},
	)
}

func (s *Scanner) executeMtimeRefreshes(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	refreshes []plannedModify,
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	return runReconcileStep(
		ctx,
		checkCancel,
		refreshes,
		func(ref plannedModify) error {
			return s.applyMtimeRefresh(ctx, libraryID, checkedRoots, ref)
		},
		func(ref plannedModify, err error) {
			res.recordReconcileSkipped(ref.RootID, SkippedEntry{
				RelPath: ref.Inspected.RelPath,
				Reason:  SkipReasonIndexFailed,
				Err:     err,
			})
		},
	)
}

func (s *Scanner) applyMtimeRefresh(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	ref plannedModify,
) error {
	return s.withLockedAssetSnapshot(ctx, libraryID, checkedRoots, ref.Existing, "mtime refresh",
		func(_ storage.Tx, cw storage.CatalogueWriteRepository, _ *domain.Volume, asset *domain.MediaAsset) error {
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
			updated.UpdatedAt = s.now().UTC().Truncate(time.Microsecond)
			return cw.UpdateAssetContent(ctx, &updated)
		},
	)
}

func (s *Scanner) executeMarkMissing(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	items []plannedAssetTransition,
	missingAt time.Time,
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	return runReconcileStep(
		ctx,
		checkCancel,
		items,
		func(item plannedAssetTransition) error {
			if err := s.applyMarkMissing(ctx, libraryID, checkedRoots, item, missingAt); err != nil {
				return err
			}
			res.recordReconcileMissing(item.RootID)
			return nil
		},
		nil,
	)
}

func (s *Scanner) applyMarkMissing(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	item plannedAssetTransition,
	missingAt time.Time,
) error {
	return s.withLockedAssetSnapshot(ctx, libraryID, checkedRoots, item.Existing, "mark missing",
		func(tx storage.Tx, cw storage.CatalogueWriteRepository, _ *domain.Volume, asset *domain.MediaAsset) error {
			appender := tx.Events()
			if appender == nil {
				return errors.New("scanner: event repository is required")
			}
			if asset.MissingSince != nil || asset.RetiredAt != nil {
				return fmt.Errorf("%w: asset %q already missing before mark missing", ErrStaleAssetState, item.Existing.ID)
			}
			ts := missingAt.UTC().Truncate(time.Microsecond)
			asset.MissingSince = &ts
			asset.UpdatedAt = ts
			if err := cw.UpdateAsset(ctx, asset); err != nil {
				return err
			}
			return s.appendAssetEvent(ctx, appender, domain.EventAssetMissing, libraryID, *asset)
		},
	)
}

func (s *Scanner) executeRetireMissing(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	items []plannedAssetTransition,
	grace time.Duration,
	retiredAt time.Time,
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	return runReconcileStep(
		ctx,
		checkCancel,
		items,
		func(item plannedAssetTransition) error {
			if err := s.applyRetireMissing(ctx, libraryID, checkedRoots, item, grace, retiredAt); err != nil {
				return err
			}
			res.recordReconcileRetired(item.RootID)
			return nil
		},
		nil,
	)
}

func (s *Scanner) applyRetireMissing(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	item plannedAssetTransition,
	grace time.Duration,
	retiredAt time.Time,
) error {
	return s.withLockedAssetSnapshot(ctx, libraryID, checkedRoots, item.Existing, "retire",
		func(_ storage.Tx, cw storage.CatalogueWriteRepository, _ *domain.Volume, asset *domain.MediaAsset) error {
			if asset.MissingSince == nil || asset.RetiredAt != nil {
				return fmt.Errorf("%w: asset %q not eligible for retirement", ErrStaleAssetState, item.Existing.ID)
			}
			ts := retiredAt.UTC().Truncate(time.Microsecond)
			if ts.Before(asset.MissingSince.UTC().Add(grace)) {
				return fmt.Errorf("%w: asset %q grace period no longer elapsed before retire", ErrStaleAssetState, item.Existing.ID)
			}
			asset.RetiredAt = &ts
			asset.UpdatedAt = ts
			// Retirement within the missing state does not produce another missing event (ADR-0018 S3, M1B-D3).
			return cw.UpdateAsset(ctx, asset)
		},
	)
}

func (s *Scanner) executeDeferredIndexes(
	ctx context.Context,
	libraryID string,
	checkedRoots []RootCheckResult,
	toIndex []observedFile,
	checkCancel func(context.Context) error,
	res *ScanResult,
) error {
	for _, item := range toIndex {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkScanCancel(ctx, checkCancel); err != nil {
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
			if errors.Is(err, ErrStalePreCheck) || isScanAbortErr(ctx, err) {
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
