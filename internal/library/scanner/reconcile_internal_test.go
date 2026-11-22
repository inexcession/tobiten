package scanner

import (
	"fmt"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

const testPlannerRootID = "root-1"

func makeTestAssetForPlanner(
	id, relPath string,
	createdAt time.Time,
	missing bool,
	digests ...domain.AssetDigest,
) domain.MediaAsset {
	pathID := domain.NewPathIdentity([]byte(relPath))
	var missingSince *time.Time
	if missing {
		ts := createdAt.Add(time.Minute)
		missingSince = &ts
	}
	return domain.MediaAsset{
		ID:             id,
		VolumeID:       "vol-" + id,
		LibraryRootID:  testPlannerRootID,
		RelPathBytes:   []byte(relPath),
		RelPathDisplay: pathID.Display,
		RelPathMatch:   pathID.Match,
		FileSize:       100,
		Format:         domain.MediaFormatCBZ,
		PageCount:      1,
		MissingSince:   missingSince,
		Digests:        digests,
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
	}
}

func makeTestCandidateForPlanner(relPath, digest string) candidateObservation {
	pathID := domain.NewPathIdentity([]byte(relPath))
	return candidateObservation{
		loc: assetLocation{
			rootID:       testPlannerRootID,
			relPathBytes: relPath,
		},
		rootID: testPlannerRootID,
		inspected: InspectedFile{
			RelPath:    pathID,
			Format:     domain.MediaFormatCBZ,
			Size:       100,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     digest,
			PageCount:  1,
		},
	}
}

func TestMatchMovesFixedPoint_LargeUniqueRenameAndSemantics(t *testing.T) {
	t.Parallel()

	t.Run("4000 unique-hash directory rename completes quickly", func(t *testing.T) {
		t.Parallel()
		const count = 4000
		baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

		unconfirmed := make([]domain.MediaAsset, 0, count)
		candidates := make([]candidateObservation, 0, count)
		existingByLoc := make(map[assetLocation]domain.MediaAsset, count)

		for i := range count {
			id := fmt.Sprintf("asset-%05d", i)
			oldPath := fmt.Sprintf("old_dir/book_%05d.cbz", i)
			newPath := fmt.Sprintf("new_dir/book_%05d.cbz", i)
			digest := fmt.Sprintf("hash-%05d", i)
			asset := makeTestAssetForPlanner(id, oldPath, baseTime.Add(time.Duration(i)*time.Second), false,
				domain.AssetDigest{
					AssetID:    id,
					Kind:       domain.DigestKindXXH364Partial,
					Value:      digest,
					IsCurrent:  true,
					Reason:     domain.DigestReasonScan,
					RecordedAt: baseTime,
				},
			)
			unconfirmed = append(unconfirmed, asset)
			existingByLoc[assetLocation{rootID: testPlannerRootID, relPathBytes: oldPath}] = asset
			candidates = append(candidates, makeTestCandidateForPlanner(newPath, digest))
		}

		start := time.Now()
		proposed, matchedObs := matchMovesFixedPoint(unconfirmed, candidates, existingByLoc)
		elapsed := time.Since(start)

		if len(proposed) != count {
			t.Fatalf("expected %d matched moves, got %d", count, len(proposed))
		}
		for i, matched := range matchedObs {
			if !matched {
				t.Fatalf("expected candidate %d matched", i)
			}
		}
		if elapsed > 2*time.Second {
			t.Fatalf("4000-move digest-indexed plan took too long: %v", elapsed)
		}
	})

	t.Run("current priority, tie-breaking, same-loc skip, and blocked destinations", func(t *testing.T) {
		t.Parallel()
		t0 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

		// asset-hist is at blocked_hist.cbz and has dupDigest only historically + same-loc skip test:
		// asset-dup-1 (earlier) vs asset-dup-2 (later) share same current digest; candidate at
		// asset-dup-1's own location ("dup_a.cbz") must skip asset-dup-1 without losing asset-dup-1
		// for a second candidate at "dup_c.cbz".
		dupA := makeTestAssetForPlanner("a-dup-1", "dup_a.cbz", t0, false,
			domain.AssetDigest{AssetID: "a-dup-1", Kind: domain.DigestKindXXH364Partial, Value: "dup-hash", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)
		dupB := makeTestAssetForPlanner("a-dup-2", "dup_b.cbz", t0.Add(time.Second), false,
			domain.AssetDigest{AssetID: "a-dup-2", Kind: domain.DigestKindXXH364Partial, Value: "dup-hash", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)

		// Priority test: asset-cur has prio-hash as IsCurrent=true (newer CreatedAt) while
		// asset-hist has prio-hash as IsCurrent=false (older CreatedAt). Current match must win!
		histOwner := makeTestAssetForPlanner("a-hist", "hist_old.cbz", t0, false,
			domain.AssetDigest{AssetID: "a-hist", Kind: domain.DigestKindXXH364Partial, Value: "prio-hash", IsCurrent: false, Reason: domain.DigestReasonScan, RecordedAt: t0},
			domain.AssetDigest{AssetID: "a-hist", Kind: domain.DigestKindXXH364Partial, Value: "hist-new-cur", IsCurrent: true, Reason: domain.DigestReasonExternalModify, RecordedAt: t0.Add(time.Minute)},
		)
		curOwner := makeTestAssetForPlanner("a-cur", "cur_old.cbz", t0.Add(2*time.Second), false,
			domain.AssetDigest{AssetID: "a-cur", Kind: domain.DigestKindXXH364Partial, Value: "prio-hash", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)

		// Blocked destination test: dest is occupied by a missing asset, so histOwner's historical
		// move target at "missing_occupied.cbz" is blocked during fixed-point convergence.
		missingOccupant := makeTestAssetForPlanner("a-missing", "missing_occupied.cbz", t0, true,
			domain.AssetDigest{AssetID: "a-missing", Kind: domain.DigestKindXXH364Partial, Value: "other", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)
		histFallback := makeTestAssetForPlanner("a-hist-only", "hist_only_old.cbz", t0.Add(3*time.Second), false,
			domain.AssetDigest{AssetID: "a-hist-only", Kind: domain.DigestKindXXH364Partial, Value: "fallback-hash", IsCurrent: false, Reason: domain.DigestReasonScan, RecordedAt: t0},
			domain.AssetDigest{AssetID: "a-hist-only", Kind: domain.DigestKindXXH364Partial, Value: "fallback-cur", IsCurrent: true, Reason: domain.DigestReasonExternalModify, RecordedAt: t0.Add(time.Minute)},
		)

		unconfirmed := []domain.MediaAsset{dupA, dupB, histOwner, curOwner, histFallback}
		existingByLoc := map[assetLocation]domain.MediaAsset{
			{rootID: testPlannerRootID, relPathBytes: "dup_a.cbz"}:            dupA,
			{rootID: testPlannerRootID, relPathBytes: "dup_b.cbz"}:            dupB,
			{rootID: testPlannerRootID, relPathBytes: "hist_old.cbz"}:         histOwner,
			{rootID: testPlannerRootID, relPathBytes: "cur_old.cbz"}:          curOwner,
			{rootID: testPlannerRootID, relPathBytes: "missing_occupied.cbz"}: missingOccupant,
			{rootID: testPlannerRootID, relPathBytes: "hist_only_old.cbz"}:    histFallback,
		}

		// Candidates sorted by relPath:
		// 1. "dup_a.cbz" (dup-hash): cannot claim dupA (same loc!), claims dupB instead, leaving dupA unclaimed at cursor 0!
		// 2. "dup_c.cbz" (dup-hash): cursor still at 0, so claims dupA!
		// 3. "missing_occupied.cbz" (fallback-hash): blocked by missingOccupant; second candidate at "open_fallback.cbz" wins.
		// 4. "open_fallback.cbz" (fallback-hash): claims histFallback after blocked pass!
		// 5. "prio_moved.cbz" (prio-hash): claims curOwner over histOwner despite histOwner having older CreatedAt.
		candidates := []candidateObservation{
			makeTestCandidateForPlanner("dup_a.cbz", "dup-hash"),
			makeTestCandidateForPlanner("dup_c.cbz", "dup-hash"),
			makeTestCandidateForPlanner("missing_occupied.cbz", "fallback-hash"),
			makeTestCandidateForPlanner("open_fallback.cbz", "fallback-hash"),
			makeTestCandidateForPlanner("prio_moved.cbz", "prio-hash"),
		}

		proposed, matchedObs := matchMovesFixedPoint(unconfirmed, candidates, existingByLoc)

		if mv, ok := proposed[dupB.ID]; !ok || mv.Inspected.RelPath.Display != "dup_a.cbz" {
			t.Fatalf("expected dupB -> dup_a.cbz, got (%+v, %v)", mv, ok)
		}
		if mv, ok := proposed[dupA.ID]; !ok || mv.Inspected.RelPath.Display != "dup_c.cbz" {
			t.Fatalf("expected dupA -> dup_c.cbz (cursor must not skip unclaimed dupA), got (%+v, %v)", mv, ok)
		}
		if mv, ok := proposed[curOwner.ID]; !ok || mv.Inspected.RelPath.Display != "prio_moved.cbz" {
			t.Fatalf("expected current match curOwner -> prio_moved.cbz, got (%+v, %v)", mv, ok)
		}
		if _, ok := proposed[histOwner.ID]; ok {
			t.Fatalf("did not expect histOwner to claim prio_moved.cbz")
		}
		if mv, ok := proposed[histFallback.ID]; !ok || mv.Inspected.RelPath.Display != "open_fallback.cbz" {
			t.Fatalf("expected histFallback -> open_fallback.cbz after missing_occupied blocked, got (%+v, %v)", mv, ok)
		}
		if matchedObs[2] {
			t.Fatalf("expected blocked candidate missing_occupied.cbz unmatched")
		}
	})
}

func BenchmarkMatchMovesFixedPoint_4000(b *testing.B) {
	const count = 4000
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	unconfirmed := make([]domain.MediaAsset, 0, count)
	candidates := make([]candidateObservation, 0, count)
	existingByLoc := make(map[assetLocation]domain.MediaAsset, count)

	for i := range count {
		id := fmt.Sprintf("asset-%05d", i)
		oldPath := fmt.Sprintf("old_dir/book_%05d.cbz", i)
		newPath := fmt.Sprintf("new_dir/book_%05d.cbz", i)
		digest := fmt.Sprintf("hash-%05d", i)
		asset := makeTestAssetForPlanner(id, oldPath, baseTime.Add(time.Duration(i)*time.Second), false,
			domain.AssetDigest{
				AssetID:    id,
				Kind:       domain.DigestKindXXH364Partial,
				Value:      digest,
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: baseTime,
			},
		)
		unconfirmed = append(unconfirmed, asset)
		existingByLoc[assetLocation{rootID: testPlannerRootID, relPathBytes: oldPath}] = asset
		candidates = append(candidates, makeTestCandidateForPlanner(newPath, digest))
	}

	b.ResetTimer()
	for range b.N {
		proposed, _ := matchMovesFixedPoint(unconfirmed, candidates, existingByLoc)
		if len(proposed) != count {
			b.Fatalf("expected %d matches, got %d", count, len(proposed))
		}
	}
}

func TestPlanReconciliation_SamePathHistoricalPrecedence(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	makeHistoryAsset := func(id, relPath, curDigest, histDigest string, ts time.Time) domain.MediaAsset {
		return makeTestAssetForPlanner(id, relPath, ts, false,
			domain.AssetDigest{AssetID: id, Kind: domain.DigestKindXXH364Partial, Value: curDigest, IsCurrent: true, Reason: domain.DigestReasonExternalModify, RecordedAt: ts.Add(time.Minute)},
			domain.AssetDigest{AssetID: id, Kind: domain.DigestKindXXH364Partial, Value: histDigest, IsCurrent: false, Reason: domain.DigestReasonScan, RecordedAt: ts},
		)
	}

	runPlan := func(assets []domain.MediaAsset, changed []ChangedFile, newFiles []observedFile) reconciliationPlan {
		obs := newLibraryWalkObservations()
		obs.newFiles = newFiles
		roots := []RootScanResult{{
			LibraryRootID: testPlannerRootID,
			Complete:      true,
			Changed:       changed,
		}}
		return planReconciliation(roots, assets, obs, t0, DefaultMissingGracePeriod)
	}

	t.Run("same-path historical overwrite plus copy yields modify at original and add for copy", func(t *testing.T) {
		t.Parallel()
		orig := makeHistoryAsset("asset-a", "a.cbz", "digest-v2", "digest-v1", t0)
		candA := makeTestCandidateForPlanner("a.cbz", "digest-v1")
		candB := makeTestCandidateForPlanner("b.cbz", "digest-v1")

		plan := runPlan(
			[]domain.MediaAsset{orig},
			[]ChangedFile{{Existing: orig, Inspected: candA.inspected}},
			[]observedFile{{rootID: testPlannerRootID, inspected: candB.inspected}},
		)
		if len(plan.MoveComponents) != 0 {
			t.Fatalf("expected 0 moves, got %+v", plan.MoveComponents)
		}
		if len(plan.Modifies) != 1 || plan.Modifies[0].Existing.ID != orig.ID || plan.Modifies[0].Inspected.RelPath.Display != "a.cbz" {
			t.Fatalf("expected 1 modify for orig at a.cbz, got %+v", plan.Modifies)
		}
		if len(plan.ToIndex) != 1 || plan.ToIndex[0].inspected.RelPath.Display != "b.cbz" {
			t.Fatalf("expected 1 new index at b.cbz, got %+v", plan.ToIndex)
		}
	})

	t.Run("current-digest move explaining departure takes precedence over same-path historical", func(t *testing.T) {
		t.Parallel()
		orig := makeHistoryAsset("asset-a", "a.cbz", "digest-v2", "digest-v1", t0)
		candA := makeTestCandidateForPlanner("a.cbz", "digest-v1")
		candB := makeTestCandidateForPlanner("b.cbz", "digest-v2")

		plan := runPlan(
			[]domain.MediaAsset{orig},
			[]ChangedFile{{Existing: orig, Inspected: candA.inspected}},
			[]observedFile{{rootID: testPlannerRootID, inspected: candB.inspected}},
		)
		if len(plan.MoveComponents) != 1 || len(plan.MoveComponents[0]) != 1 ||
			plan.MoveComponents[0][0].Asset.ID != orig.ID || plan.MoveComponents[0][0].Inspected.RelPath.Display != "b.cbz" {
			t.Fatalf("expected current move orig -> b.cbz, got %+v", plan.MoveComponents)
		}
		if len(plan.Modifies) != 0 {
			t.Fatalf("expected 0 modifies, got %+v", plan.Modifies)
		}
		if len(plan.ToIndex) != 1 || plan.ToIndex[0].inspected.RelPath.Display != "a.cbz" {
			t.Fatalf("expected vacated a.cbz indexed as new, got %+v", plan.ToIndex)
		}
	})

	t.Run("exact-current exchange is preserved even with cross historical digests", func(t *testing.T) {
		t.Parallel()
		assetA := makeHistoryAsset("asset-a", "a.cbz", "digest-v2", "digest-v1", t0)
		assetB := makeHistoryAsset("asset-b", "b.cbz", "digest-v1", "digest-v2", t0.Add(time.Second))
		candA := makeTestCandidateForPlanner("a.cbz", "digest-v1")
		candB := makeTestCandidateForPlanner("b.cbz", "digest-v2")

		plan := runPlan(
			[]domain.MediaAsset{assetA, assetB},
			[]ChangedFile{
				{Existing: assetA, Inspected: candA.inspected},
				{Existing: assetB, Inspected: candB.inspected},
			},
			nil,
		)
		if len(plan.MoveComponents) != 1 || len(plan.MoveComponents[0]) != 2 {
			t.Fatalf("expected 1 exchange component of 2 moves, got %+v", plan.MoveComponents)
		}
		if len(plan.Modifies) != 0 || len(plan.ToIndex) != 0 {
			t.Fatalf("expected 0 modifies and 0 indexes, got modifies=%+v toIndex=%+v", plan.Modifies, plan.ToIndex)
		}
	})

	t.Run("historical move where source is gone is preserved", func(t *testing.T) {
		t.Parallel()
		orig := makeHistoryAsset("asset-a", "a.cbz", "digest-v2", "digest-v1", t0)
		candB := makeTestCandidateForPlanner("b.cbz", "digest-v1")

		plan := runPlan(
			[]domain.MediaAsset{orig},
			nil,
			[]observedFile{{rootID: testPlannerRootID, inspected: candB.inspected}},
		)
		if len(plan.MoveComponents) != 1 || len(plan.MoveComponents[0]) != 1 ||
			plan.MoveComponents[0][0].Asset.ID != orig.ID || plan.MoveComponents[0][0].Inspected.RelPath.Display != "b.cbz" {
			t.Fatalf("expected historical move orig -> b.cbz, got %+v", plan.MoveComponents)
		}
		if len(plan.Modifies) != 0 || len(plan.ToIndex) != 0 {
			t.Fatalf("expected 0 modifies and 0 indexes, got modifies=%+v toIndex=%+v", plan.Modifies, plan.ToIndex)
		}
	})
}

func TestPlanReconciliation_MissingRetirementAndRevivalPrecedence(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	runPlanAt := func(now time.Time, grace time.Duration, assets []domain.MediaAsset, changed []ChangedFile, newFiles []observedFile) reconciliationPlan {
		obs := newLibraryWalkObservations()
		obs.newFiles = newFiles
		roots := []RootScanResult{{
			LibraryRootID: testPlannerRootID,
			Complete:      true,
			Changed:       changed,
		}}
		return planReconciliation(roots, assets, obs, now, grace)
	}

	t.Run("unseen active marks missing, within grace does not retire, at grace retires", func(t *testing.T) {
		t.Parallel()
		active := makeTestAssetForPlanner("a-active", "a.cbz", t0, false,
			domain.AssetDigest{AssetID: "a-active", Kind: domain.DigestKindXXH364Partial, Value: "d-active", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)
		plan1 := runPlanAt(t0.Add(time.Hour), DefaultMissingGracePeriod, []domain.MediaAsset{active}, nil, nil)
		if len(plan1.MarkMissing) != 1 || plan1.MarkMissing[0].Existing.ID != active.ID || len(plan1.RetireMissing) != 0 {
			t.Fatalf("expected 1 MarkMissing and 0 RetireMissing, got %+v", plan1)
		}

		missingAt := t0.Add(time.Hour)
		withinGrace := active
		withinGrace.MissingSince = &missingAt
		plan2 := runPlanAt(missingAt.Add(DefaultMissingGracePeriod-time.Second), DefaultMissingGracePeriod, []domain.MediaAsset{withinGrace}, nil, nil)
		if len(plan2.MarkMissing) != 0 || len(plan2.RetireMissing) != 0 {
			t.Fatalf("expected 0 MarkMissing and 0 RetireMissing before grace, got %+v", plan2)
		}

		plan3 := runPlanAt(missingAt.Add(DefaultMissingGracePeriod), DefaultMissingGracePeriod, []domain.MediaAsset{withinGrace}, nil, nil)
		if len(plan3.RetireMissing) != 1 || plan3.RetireMissing[0].Existing.ID != withinGrace.ID {
			t.Fatalf("expected 1 RetireMissing at grace boundary, got %+v", plan3)
		}

		retiredAt := missingAt.Add(DefaultMissingGracePeriod)
		alreadyRetired := withinGrace
		alreadyRetired.RetiredAt = &retiredAt
		plan4 := runPlanAt(retiredAt.Add(24*time.Hour), DefaultMissingGracePeriod, []domain.MediaAsset{alreadyRetired}, nil, nil)
		if plan4.hasWrites() {
			t.Fatalf("expected no writes for already-retired unseen asset, got %+v", plan4)
		}
	})

	t.Run("active asset move takes priority over missing asset with same current digest", func(t *testing.T) {
		t.Parallel()
		missingAt := t0.Add(time.Minute)
		missingAsset := makeTestAssetForPlanner("a-miss", "miss.cbz", t0, true,
			domain.AssetDigest{AssetID: "a-miss", Kind: domain.DigestKindXXH364Partial, Value: "shared-d", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)
		missingAsset.MissingSince = &missingAt
		activeAsset := makeTestAssetForPlanner("a-act", "act.cbz", t0.Add(time.Second), false,
			domain.AssetDigest{AssetID: "a-act", Kind: domain.DigestKindXXH364Partial, Value: "shared-d", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)

		candMoved := makeTestCandidateForPlanner("moved.cbz", "shared-d")
		plan := runPlanAt(t0.Add(time.Hour), DefaultMissingGracePeriod,
			[]domain.MediaAsset{missingAsset, activeAsset},
			nil,
			[]observedFile{{rootID: testPlannerRootID, inspected: candMoved.inspected}},
		)
		if len(plan.MoveComponents) != 1 || len(plan.MoveComponents[0]) != 1 || plan.MoveComponents[0][0].Asset.ID != activeAsset.ID {
			t.Fatalf("expected active asset to claim moved.cbz over missing asset, got %+v", plan.MoveComponents)
		}
	})

	t.Run("missing asset same-path historical restore beats copy and different file yields modify", func(t *testing.T) {
		t.Parallel()
		missingAsset := makeTestAssetForPlanner("a-hist-miss", "m.cbz", t0, true,
			domain.AssetDigest{AssetID: "a-hist-miss", Kind: domain.DigestKindXXH364Partial, Value: "v2", IsCurrent: true, Reason: domain.DigestReasonExternalModify, RecordedAt: t0.Add(time.Minute)},
			domain.AssetDigest{AssetID: "a-hist-miss", Kind: domain.DigestKindXXH364Partial, Value: "v1", IsCurrent: false, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)
		candM := makeTestCandidateForPlanner("m.cbz", "v1")
		candCopy := makeTestCandidateForPlanner("copy.cbz", "v1")
		planHist := runPlanAt(t0.Add(time.Hour), DefaultMissingGracePeriod,
			[]domain.MediaAsset{missingAsset},
			[]ChangedFile{{Existing: missingAsset, Inspected: candM.inspected}},
			[]observedFile{{rootID: testPlannerRootID, inspected: candCopy.inspected}},
		)
		if len(planHist.MoveComponents) != 0 || len(planHist.Modifies) != 0 ||
			len(planHist.Restores) != 1 || planHist.Restores[0].Existing.ID != missingAsset.ID ||
			len(planHist.ToIndex) != 1 || planHist.ToIndex[0].inspected.RelPath.Display != "copy.cbz" {
			t.Fatalf("unexpected same-path historical restore plan: %+v", planHist)
		}

		candDiff := makeTestCandidateForPlanner("m.cbz", "v3")
		planDiff := runPlanAt(t0.Add(time.Hour), DefaultMissingGracePeriod,
			[]domain.MediaAsset{missingAsset},
			[]ChangedFile{{Existing: missingAsset, Inspected: candDiff.inspected}},
			nil,
		)
		if len(planDiff.Restores) != 0 || len(planDiff.Modifies) != 1 || planDiff.Modifies[0].Existing.ID != missingAsset.ID {
			t.Fatalf("expected different file at missing path to plan modify, got %+v", planDiff)
		}
	})

	t.Run("skipped missing original path stays excluded from revival move matching", func(t *testing.T) {
		t.Parallel()
		missingAsset := makeTestAssetForPlanner("a-skip-miss", "a.cbz", t0, true,
			domain.AssetDigest{AssetID: "a-skip-miss", Kind: domain.DigestKindXXH364Partial, Value: "d-skip", IsCurrent: true, Reason: domain.DigestReasonScan, RecordedAt: t0},
		)
		obs := newLibraryWalkObservations()
		obs.seenSkipped[assetLocation{rootID: testPlannerRootID, relPathBytes: "a.cbz"}] = struct{}{}
		candCopy := makeTestCandidateForPlanner("copy.cbz", "d-skip")
		obs.newFiles = []observedFile{{rootID: testPlannerRootID, inspected: candCopy.inspected}}
		roots := []RootScanResult{{
			LibraryRootID: testPlannerRootID,
			Complete:      true,
		}}

		plan := planReconciliation(roots, []domain.MediaAsset{missingAsset}, obs, t0.Add(8*24*time.Hour), DefaultMissingGracePeriod)
		if len(plan.MoveComponents) != 0 || len(plan.Restores) != 0 || len(plan.RetireMissing) != 0 ||
			len(plan.ToIndex) != 1 || plan.ToIndex[0].inspected.RelPath.Display != "copy.cbz" {
			t.Fatalf("expected skipped missing asset excluded from move/retire and copy.cbz indexed as new, got %+v", plan)
		}
	})
}
