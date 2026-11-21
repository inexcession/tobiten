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
