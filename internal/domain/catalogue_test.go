package domain

import (
	"errors"
	"testing"
	"time"
)

func strPtr(s string) *string     { return &s }
func floatPtr(f float64) *float64 { return &f }

func TestFieldLocks(t *testing.T) {
	var locks FieldLocks
	if locks.IsLocked("title") {
		t.Fatal("nil FieldLocks should not report any field locked")
	}
	locks = locks.Lock("title")
	if !locks.IsLocked("title") {
		t.Fatal("expected title to be locked")
	}
	if locks.IsLocked("summary") {
		t.Fatal("expected summary to remain unlocked")
	}
	locks.Unlock("title")
	if locks.IsLocked("title") {
		t.Fatal("expected title to be unlocked after Unlock")
	}
}

func TestSeriesValidate(t *testing.T) {
	now := time.Now().UTC()
	policyFirst := CoverPolicyFirst
	invalidPolicy := CoverPolicy("INVALID")

	tests := []struct {
		name    string
		series  Series
		wantErr error
	}{
		{
			name: "valid series with volume cover",
			series: Series{
				ID:            "ser-1",
				LibraryID:     "lib-1",
				Title:         "Dorohedoro",
				TitleSort:     "Dorohedoro",
				Status:        SeriesStatusEnded,
				CoverPolicy:   &policyFirst,
				CoverVolumeID: strPtr("vol-1"),
				CreatedAt:     now,
				UpdatedAt:     now,
			},
		},
		{
			name: "missing ID",
			series: Series{
				LibraryID: "lib-1",
				Title:     "Dorohedoro",
			},
			wantErr: ErrEmptySeriesID,
		},
		{
			name: "missing library ID",
			series: Series{
				ID:    "ser-1",
				Title: "Dorohedoro",
			},
			wantErr: ErrEmptyLibraryID,
		},
		{
			name: "missing title",
			series: Series{
				ID:        "ser-1",
				LibraryID: "lib-1",
			},
			wantErr: ErrEmptySeriesTitle,
		},
		{
			name: "invalid cover policy",
			series: Series{
				ID:          "ser-1",
				LibraryID:   "lib-1",
				Title:       "Dorohedoro",
				CoverPolicy: &invalidPolicy,
			},
			wantErr: ErrInvalidCoverPolicy,
		},
		{
			name: "conflicting manual cover volume and image digest",
			series: Series{
				ID:               "ser-1",
				LibraryID:        "lib-1",
				Title:            "Dorohedoro",
				CoverVolumeID:    strPtr("vol-1"),
				CoverImageDigest: strPtr("deadbeef"),
			},
			wantErr: ErrConflictingManualCover,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.series.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Series.Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateSeriesMemberships(t *testing.T) {
	tests := []struct {
		name        string
		volumeID    string
		memberships []SeriesMembership
		wantErr     error
	}{
		{
			name:        "standalone volume with zero memberships",
			volumeID:    "vol-1",
			memberships: nil,
			wantErr:     nil,
		},
		{
			name:     "primary and secondary memberships",
			volumeID: "vol-1",
			memberships: []SeriesMembership{
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-main",
					IsPrimary: true,
					Kind:      MembershipKindSeries,
					Position:  floatPtr(8),
					Source:    MembershipSourceResolver,
				},
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-sub",
					IsPrimary: false,
					Kind:      MembershipKindSubSeries,
					Position:  floatPtr(2),
					Source:    MembershipSourceMetadata,
				},
			},
			wantErr: nil,
		},
		{
			name:     "missing primary membership",
			volumeID: "vol-1",
			memberships: []SeriesMembership{
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-sub",
					IsPrimary: false,
					Kind:      MembershipKindSubSeries,
					Source:    MembershipSourceMetadata,
				},
			},
			wantErr: ErrMissingPrimarySeries,
		},
		{
			name:     "multiple primary memberships",
			volumeID: "vol-1",
			memberships: []SeriesMembership{
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-1",
					IsPrimary: true,
					Kind:      MembershipKindSeries,
					Source:    MembershipSourceManual,
				},
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-2",
					IsPrimary: true,
					Kind:      MembershipKindSubSeries,
					Source:    MembershipSourceManual,
				},
			},
			wantErr: ErrMultiplePrimarySeries,
		},
		{
			name:     "duplicate series membership",
			volumeID: "vol-1",
			memberships: []SeriesMembership{
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-1",
					IsPrimary: true,
					Kind:      MembershipKindSeries,
					Source:    MembershipSourceResolver,
				},
				{
					VolumeID:  "vol-1",
					SeriesID:  "ser-1",
					IsPrimary: false,
					Kind:      MembershipKindSubSeries,
					Source:    MembershipSourceMetadata,
				},
			},
			wantErr: ErrDuplicateSeriesMember,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSeriesMemberships(tc.volumeID, tc.memberships)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ValidateSeriesMemberships() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestVolumeZeroManifestationsAndDerivedHelpers(t *testing.T) {
	now := time.Now().UTC()
	missingAt := now.Add(-24 * time.Hour)
	retiredAt := now.Add(-1 * time.Hour)

	makeAsset := func(id string, missing, retired *time.Time) MediaAsset {
		return MediaAsset{
			ID:             id,
			VolumeID:       "vol-1",
			LibraryRootID:  "root-1",
			RelPathBytes:   []byte("Series/" + id + ".cbz"),
			RelPathDisplay: "Series/" + id + ".cbz",
			RelPathMatch:   "Series/" + id + ".cbz",
			FileSize:       1024,
			Format:         MediaFormatCBZ,
			PageCount:      20,
			MissingSince:   missing,
			RetiredAt:      retired,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}

	t.Run("zero manifestations reports missing and unreadable", func(t *testing.T) {
		v := Volume{
			ID:        "vol-1",
			LibraryID: "lib-1",
			Kind:      VolumeKindVolume,
			Title:     strPtr("Missing Vol 1"),
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Volume.Validate() unexpected error for zero manifestations: %v", err)
		}
		if v.HasManifestations() {
			t.Error("expected HasManifestations() == false")
		}
		if !v.IsMissing() {
			t.Error("expected IsMissing() == true when Volume has no assets")
		}
		if v.IsReadable() {
			t.Error("expected IsReadable() == false when Volume has no assets")
		}
		if v.IsOwned() {
			t.Error("expected IsOwned() == false when Volume has no assets")
		}
		if got := v.AvailableAssets(); len(got) != 0 {
			t.Errorf("expected 0 AvailableAssets, got %d", len(got))
		}
	})

	t.Run("only missing and retired manifestations still report missing and unreadable", func(t *testing.T) {
		v := Volume{
			ID:        "vol-1",
			LibraryID: "lib-1",
			Kind:      VolumeKindVolume,
			Assets: []MediaAsset{
				makeAsset("asset-missing", &missingAt, nil),
				makeAsset("asset-retired", &missingAt, &retiredAt),
			},
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Volume.Validate() unexpected error: %v", err)
		}
		if !v.HasManifestations() {
			t.Error("expected HasManifestations() == true")
		}
		if !v.IsMissing() {
			t.Error("expected IsMissing() == true when all assets are missing/retired")
		}
		if v.IsReadable() {
			t.Error("expected IsReadable() == false when all assets are missing/retired")
		}
		if v.IsOwned() {
			t.Error("expected IsOwned() == false when all assets are missing/retired")
		}
		if got := v.AvailableAssets(); len(got) != 0 {
			t.Errorf("expected 0 AvailableAssets, got %d", len(got))
		}
	})

	t.Run("ordinary available asset control reports owned and readable", func(t *testing.T) {
		v := Volume{
			ID:        "vol-1",
			LibraryID: "lib-1",
			Kind:      VolumeKindVolume,
			Assets: []MediaAsset{
				makeAsset("asset-missing", &missingAt, nil),
				makeAsset("asset-live", nil, nil),
			},
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Volume.Validate() unexpected error: %v", err)
		}
		if v.IsMissing() {
			t.Error("expected IsMissing() == false when an available asset exists")
		}
		if !v.IsReadable() {
			t.Error("expected IsReadable() == true when an available asset exists")
		}
		if !v.IsOwned() {
			t.Error("expected IsOwned() == true when an available asset exists")
		}
		avail := v.AvailableAssets()
		if len(avail) != 1 || avail[0].ID != "asset-live" {
			t.Errorf("expected [asset-live], got %+v", avail)
		}
	})
}

func TestVolumeChapterRangeAndMemberships(t *testing.T) {
	srcManual := ChapterRangeSourceManual

	t.Run("complete chapter range", func(t *testing.T) {
		v := Volume{
			ID:                 "vol-1",
			LibraryID:          "lib-1",
			Kind:               VolumeKindVolume,
			ChapterStart:       floatPtr(291),
			ChapterEnd:         floatPtr(300),
			ChapterRangeSource: &srcManual,
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		cr, ok := v.ChapterRange()
		if !ok || cr.Start != 291 || cr.End != 300 || cr.Source != ChapterRangeSourceManual {
			t.Fatalf("unexpected ChapterRange: (%+v, %v)", cr, ok)
		}
	})

	t.Run("partial or inverted chapter range rejected", func(t *testing.T) {
		partial := Volume{
			ID:           "vol-1",
			LibraryID:    "lib-1",
			Kind:         VolumeKindVolume,
			ChapterStart: floatPtr(1),
		}
		if !errors.Is(partial.Validate(), ErrInvalidChapterRange) {
			t.Fatalf("expected ErrInvalidChapterRange for partial range, got %v", partial.Validate())
		}

		inverted := Volume{
			ID:                 "vol-1",
			LibraryID:          "lib-1",
			Kind:               VolumeKindVolume,
			ChapterStart:       floatPtr(10),
			ChapterEnd:         floatPtr(5),
			ChapterRangeSource: &srcManual,
		}
		if !errors.Is(inverted.Validate(), ErrInvalidChapterRange) {
			t.Fatalf("expected ErrInvalidChapterRange for inverted range, got %v", inverted.Validate())
		}
	})

	t.Run("membership derivation helpers", func(t *testing.T) {
		standalone := Volume{ID: "vol-s", LibraryID: "lib-1", Kind: VolumeKindVolume}
		if !standalone.IsStandalone() {
			t.Error("expected standalone volume to report IsStandalone() == true")
		}
		if _, ok := standalone.PrimarySeriesID(); ok {
			t.Error("expected standalone volume to have no primary series ID")
		}

		member := Volume{
			ID:        "vol-m",
			LibraryID: "lib-1",
			Kind:      VolumeKindVolume,
			Memberships: []SeriesMembership{
				{VolumeID: "vol-m", SeriesID: "ser-sec", IsPrimary: false, Kind: MembershipKindSubSeries, Source: MembershipSourceMetadata},
				{VolumeID: "vol-m", SeriesID: "ser-pri", IsPrimary: true, Kind: MembershipKindSeries, Source: MembershipSourceResolver},
			},
		}
		if member.IsStandalone() {
			t.Error("expected member volume to report IsStandalone() == false")
		}
		priID, ok := member.PrimarySeriesID()
		if !ok || priID != "ser-pri" {
			t.Errorf("PrimarySeriesID() = (%q, %v), want (ser-pri, true)", priID, ok)
		}
		if sec := member.SecondaryMemberships(); len(sec) != 1 || sec[0].SeriesID != "ser-sec" {
			t.Errorf("SecondaryMemberships() = %+v, want [ser-sec]", sec)
		}
	})
}

func TestVolumeSelectPreferredAsset(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	missingAt := t1.Add(time.Minute)

	assetA := MediaAsset{ID: "a-1", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("a1.cbz"), Format: MediaFormatCBZ, CreatedAt: t1}
	assetB := MediaAsset{ID: "a-2", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("a2.cbz"), Format: MediaFormatCBZ, CreatedAt: t0}
	assetC := MediaAsset{ID: "a-0", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("a0.cbz"), Format: MediaFormatCBZ, CreatedAt: t0}
	assetMissing := MediaAsset{ID: "a-missing", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("am.cbz"), Format: MediaFormatCBZ, CreatedAt: t0.Add(-time.Hour), MissingSince: &missingAt}

	v := Volume{
		ID:               "vol-1",
		LibraryID:        "lib-1",
		Kind:             VolumeKindVolume,
		PreferredAssetID: strPtr("a-1"),
		Assets:           []MediaAsset{assetA, assetB, assetC, assetMissing},
	}

	allowAll := func(MediaAsset) bool { return true }

	// 0. Nil eligibility predicate fails closed even when available assets exist.
	if chosen, ok := v.SelectPreferredAsset("a-2", nil); ok || chosen != nil {
		t.Fatalf("expected nil eligibility predicate to fail closed, got (%+v, %v)", chosen, ok)
	}

	// 1. Explicit manual preferred asset wins when available and eligible.
	chosen, ok := v.SelectPreferredAsset("a-2", allowAll)
	if !ok || chosen.ID != "a-1" {
		t.Fatalf("expected manual preferred a-1, got (%+v, %v)", chosen, ok)
	}

	// 2. If manual preferred is restricted, fall back to eligible locator asset.
	restrictedIDs := map[string]bool{"a-1": true}
	trashedIDs := map[string]bool{}
	eligibility := func(a MediaAsset) bool {
		return !restrictedIDs[a.ID] && !trashedIDs[a.ID]
	}
	chosen, ok = v.SelectPreferredAsset("a-2", eligibility)
	if !ok || chosen.ID != "a-2" {
		t.Fatalf("expected locator asset a-2 when a-1 restricted, got (%+v, %v)", chosen, ok)
	}

	// 3. If manual preferred and locator asset are trashed or restricted, fall back to oldest eligible (CreatedAt, ID).
	trashedIDs["a-2"] = true
	chosen, ok = v.SelectPreferredAsset("a-2", eligibility)
	if !ok || chosen.ID != "a-0" {
		t.Fatalf("expected oldest non-trashed/non-restricted a-0, got (%+v, %v)", chosen, ok)
	}

	// 4. Missing/retired assets remain excluded even if predicate returns true.
	v.PreferredAssetID = strPtr("a-missing")
	chosen, ok = v.SelectPreferredAsset("a-missing", allowAll)
	if !ok || chosen.ID != "a-0" {
		t.Fatalf("expected oldest available a-0 (tie-broken over a-2 at t0), got (%+v, %v)", chosen, ok)
	}

	// 5. Zero eligible assets returns false.
	chosen, ok = v.SelectPreferredAsset("", func(MediaAsset) bool { return false })
	if ok || chosen != nil {
		t.Fatalf("expected no eligible asset, got (%+v, %v)", chosen, ok)
	}
}

func TestMediaAssetValidateAndDigests(t *testing.T) {
	now := time.Now().UTC()

	goodPaths := [][]byte{
		[]byte("Series/v01.cbz"),
		[]byte("a:sample.cbz"),
		[]byte(`\sample.cbz`),
		[]byte(`..\sample.cbz`),
		[]byte(`dir/a:sample.cbz`),
		[]byte(`dir/\sample.cbz`),
		[]byte(".hidden.cbz"),
		[]byte("..dots.cbz"),
		{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', '.', 'c', 'b', 'z'},
	}
	for _, p := range goodPaths {
		a := MediaAsset{
			ID:            "asset-1",
			VolumeID:      "vol-1",
			LibraryRootID: "root-1",
			RelPathBytes:  p,
			Format:        MediaFormatCBZ,
		}
		if err := a.Validate(); err != nil {
			t.Errorf("expected valid Unix relative path %q to pass Validate(), got %v", p, err)
		}
	}

	badPaths := [][]byte{
		nil,
		{},
		[]byte("/abs/path.cbz"),
		[]byte("../escape.cbz"),
		[]byte("sub/../../escape.cbz"),
		[]byte("sub/.."),
		[]byte("."),
		[]byte("./book.cbz"),
		[]byte("sub//book.cbz"),
		[]byte("sub/"),
		[]byte("nul\x00byte.cbz"),
	}
	for _, p := range badPaths {
		a := MediaAsset{
			ID:            "asset-1",
			VolumeID:      "vol-1",
			LibraryRootID: "root-1",
			RelPathBytes:  p,
			Format:        MediaFormatCBZ,
		}
		if !errors.Is(a.Validate(), ErrInvalidAssetPath) {
			t.Errorf("expected ErrInvalidAssetPath for %q, got %v", p, a.Validate())
		}
	}

	retiredWithoutMissing := MediaAsset{
		ID:            "asset-1",
		VolumeID:      "vol-1",
		LibraryRootID: "root-1",
		RelPathBytes:  []byte("Series/v01.cbz"),
		Format:        MediaFormatCBZ,
		RetiredAt:     &now,
	}
	if !errors.Is(retiredWithoutMissing.Validate(), ErrRetiredWithoutMissing) {
		t.Fatalf("expected ErrRetiredWithoutMissing, got %v", retiredWithoutMissing.Validate())
	}

	asset := MediaAsset{
		ID:            "asset-1",
		VolumeID:      "vol-1",
		LibraryRootID: "root-1",
		RelPathBytes:  []byte("Series/v01.cbz"),
		Format:        MediaFormatCBZ,
		Digests: []AssetDigest{
			{AssetID: "asset-1", Kind: DigestKindXXH364Partial, Value: "old1", IsCurrent: false, Reason: DigestReasonScan, RecordedAt: now.Add(-time.Hour)},
			{AssetID: "asset-1", Kind: DigestKindXXH364Partial, Value: "curr1", IsCurrent: true, Reason: DigestReasonWriteBack, RecordedAt: now},
		},
	}
	if err := asset.Validate(); err != nil {
		t.Fatalf("valid MediaAsset failed Validate(): %v", err)
	}
	curr, ok := asset.CurrentDigest(DigestKindXXH364Partial)
	if !ok || curr.Value != "curr1" {
		t.Fatalf("expected current digest curr1, got (%+v, %v)", curr, ok)
	}
	if _, ok := asset.CurrentDigest(DigestKindXXH3128Full); ok {
		t.Fatal("did not expect XXH3_128_FULL digest")
	}
}
