package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestDomainToAppMappings(t *testing.T) {
	now := time.Now().UTC()
	volTitle := "Volume 8"
	pos := 8.0

	ser := domain.Series{
		ID:        "ser-1",
		LibraryID: "lib-1",
		Title:     "Discworld",
		TitleSort: "Discworld",
		Status:    domain.SeriesStatusOngoing,
		CreatedAt: now,
		UpdatedAt: now,
	}
	serDTO := SeriesFromDomain(ser)
	if serDTO.ID != ser.ID || serDTO.LibraryID != ser.LibraryID || serDTO.Title != ser.Title {
		t.Fatalf("SeriesFromDomain mismatch: %+v", serDTO)
	}

	missingAt := now.Add(-time.Hour)
	subPos := 2.0
	vol := domain.Volume{
		ID:        "vol-1",
		LibraryID: "lib-1",
		Kind:      domain.VolumeKindVolume,
		Title:     &volTitle,
		PageCount: 144, // Stale volume-level count (e.g. from an inaccessible asset) must not override chosen asset.
		Memberships: []domain.SeriesMembership{
			{VolumeID: "vol-1", SeriesID: "ser-sub", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: &subPos, Source: domain.MembershipSourceMetadata},
			{VolumeID: "vol-1", SeriesID: "ser-1", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver},
		},
		Assets: []domain.MediaAsset{
			{ID: "asset-live", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("d8.cbz"), Format: domain.MediaFormatCBZ, PageCount: 24, CreatedAt: now.Add(-time.Minute)},
			{ID: "asset-missing", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("d8m.cbz"), Format: domain.MediaFormatCBZ, PageCount: 144, MissingSince: &missingAt, CreatedAt: now},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	volDTO := VolumeFromDomain(vol)
	if volDTO.SeriesID == nil || *volDTO.SeriesID != "ser-1" {
		t.Fatalf("expected projected primary SeriesID ser-1, got %v", volDTO.SeriesID)
	}
	if volDTO.NumberSort != 8.0 || volDTO.AssetCount != 1 || volDTO.PageCount != 24 || volDTO.Title != "Volume 8" {
		t.Fatalf("VolumeFromDomain mismatch: %+v", volDTO)
	}
	if len(volDTO.SeriesMemberships) != 2 ||
		volDTO.SeriesMemberships[0].SeriesID != "ser-1" || !volDTO.SeriesMemberships[0].IsPrimary ||
		volDTO.SeriesMemberships[1].SeriesID != "ser-sub" || volDTO.SeriesMemberships[1].IsPrimary ||
		volDTO.SeriesMemberships[1].Position == nil || *volDTO.SeriesMemberships[1].Position != 2.0 {
		t.Fatalf("expected sorted visible memberships, got %+v", volDTO.SeriesMemberships)
	}

	// Hidden primary membership with visible secondary membership: series_id is null, secondary membership preserved.
	hiddenPrimary := domain.Volume{
		ID:        "vol-sec-only",
		LibraryID: "lib-1",
		Kind:      domain.VolumeKindVolume,
		PageCount: 99,
		Memberships: []domain.SeriesMembership{
			{VolumeID: "vol-sec-only", SeriesID: "ser-sub", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: &subPos, Source: domain.MembershipSourceMetadata},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	hiddenPrimaryDTO := VolumeFromDomain(hiddenPrimary)
	if hiddenPrimaryDTO.SeriesID != nil || hiddenPrimaryDTO.PageCount != 0 ||
		len(hiddenPrimaryDTO.SeriesMemberships) != 1 ||
		hiddenPrimaryDTO.SeriesMemberships[0].SeriesID != "ser-sub" || hiddenPrimaryDTO.SeriesMemberships[0].IsPrimary {
		t.Fatalf("unexpected hidden-primary VolumeFromDomain projection: %+v", hiddenPrimaryDTO)
	}

	standalone := domain.Volume{
		ID:        "vol-standalone",
		LibraryID: "lib-1",
		Kind:      domain.VolumeKindVolume,
		PageCount: 144,
		CreatedAt: now,
		UpdatedAt: now,
	}
	standaloneDTO := VolumeFromDomain(standalone)
	if standaloneDTO.SeriesID != nil || standaloneDTO.AssetCount != 0 || standaloneDTO.PageCount != 0 || !standaloneDTO.IsMissing {
		t.Fatalf("expected standalone nil SeriesID, 0 AssetCount, 0 PageCount, IsMissing=true, got %+v", standaloneDTO)
	}
	rawStandalone, err := json.Marshal(standaloneDTO)
	if err != nil {
		t.Fatalf("Marshal standaloneDTO: %v", err)
	}
	if !strings.Contains(string(rawStandalone), `"series_id":null`) || !strings.Contains(string(rawStandalone), `"series_memberships":[]`) {
		t.Fatalf("expected explicit null series_id and [] series_memberships in JSON, got %s", rawStandalone)
	}

	// Library mapping: ONLINE/OFFLINE aggregate state + admin vs reader path/error redaction.
	intake := domain.NewPathIdentity([]byte("/srv/intake"))
	rootErr := "stat root: permission denied"
	lib := domain.Library{
		ID:                    "lib-1",
		Name:                  "Manga",
		Mode:                  domain.LibraryModeManaged,
		IntakePath:            &intake,
		AutoMetadataWriteback: true,
		ResolverChain:         []domain.ResolverName{domain.ResolverFolder},
		MetadataPrecedence:    domain.MetadataPrecedenceEmbeddedOverOnline,
		DefaultCoverPolicy:    domain.CoverPolicyFirst,
		CreatedAt:             now,
	}
	roots := []domain.LibraryRoot{
		{
			ID:        "root-1",
			LibraryID: "lib-1",
			Path:      domain.NewPathIdentity([]byte("/mnt/r1")),
			State:     domain.LibraryRootStateOnline,
		},
		{
			ID:        "root-2",
			LibraryID: "lib-1",
			Path:      domain.NewPathIdentity([]byte("/mnt/r2")),
			State:     domain.LibraryRootStateOffline,
			LastError: &rootErr,
		},
	}

	adminRole := domain.Role{
		ID:          domain.RoleAdminID,
		Name:        "Admin",
		Permissions: []domain.Permission{domain.PermissionLibraryRead, domain.PermissionLibraryAdmin},
	}
	readerRole := domain.Role{
		ID:          domain.RoleReaderID,
		Name:        "Reader",
		Permissions: []domain.Permission{domain.PermissionLibraryRead},
	}
	adminPrincipal := domain.NewPrincipal(domain.User{ID: "u-admin", Username: "admin"}, []domain.Role{adminRole}, domain.CredentialKindPassword, "c1", nil)
	readerPrincipal := domain.NewPrincipal(domain.User{ID: "u-reader", Username: "reader"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c2", nil)

	adminLib := LibraryFromDomain(lib, roots, adminPrincipal)
	if adminLib.State != "OFFLINE" || adminLib.IntakePathDisplay == nil || *adminLib.IntakePathDisplay != "/srv/intake" {
		t.Fatalf("unexpected admin LibraryFromDomain projection: %+v", adminLib)
	}
	if len(adminLib.Roots) != 2 || adminLib.Roots[0].PathDisplay == nil || *adminLib.Roots[0].PathDisplay != "/mnt/r1" || adminLib.Roots[1].LastError == nil {
		t.Fatalf("expected admin root display paths and last_error preserved: %+v", adminLib.Roots)
	}

	readerLib := LibraryFromDomain(lib, roots, readerPrincipal)
	if readerLib.State != "OFFLINE" || readerLib.IntakePathDisplay != nil {
		t.Fatalf("expected reader intake_path_display redacted, got %+v", readerLib)
	}
	for _, r := range readerLib.Roots {
		if r.PathDisplay != nil || r.LastError != nil {
			t.Fatalf("expected reader root path_display and last_error redacted, got %+v", r)
		}
	}

	onlineLib := LibraryFromDomain(lib, roots[:1], readerPrincipal)
	if onlineLib.State != "ONLINE" {
		t.Fatalf("expected ONLINE state when all roots are ONLINE, got %q", onlineLib.State)
	}
}

func TestLocatorAppMappingRoundTrip(t *testing.T) {
	digest := &domain.LocatorDigest{Kind: domain.DigestKindXXH364Partial, Value: "deadbeef"}

	cases := []struct {
		name string
		dom  domain.Locator
	}{
		{
			name: "zero page locator",
			dom:  domain.NewPageLocator(0, 0, "asset-1", digest),
		},
		{
			name: "zero text progression locator",
			dom:  domain.NewTextLocator("OEBPS/ch01.xhtml", 0.0, nil, "asset-2", digest),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dto := LocatorFromDomain(tc.dom)
			got, err := LocatorToDomain(dto)
			if err != nil {
				t.Fatalf("LocatorToDomain error: %v", err)
			}
			if got.Type != tc.dom.Type || got.AssetID != tc.dom.AssetID {
				t.Fatalf("metadata mismatch: got %+v, want %+v", got, tc.dom)
			}
			if got.AssetDigest == nil || *got.AssetDigest != *tc.dom.AssetDigest {
				t.Fatalf("digest mismatch: got %+v, want %+v", got.AssetDigest, tc.dom.AssetDigest)
			}
			if tc.dom.Page != nil && (got.Page == nil || *got.Page != *tc.dom.Page) {
				t.Fatalf("page mismatch: got %v, want %v", got.Page, *tc.dom.Page)
			}
			if tc.dom.Progression != nil && (got.Progression == nil || *got.Progression != *tc.dom.Progression) {
				t.Fatalf("progression mismatch: got %v, want %v", got.Progression, *tc.dom.Progression)
			}
		})
	}

	t.Run("partial client DTO locator without source identity remains valid shape", func(t *testing.T) {
		got, err := LocatorToDomain(Locator{Type: "page", Page: 0})
		if err != nil {
			t.Fatalf("LocatorToDomain error: %v", err)
		}
		if got.Page == nil || *got.Page != 0 {
			t.Fatalf("expected page 0 preserved, got %v", got.Page)
		}
	})
}

func TestReadingProgressAndJobInfoMappings(t *testing.T) {
	now := time.Now().UTC()
	dev := "Pixel"
	digest := &domain.LocatorDigest{Kind: domain.DigestKindXXH364Partial, Value: "deadbeef"}
	prog := ReadingProgressFromDomain(domain.ReadingProgress{
		UserID:           "u1",
		VolumeID:         "v1",
		Locator:          domain.NewPageLocator(0, 50, "a1", digest),
		TotalProgression: 0.0,
		Device:           &dev,
		Source:           "KOMGA",
		Completed:        false,
		UpdatedAt:        now,
	})
	if prog.Device != "Pixel" || prog.Locator.Page != 0 || prog.Locator.PageCount != 50 || prog.Locator.AssetDigest != "XXH3_64_PARTIAL:deadbeef" {
		t.Fatalf("ReadingProgressFromDomain mismatch: %+v", prog)
	}

	creator := "u1"
	errMsg := "boom"
	jobInfo := JobInfoFromDomain(domain.Job{
		ID:              "j1",
		Kind:            domain.JobKindScan,
		State:           domain.JobStateSucceeded,
		Attempts:        1,
		MaxAttempts:     3,
		CancelRequested: true,
		CreatedBy:       &creator,
		LastError:       &errMsg,
		CreatedAt:       now,
		UpdatedAt:       now,
		FinishedAt:      &now,
	})
	if jobInfo.ProgressPct != 100 || jobInfo.State != "SUCCEEDED" ||
		!jobInfo.CancelRequested || jobInfo.Attempts != 1 || jobInfo.MaxAttempts != 3 ||
		jobInfo.CreatedBy == nil || *jobInfo.CreatedBy != creator ||
		jobInfo.Error == nil || *jobInfo.Error != errMsg ||
		string(jobInfo.Checkpoint) != "{}" {
		t.Fatalf("JobInfoFromDomain mismatch: %+v", jobInfo)
	}
}

func TestVolumeFromDomain_LocatorAssetPreferenceAndJSONShape(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t1.Add(time.Hour)

	oldest := domain.MediaAsset{ID: "asset-oldest", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("v1.cbz"), Format: domain.MediaFormatCBZ, PageCount: 18, CreatedAt: t0}
	locator := domain.MediaAsset{ID: "asset-locator", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("v2.cbz"), Format: domain.MediaFormatCBZ, PageCount: 42, CreatedAt: t1}
	manual := domain.MediaAsset{ID: "asset-manual", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("v3.cbz"), Format: domain.MediaFormatCBZ, PageCount: 55, CreatedAt: t2}

	vol := domain.Volume{
		ID:             "vol-1",
		LibraryID:      "lib-1",
		Kind:           domain.VolumeKindVolume,
		PageCount:      18,
		LocatorAssetID: locator.ID,
		Assets:         []domain.MediaAsset{oldest, locator, manual},
		CreatedAt:      t0,
		UpdatedAt:      t2,
	}

	dto := VolumeFromDomain(vol)
	if dto.PageCount != 42 {
		t.Fatalf("VolumeFromDomain locator PageCount = %d, want 42", dto.PageCount)
	}

	// Explicit PreferredAssetID still takes precedence over LocatorAssetID.
	vol.PreferredAssetID = &manual.ID
	dtoManual := VolumeFromDomain(vol)
	if dtoManual.PageCount != 55 {
		t.Fatalf("VolumeFromDomain manual PreferredAssetID PageCount = %d, want 55", dtoManual.PageCount)
	}

	// Fallback to oldest available when neither PreferredAssetID nor LocatorAssetID matches an available asset.
	vol.PreferredAssetID = nil
	vol.LocatorAssetID = "asset-unavailable"
	dtoFallback := VolumeFromDomain(vol)
	if dtoFallback.PageCount != 18 {
		t.Fatalf("VolumeFromDomain fallback PageCount = %d, want 18", dtoFallback.PageCount)
	}

	// Trashed manual preference and trashed locator are skipped in favor of the oldest readable variant.
	vol.Assets[2].Trashed = true
	vol.Assets[1].Trashed = true
	vol.PreferredAssetID = &manual.ID
	vol.LocatorAssetID = locator.ID
	dtoTrashedPrefAndLoc := VolumeFromDomain(vol)
	if dtoTrashedPrefAndLoc.PageCount != 18 {
		t.Fatalf("VolumeFromDomain trashed preference+locator PageCount = %d, want 18", dtoTrashedPrefAndLoc.PageCount)
	}

	raw, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("json.Marshal(Volume): %v", err)
	}
	if strings.Contains(string(raw), "locator_asset_id") || strings.Contains(string(raw), "LocatorAssetID") ||
		strings.Contains(string(raw), "trashed") || strings.Contains(string(raw), "Trashed") {
		t.Fatalf("Volume DTO JSON shape unexpectedly contains internal fields: %s", raw)
	}
}
