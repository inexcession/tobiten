package app

import (
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
	vol := domain.Volume{
		ID:        "vol-1",
		LibraryID: "lib-1",
		Kind:      domain.VolumeKindVolume,
		Title:     &volTitle,
		Memberships: []domain.SeriesMembership{
			{VolumeID: "vol-1", SeriesID: "ser-sub", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: &pos, Source: domain.MembershipSourceMetadata},
			{VolumeID: "vol-1", SeriesID: "ser-1", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver},
		},
		Assets: []domain.MediaAsset{
			{ID: "asset-live", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("d8.cbz"), Format: domain.MediaFormatCBZ},
			{ID: "asset-missing", VolumeID: "vol-1", LibraryRootID: "r1", RelPathBytes: []byte("d8m.cbz"), Format: domain.MediaFormatCBZ, MissingSince: &missingAt},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	volDTO := VolumeFromDomain(vol)
	if volDTO.SeriesID == nil || *volDTO.SeriesID != "ser-1" {
		t.Fatalf("expected projected primary SeriesID ser-1, got %v", volDTO.SeriesID)
	}
	if volDTO.NumberSort != 8.0 || volDTO.AssetCount != 1 || volDTO.Title != "Volume 8" {
		t.Fatalf("VolumeFromDomain mismatch: %+v", volDTO)
	}

	standalone := domain.Volume{
		ID:        "vol-standalone",
		LibraryID: "lib-1",
		Kind:      domain.VolumeKindVolume,
		CreatedAt: now,
		UpdatedAt: now,
	}
	standaloneDTO := VolumeFromDomain(standalone)
	if standaloneDTO.SeriesID != nil || standaloneDTO.AssetCount != 0 {
		t.Fatalf("expected standalone nil SeriesID and 0 AssetCount, got %+v", standaloneDTO)
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
