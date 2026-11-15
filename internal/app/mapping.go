package app

import (
	"encoding/json"

	"github.com/inexcession/tobiten/internal/domain"
)

// SeriesFromDomain projects a domain Series into the application Series DTO.
func SeriesFromDomain(s domain.Series) Series {
	return Series{
		ID:        s.ID,
		LibraryID: s.LibraryID,
		Title:     s.Title,
		CreatedAt: s.CreatedAt,
		UpdatedAt: s.UpdatedAt,
	}
}

// VolumeFromDomain projects a domain Volume into the application Volume DTO.
// SeriesID is derived strictly from the volume's primary SeriesMembership (ADR-0008 §5).
func VolumeFromDomain(v domain.Volume) Volume {
	var seriesID *string
	if id, ok := v.PrimarySeriesID(); ok {
		seriesID = &id
	}

	var title string
	if v.Title != nil {
		title = *v.Title
	}

	var numberSort float64
	if pm, ok := v.PrimaryMembership(); ok && pm.Position != nil {
		numberSort = *pm.Position
	} else if v.Number != nil {
		numberSort = *v.Number
	}

	return Volume{
		ID:         v.ID,
		LibraryID:  v.LibraryID,
		SeriesID:   seriesID,
		Title:      title,
		NumberSort: numberSort,
		AssetCount: len(v.AvailableAssets()),
		CreatedAt:  v.CreatedAt,
		UpdatedAt:  v.UpdatedAt,
	}
}

// LocatorFromDomain converts a typed domain Locator into the application Locator DTO.
func LocatorFromDomain(l domain.Locator) Locator {
	out := Locator{
		Type:    string(l.Type),
		AssetID: l.AssetID,
		Href:    l.Href,
		CFI:     l.CFI,
	}
	if l.AssetDigest != nil {
		out.AssetDigest = l.AssetDigest.String()
	}
	if l.Page != nil {
		out.Page = *l.Page
	}
	if l.PageCount != nil {
		out.PageCount = *l.PageCount
	}
	if l.Progression != nil {
		out.Progression = *l.Progression
	}
	return out
}

// LocatorToDomain converts an application Locator DTO into a validated domain Locator.
func LocatorToDomain(l Locator) (domain.Locator, error) {
	digest, err := domain.ParseLocatorDigest(l.AssetDigest)
	if err != nil {
		return domain.Locator{}, err
	}

	out := domain.Locator{
		Type:        domain.LocatorType(l.Type),
		AssetID:     l.AssetID,
		AssetDigest: digest,
		Href:        l.Href,
		CFI:         l.CFI,
	}

	switch out.Type {
	case domain.LocatorTypePage:
		page := l.Page
		pageCount := l.PageCount
		out.Page = &page
		out.PageCount = &pageCount
		if l.Progression != 0 {
			prog := l.Progression
			out.Progression = &prog
		}
	case domain.LocatorTypeText:
		prog := l.Progression
		out.Progression = &prog
		if l.Page != 0 {
			page := l.Page
			out.Page = &page
		}
		if l.PageCount != 0 {
			pc := l.PageCount
			out.PageCount = &pc
		}
	}

	if err := out.Validate(); err != nil {
		return domain.Locator{}, err
	}
	return out, nil
}

// ReadingProgressFromDomain projects a domain ReadingProgress into the application DTO.
func ReadingProgressFromDomain(p domain.ReadingProgress) ReadingProgress {
	var device string
	if p.Device != nil {
		device = *p.Device
	}
	return ReadingProgress{
		UserID:           p.UserID,
		VolumeID:         p.VolumeID,
		Completed:        p.Completed,
		TotalProgression: p.TotalProgression,
		Locator:          LocatorFromDomain(p.Locator),
		Device:           device,
		Source:           p.Source,
		UpdatedAt:        p.UpdatedAt,
	}
}

// JobInfoFromDomain projects a domain Job into the application JobInfo DTO.
func JobInfoFromDomain(j domain.Job) JobInfo {
	var progressPct float64
	if j.State == domain.JobStateSucceeded {
		progressPct = 100
	} else if len(j.Checkpoint) > 0 {
		var cp struct {
			ProgressPct float64 `json:"progress_pct"`
		}
		if err := json.Unmarshal(j.Checkpoint, &cp); err == nil && cp.ProgressPct > 0 {
			progressPct = cp.ProgressPct
		}
	}

	return JobInfo{
		ID:          j.ID,
		Kind:        string(j.Kind),
		State:       string(j.State),
		ProgressPct: progressPct,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		FinishedAt:  j.FinishedAt,
		Error:       j.LastError,
	}
}
