package app

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// ComputeLibraryState computes a library's aggregate ONLINE/OFFLINE state from its configured roots (ADR-0019 §5).
// A library is ONLINE iff every configured root is ONLINE.
func ComputeLibraryState(roots []domain.LibraryRoot) domain.LibraryRootState {
	for _, r := range roots {
		if r.State != domain.LibraryRootStateOnline {
			return domain.LibraryRootStateOffline
		}
	}
	return domain.LibraryRootStateOnline
}

// LibraryFromDomain projects a domain Library and its roots into the application Library DTO,
// redacting host filesystem display paths (and root last_error values that embed path errors)
// unless principal holds domain.PermissionLibraryAdmin.
func LibraryFromDomain(lib domain.Library, roots []domain.LibraryRoot, principal *domain.Principal) Library {
	isAdmin := principal != nil && principal.HasPermission(domain.PermissionLibraryAdmin)

	rootDTOs := make([]LibraryRoot, 0, len(roots))
	for _, r := range roots {
		dto := LibraryRoot{
			ID:            r.ID,
			LibraryID:     r.LibraryID,
			InvalidUTF8:   r.Path.InvalidUTF8,
			State:         string(r.State),
			LastCheckedAt: cloneTimePtr(r.LastCheckedAt),
		}
		if isAdmin {
			display := r.Path.Display
			dto.PathDisplay = &display
			dto.LastError = cloneStringPtr(r.LastError)
		}
		rootDTOs = append(rootDTOs, dto)
	}

	var intakeDisplay *string
	if isAdmin && lib.IntakePath != nil {
		display := lib.IntakePath.Display
		intakeDisplay = &display
	}

	resolvers := make([]string, len(lib.ResolverChain))
	for i, r := range lib.ResolverChain {
		resolvers[i] = string(r)
	}

	return Library{
		ID:                    lib.ID,
		Name:                  lib.Name,
		Mode:                  string(lib.Mode),
		State:                 string(ComputeLibraryState(roots)),
		Roots:                 rootDTOs,
		IntakePathDisplay:     intakeDisplay,
		AutoMetadataWriteback: lib.AutoMetadataWriteback,
		ResolverChain:         resolvers,
		MetadataPrecedence:    string(lib.MetadataPrecedence),
		DefaultCoverPolicy:    string(lib.DefaultCoverPolicy),
		CreatedAt:             lib.CreatedAt.UTC(),
	}
}

// SeriesFromDomain projects a domain Series into the application Series DTO.
func SeriesFromDomain(s domain.Series) Series {
	genres := []string{}
	if len(s.Genres) > 0 {
		genres = slices.Clone(s.Genres)
	}
	tags := []string{}
	if len(s.Tags) > 0 {
		tags = slices.Clone(s.Tags)
	}
	return Series{
		ID:            s.ID,
		LibraryID:     s.LibraryID,
		Title:         s.Title,
		TitleSort:     s.TitleSort,
		Language:      cloneStringPtr(s.Language),
		OriginalTitle: cloneStringPtr(s.OriginalTitle),
		RomajiTitle:   cloneStringPtr(s.RomajiTitle),
		Franchise:     cloneStringPtr(s.Franchise),
		EditionName:   cloneStringPtr(s.EditionName),
		Publisher:     cloneStringPtr(s.Publisher),
		Imprint:       cloneStringPtr(s.Imprint),
		Status:        string(s.Status),
		Summary:       cloneStringPtr(s.Summary),
		AgeRating:     cloneStringPtr(s.AgeRating),
		Genres:        genres,
		Tags:          tags,
		TotalVolumes:  cloneIntPtr(s.TotalVolumes),
		ReleaseYear:   cloneIntPtr(s.ReleaseYear),
		CreatedAt:     s.CreatedAt,
		UpdatedAt:     s.UpdatedAt,
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

	pageCount := 0
	if chosen, ok := v.SelectPreferredAsset("", func(a domain.MediaAsset) bool {
		return a.IsAvailable()
	}); ok && chosen != nil {
		pageCount = chosen.PageCount
	}

	memberships := make([]SeriesMembership, 0, len(v.Memberships))
	for _, m := range v.Memberships {
		memberships = append(memberships, SeriesMembership{
			SeriesID:  m.SeriesID,
			IsPrimary: m.IsPrimary,
			Kind:      string(m.Kind),
			Position:  cloneFloat64Ptr(m.Position),
		})
	}
	slices.SortFunc(memberships, func(a, b SeriesMembership) int {
		if a.IsPrimary != b.IsPrimary {
			if a.IsPrimary {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.SeriesID, b.SeriesID)
	})

	return Volume{
		ID:                v.ID,
		LibraryID:         v.LibraryID,
		SeriesID:          seriesID,
		SeriesMemberships: memberships,
		Kind:              string(v.Kind),
		Number:            cloneFloat64Ptr(v.Number),
		ChapterStart:      cloneFloat64Ptr(v.ChapterStart),
		ChapterEnd:        cloneFloat64Ptr(v.ChapterEnd),
		Title:             title,
		Summary:           cloneStringPtr(v.Summary),
		ReleaseDate:       cloneTimePtr(v.ReleaseDate),
		NumberSort:        numberSort,
		PageCount:         pageCount,
		AssetCount:        len(v.AvailableAssets()),
		PreferredAssetID:  cloneStringPtr(v.PreferredAssetID),
		IsMissing:         v.IsMissing(),
		CreatedAt:         v.CreatedAt,
		UpdatedAt:         v.UpdatedAt,
	}
}

func cloneStringPtr(v *string) *string {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneIntPtr(v *int) *int {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneFloat64Ptr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneTimePtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	cp := v.UTC()
	return &cp
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

	cp := json.RawMessage("{}")
	if len(j.Checkpoint) > 0 && json.Valid(j.Checkpoint) {
		cp = make(json.RawMessage, len(j.Checkpoint))
		copy(cp, j.Checkpoint)
	}

	return JobInfo{
		ID:              j.ID,
		Kind:            string(j.Kind),
		State:           string(j.State),
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		CancelRequested: j.CancelRequested,
		ProgressPct:     progressPct,
		Checkpoint:      cp,
		CreatedBy:       copyStringPtr(j.CreatedBy),
		DedupeKey:       copyStringPtr(j.DedupeKey),
		RunAfter:        j.RunAfter,
		CreatedAt:       j.CreatedAt,
		StartedAt:       j.StartedAt,
		UpdatedAt:       j.UpdatedAt,
		FinishedAt:      j.FinishedAt,
		Error:           copyStringPtr(j.LastError),
	}
}

func copyStringPtr(src *string) *string {
	if src == nil {
		return nil
	}
	v := *src
	return &v
}
