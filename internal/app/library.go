package app

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

const (
	// DefaultLibraryPageLimit is the default page size for paginated library/catalogue read routes.
	DefaultLibraryPageLimit = 50
	// MaxLibraryPageLimit is the hard maximum page size enforced across paginated read routes (ADR-0020 §7).
	MaxLibraryPageLimit = 200
)

// LibraryRoot represents one configured filesystem root for a library (ADR-0019 §2, §5).
// PathDisplay and LastError are returned only to principals holding domain.PermissionLibraryAdmin;
// non-admin readers receive explicit JSON null for both fields.
type LibraryRoot struct {
	ID            string     `json:"id"`
	LibraryID     string     `json:"library_id"`
	PathDisplay   *string    `json:"path_display"`
	InvalidUTF8   bool       `json:"invalid_utf8"`
	State         string     `json:"state"` // ONLINE or OFFLINE
	LastCheckedAt *time.Time `json:"last_checked_at"`
	LastError     *string    `json:"last_error"`
}

// Library represents a configured multi-root library container (ADR-0002 §1, ADR-0019 §2, §5).
// Optional admin-only or unconfigured fields emit explicit JSON null (ADR-0020 §4).
type Library struct {
	ID                    string        `json:"id"`
	Name                  string        `json:"name"`
	Mode                  string        `json:"mode"`  // VIRTUAL or MANAGED
	State                 string        `json:"state"` // ONLINE or OFFLINE
	Roots                 []LibraryRoot `json:"roots"`
	IntakePathDisplay     *string       `json:"intake_path_display"`
	AutoMetadataWriteback bool          `json:"auto_metadata_writeback"`
	ResolverChain         []string      `json:"resolver_chain"`
	MetadataPrecedence    string        `json:"metadata_precedence"`
	DefaultCoverPolicy    string        `json:"default_cover_policy"`
	CreatedAt             time.Time     `json:"created_at"`
}

// Series represents a logical series containing volumes (ADR-0019, ADR-0020 §4).
type Series struct {
	ID            string    `json:"id"`
	LibraryID     string    `json:"library_id"`
	Title         string    `json:"title"`
	TitleSort     string    `json:"title_sort"`
	Language      *string   `json:"language"`
	OriginalTitle *string   `json:"original_title"`
	RomajiTitle   *string   `json:"romaji_title"`
	Franchise     *string   `json:"franchise"`
	EditionName   *string   `json:"edition_name"`
	Publisher     *string   `json:"publisher"`
	Imprint       *string   `json:"imprint"`
	Status        string    `json:"status"`
	Summary       *string   `json:"summary"`
	AgeRating     *string   `json:"age_rating"`
	Genres        []string  `json:"genres"`
	Tags          []string  `json:"tags"`
	TotalVolumes  *int      `json:"total_volumes"`
	ReleaseYear   *int      `json:"release_year"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Volume represents a bibliographic unit with zero or more manifestations (ADR-0018 S5, ADR-0020 §4).
// SeriesID is explicitly serialized as null for standalone volumes (zero series_memberships).
type Volume struct {
	ID               string     `json:"id"`
	LibraryID        string     `json:"library_id"`
	SeriesID         *string    `json:"series_id"`
	Kind             string     `json:"kind"`
	Number           *float64   `json:"number"`
	ChapterStart     *float64   `json:"chapter_start"`
	ChapterEnd       *float64   `json:"chapter_end"`
	Title            string     `json:"title"`
	Summary          *string    `json:"summary"`
	ReleaseDate      *time.Time `json:"release_date"`
	NumberSort       float64    `json:"number_sort"`
	PageCount        int        `json:"page_count"`
	AssetCount       int        `json:"asset_count"`
	PreferredAssetID *string    `json:"preferred_asset_id"`
	IsMissing        bool       `json:"is_missing"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// FolderEntry represents one immediate child folder or available file in a library folder listing (ADR-0019 §4).
// File-specific fields are explicitly serialized as null on FOLDER entries (ADR-0020 §4).
type FolderEntry struct {
	Kind           string  `json:"kind"` // FOLDER or FILE
	NameDisplay    string  `json:"name_display"`
	NameHex        string  `json:"name_hex"`
	RelPathDisplay string  `json:"rel_path_display"`
	RelPathHex     string  `json:"rel_path_hex"`
	InvalidUTF8    bool    `json:"invalid_utf8"`
	AssetID        *string `json:"asset_id"`
	VolumeID       *string `json:"volume_id"`
	LibraryRootID  *string `json:"library_root_id"`
	Format         *string `json:"format"`
	FileSize       *int64  `json:"file_size"`
	PageCount      *int    `json:"page_count"`
}

// FolderBrowseResult holds one paginated slice of immediate child folders and files (ADR-0019 §4, ADR-0020 §7).
type FolderBrowseResult struct {
	LibraryID         string        `json:"library_id"`
	FolderPathDisplay string        `json:"folder_path_display"`
	FolderPathHex     string        `json:"folder_path_hex"`
	InvalidUTF8       bool          `json:"invalid_utf8"`
	Entries           []FolderEntry `json:"entries"`
	TotalCount        int64         `json:"total_count"`
	Limit             int           `json:"limit"`
	Offset            int           `json:"offset"`
}

// LibraryFilter specifies pagination criteria for listing libraries.
type LibraryFilter struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// FolderBrowseFilter specifies criteria for browsing folders inside a library.
type FolderBrowseFilter struct {
	LibraryID       string
	FolderPathBytes []byte
	Limit           int
	Offset          int
}

// LibraryPage holds one server-paginated page of libraries (ADR-0020 §7).
type LibraryPage struct {
	Items      []Library `json:"items"`
	TotalCount int64     `json:"total_count"`
	Limit      int       `json:"limit"`
	Offset     int       `json:"offset"`
}

// SeriesPage holds one server-paginated page of series (ADR-0020 §7).
type SeriesPage struct {
	Items      []Series `json:"items"`
	TotalCount int64    `json:"total_count"`
	Limit      int      `json:"limit"`
	Offset     int      `json:"offset"`
}

// VolumePage holds one server-paginated page of volumes (ADR-0020 §7).
type VolumePage struct {
	Items      []Volume `json:"items"`
	TotalCount int64    `json:"total_count"`
	Limit      int      `json:"limit"`
	Offset     int      `json:"offset"`
}

// SeriesFilter specifies criteria for listing series.
type SeriesFilter struct {
	LibraryID string `json:"library_id,omitempty"`
	Search    string `json:"search,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// VolumeFilter specifies criteria for listing volumes.
type VolumeFilter struct {
	LibraryID string `json:"library_id,omitempty"`
	SeriesID  string `json:"series_id,omitempty"`
	Search    string `json:"search,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// LibraryService defines use-cases for library catalogue, folder browsing, series, volumes, and scanning.
type LibraryService interface {
	ListLibraries(ctx context.Context, principal *domain.Principal, filter LibraryFilter) (*LibraryPage, error)
	GetLibrary(ctx context.Context, principal *domain.Principal, id string) (*Library, error)
	BrowseFolder(ctx context.Context, principal *domain.Principal, filter FolderBrowseFilter) (*FolderBrowseResult, error)
	ListSeries(ctx context.Context, principal *domain.Principal, filter SeriesFilter) (*SeriesPage, error)
	GetSeries(ctx context.Context, principal *domain.Principal, id string) (*Series, error)
	ListVolumes(ctx context.Context, principal *domain.Principal, filter VolumeFilter) (*VolumePage, error)
	GetVolume(ctx context.Context, principal *domain.Principal, id string) (*Volume, error)
	ScanLibrary(ctx context.Context, principal *domain.Principal, libraryID string) (string, error)
}
