package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ storage.LibraryRepository = (*LibraryRepository)(nil)

// libraryConfigLockID is a dedicated transaction-scoped advisory lock key ("tobitenL")
// serializing root and intake path configuration checks and mutations across transactions.
const libraryConfigLockID int64 = 0x746f626974656e4c

// LibraryRepository implements storage.LibraryRepository backed by PostgreSQL and sqlc.
type LibraryRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
}

// NewLibraryRepository creates a new LibraryRepository using the provided database handle.
func NewLibraryRepository(db DBTX) *LibraryRepository {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	var scope *txScope
	if extTx, ok := db.(pgx.Tx); ok {
		scope = newCallerTxScope(extTx)
	}
	return &LibraryRepository{
		q:     New(db),
		tx:    beginner,
		scope: scope,
	}
}

// WithTx executes fn inside a transaction (or nested savepoint when already inside one).
func (r *LibraryRepository) WithTx(ctx context.Context, fn func(repo storage.LibraryRepository) error) error {
	if r == nil {
		return errors.New("library repository is required")
	}
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	return runTx(ctx, r.tx, r.scope, "library", func(child *txScope) error {
		return fn(&LibraryRepository{q: New(child.tx), tx: child.tx, scope: child})
	})
}

// CreateLibrary persists a new library record.
func (r *LibraryRepository) CreateLibrary(ctx context.Context, library *domain.Library) error {
	candidate, err := prepareLibraryForCreate(library)
	if err != nil {
		return err
	}

	intakeBytes, intakeDisplay, intakeMatch := splitOptionalPathIdentity(candidate.IntakePath)
	row, err := r.q.CreateLibrary(ctx, CreateLibraryParams{
		ID:                    candidate.ID,
		Name:                  candidate.Name,
		Mode:                  string(candidate.Mode),
		IntakePathBytes:       intakeBytes,
		IntakePathDisplay:     intakeDisplay,
		IntakePathMatch:       intakeMatch,
		AutoMetadataWriteback: candidate.AutoMetadataWriteback,
		ResolverChain:         resolverChainToStrings(candidate.ResolverChain),
		MetadataPrecedence:    string(candidate.MetadataPrecedence),
		DefaultCoverPolicy:    string(candidate.DefaultCoverPolicy),
		CreatedAt:             candidate.CreatedAt,
	})
	if err != nil {
		return mapPgError(err)
	}
	*library = toDomainLibrary(row)
	return nil
}

// GetLibraryByID retrieves a library by its ID.
func (r *LibraryRepository) GetLibraryByID(ctx context.Context, id string) (*domain.Library, error) {
	row, err := r.q.GetLibraryByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	lib := toDomainLibrary(row)
	return &lib, nil
}

// GetLibraryByName retrieves a library by its unique name.
func (r *LibraryRepository) GetLibraryByName(ctx context.Context, name string) (*domain.Library, error) {
	row, err := r.q.GetLibraryByName(ctx, name)
	if err != nil {
		return nil, mapPgError(err)
	}
	lib := toDomainLibrary(row)
	return &lib, nil
}

// ListLibraries returns all configured libraries ordered by creation time and ID.
func (r *LibraryRepository) ListLibraries(ctx context.Context) ([]domain.Library, error) {
	rows, err := r.q.ListLibraries(ctx)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLibraries(rows), nil
}

// ListLibrariesPaged returns one server-paginated slice of libraries together with total_count
// computed in the same query snapshot, capping page size at storage.MaxLibraryPageLimit (ADR-0020 §7).
func (r *LibraryRepository) ListLibrariesPaged(ctx context.Context, limit, offset int) (*storage.LibraryPageResult, error) {
	if limit < 0 || offset < 0 || offset > math.MaxInt32 {
		return nil, fmt.Errorf("invalid pagination limit=%d offset=%d", limit, offset)
	}
	if limit == 0 {
		limit = storage.DefaultLibraryPageLimit
	} else if limit > storage.MaxLibraryPageLimit {
		limit = storage.MaxLibraryPageLimit
	}

	rows, err := r.q.ListLibrariesPaged(ctx, ListLibrariesPagedParams{
		PageLimit:  int32(limit),
		PageOffset: int32(offset),
	})
	if err != nil {
		return nil, mapPgError(err)
	}

	var total int64
	items := []domain.Library{}
	for _, row := range rows {
		total = row.TotalCount
		if !row.HasRow {
			continue
		}
		lib, err := toDomainLibraryFromPagedRow(row)
		if err != nil {
			return nil, err
		}
		items = append(items, lib)
	}
	return &storage.LibraryPageResult{
		Items:      items,
		TotalCount: total,
		Limit:      limit,
		Offset:     offset,
	}, nil
}

// UpdateLibrary persists changes to an existing library record.
func (r *LibraryRepository) UpdateLibrary(ctx context.Context, library *domain.Library) error {
	if library == nil {
		return errors.New("library cannot be nil")
	}
	if err := library.Validate(); err != nil {
		return err
	}

	intakeBytes, intakeDisplay, intakeMatch := splitOptionalPathIdentity(library.IntakePath)
	row, err := r.q.UpdateLibrary(ctx, UpdateLibraryParams{
		ID:                    library.ID,
		Name:                  library.Name,
		Mode:                  string(library.Mode),
		IntakePathBytes:       intakeBytes,
		IntakePathDisplay:     intakeDisplay,
		IntakePathMatch:       intakeMatch,
		AutoMetadataWriteback: library.AutoMetadataWriteback,
		ResolverChain:         resolverChainToStrings(library.ResolverChain),
		MetadataPrecedence:    string(library.MetadataPrecedence),
		DefaultCoverPolicy:    string(library.DefaultCoverPolicy),
	})
	if err != nil {
		return mapPgError(err)
	}
	*library = toDomainLibrary(row)
	return nil
}

func (r *LibraryRepository) runLibraryTx(ctx context.Context, label string, fn func(q *Queries) error) error {
	if r == nil {
		return errors.New("library repository is required")
	}
	if r.scope == nil {
		return runReadCommittedTx(ctx, r.tx, label, func(_ pgx.Tx, q *Queries) error {
			return fn(q)
		})
	}
	return runTx(ctx, r.tx, r.scope, label, func(child *txScope) error {
		return fn(New(child.tx))
	})
}

// DeleteLibrary deletes a library by ID after locking its row and verifying it owns no
// catalogue records (series or volumes), preserving standalone volumes and reading progress.
func (r *LibraryRepository) DeleteLibrary(ctx context.Context, id string) error {
	return r.runLibraryTx(ctx, "library delete", func(q *Queries) error {
		if _, err := q.LockLibrary(ctx, id); err != nil {
			return mapPgError(err)
		}
		hasRows, err := q.LibraryHasCatalogueRows(ctx, id)
		if err != nil {
			return mapPgError(err)
		}
		if hasRows {
			return storage.ErrLibraryInUse
		}
		rows, err := q.DeleteLibrary(ctx, id)
		if err != nil {
			return mapRootDeleteError(err)
		}
		if rows == 0 {
			return storage.ErrNotFound
		}
		return nil
	})
}

// CreateLibraryRoot persists a new root directory for an existing library.
func (r *LibraryRepository) CreateLibraryRoot(ctx context.Context, root *domain.LibraryRoot) error {
	candidate, err := prepareLibraryRootForCreate(root)
	if err != nil {
		return err
	}

	row, err := r.q.CreateLibraryRoot(ctx, CreateLibraryRootParams{
		ID:            candidate.ID,
		LibraryID:     candidate.LibraryID,
		PathBytes:     slices.Clone(candidate.Path.Bytes),
		PathDisplay:   candidate.Path.Display,
		PathMatch:     candidate.Path.Match,
		State:         string(candidate.State),
		LastCheckedAt: normalizeOptionalTimeUTC(candidate.LastCheckedAt),
		LastError:     cloneStringPtr(candidate.LastError),
	})
	if err != nil {
		return mapRootWriteError(err)
	}
	*root = toDomainLibraryRoot(row)
	return nil
}

// GetLibraryRootByID retrieves a library root by its ID.
func (r *LibraryRepository) GetLibraryRootByID(ctx context.Context, id string) (*domain.LibraryRoot, error) {
	row, err := r.q.GetLibraryRootByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	root := toDomainLibraryRoot(row)
	return &root, nil
}

// ListLibraryRoots returns all roots belonging to libraryID ordered by match key and ID.
func (r *LibraryRepository) ListLibraryRoots(ctx context.Context, libraryID string) ([]domain.LibraryRoot, error) {
	rows, err := r.q.ListLibraryRootsByLibrary(ctx, libraryID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLibraryRoots(rows), nil
}

// ListLibraryRootsForNoKeyUpdate returns all roots belonging to libraryID ordered by ID
// with row-level FOR NO KEY UPDATE locks inside an active transaction.
func (r *LibraryRepository) ListLibraryRootsForNoKeyUpdate(ctx context.Context, libraryID string) ([]domain.LibraryRoot, error) {
	if r == nil || r.scope == nil {
		return nil, storage.ErrTransactionRequired
	}
	if strings.TrimSpace(libraryID) == "" {
		return nil, domain.ErrEmptyLibraryID
	}
	rows, err := r.q.ListLibraryRootsByLibraryForNoKeyUpdate(ctx, libraryID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLibraryRoots(rows), nil
}

// ListAllLibraryRoots returns all roots across all libraries ordered by library ID, match key, and ID.
func (r *LibraryRepository) ListAllLibraryRoots(ctx context.Context) ([]domain.LibraryRoot, error) {
	rows, err := r.q.ListAllLibraryRoots(ctx)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLibraryRoots(rows), nil
}

// ListLibraryRootsByLibraryIDs returns all roots belonging to any library in libraryIDs,
// ordered by library ID, match key, and ID.
func (r *LibraryRepository) ListLibraryRootsByLibraryIDs(ctx context.Context, libraryIDs []string) ([]domain.LibraryRoot, error) {
	if len(libraryIDs) == 0 {
		return []domain.LibraryRoot{}, nil
	}
	rows, err := r.q.ListLibraryRootsByLibraryIDs(ctx, libraryIDs)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLibraryRoots(rows), nil
}

// UpdateLibraryRoot persists changes to an existing library root.
func (r *LibraryRepository) UpdateLibraryRoot(ctx context.Context, root *domain.LibraryRoot) error {
	if root == nil {
		return errors.New("library root cannot be nil")
	}
	if err := root.Validate(); err != nil {
		return err
	}

	row, err := r.q.UpdateLibraryRoot(ctx, UpdateLibraryRootParams{
		ID:            root.ID,
		LibraryID:     root.LibraryID,
		PathBytes:     slices.Clone(root.Path.Bytes),
		PathDisplay:   root.Path.Display,
		PathMatch:     root.Path.Match,
		State:         string(root.State),
		LastCheckedAt: normalizeOptionalTimeUTC(root.LastCheckedAt),
		LastError:     cloneStringPtr(root.LastError),
	})
	if err != nil {
		return mapRootWriteError(err)
	}
	*root = toDomainLibraryRoot(row)
	return nil
}

// UpdateLibraryRootStatus records availability for the checked root identity.
func (r *LibraryRepository) UpdateLibraryRootStatus(
	ctx context.Context,
	root domain.LibraryRoot,
	state domain.LibraryRootState,
	checkedAt time.Time,
	lastError *string,
) error {
	if strings.TrimSpace(root.ID) == "" {
		return domain.ErrEmptyLibraryRootID
	}
	if !state.IsValid() {
		return domain.ErrInvalidLibraryRootState
	}
	if checkedAt.IsZero() {
		checkedAt = time.Now().UTC()
	} else {
		checkedAt = checkedAt.UTC()
	}

	rows, err := r.q.UpdateLibraryRootStatus(ctx, UpdateLibraryRootStatusParams{
		ID:            root.ID,
		State:         string(state),
		LastCheckedAt: &checkedAt,
		LastError:     cloneStringPtr(lastError),
		LibraryID:     root.LibraryID,
		PathBytes:     root.Path.Bytes,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// DeleteLibraryRoot deletes a library root by ID.
func (r *LibraryRepository) DeleteLibraryRoot(ctx context.Context, id string) error {
	rows, err := r.q.DeleteLibraryRoot(ctx, id)
	if err != nil {
		return mapRootDeleteError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// LockConfiguration verifies that the active transaction runs under READ COMMITTED
// isolation (so reads executed after acquiring the lock observe a fresh snapshot) and
// acquires a transaction-scoped advisory lock that serializes library root and intake
// path configuration checks and mutations. Pool-backed or non-transactional repositories
// return storage.ErrTransactionRequired; stricter isolation levels fail closed with
// storage.ErrTransactionIsolation.
func (r *LibraryRepository) LockConfiguration(ctx context.Context) error {
	if r == nil || r.scope == nil {
		return storage.ErrTransactionRequired
	}
	iso, err := r.q.GetTransactionIsolation(ctx)
	if err != nil {
		return mapPgError(err)
	}
	if !strings.EqualFold(strings.TrimSpace(iso), "read committed") {
		return fmt.Errorf("%w: got %q", storage.ErrTransactionIsolation, iso)
	}
	if err := r.q.AcquireLibraryConfigLock(ctx, libraryConfigLockID); err != nil {
		return mapPgError(err)
	}
	return nil
}

// LockLibrary acquires a row-level FOR UPDATE lock on library id inside an active
// transaction so availability transitions and root status updates serialize per library.
func (r *LibraryRepository) LockLibrary(ctx context.Context, id string) error {
	if r == nil || r.scope == nil {
		return storage.ErrTransactionRequired
	}
	if strings.TrimSpace(id) == "" {
		return domain.ErrEmptyLibraryID
	}
	if _, err := r.q.LockLibrary(ctx, id); err != nil {
		return mapPgError(err)
	}
	return nil
}

// LockLibraryForKeyShare acquires a row-level FOR KEY SHARE lock on library id inside an active
// transaction, blocking concurrent FOR UPDATE availability writes while allowing concurrent scans.
func (r *LibraryRepository) LockLibraryForKeyShare(ctx context.Context, id string) error {
	if r == nil || r.scope == nil {
		return storage.ErrTransactionRequired
	}
	if strings.TrimSpace(id) == "" {
		return domain.ErrEmptyLibraryID
	}
	if _, err := r.q.LockLibraryForKeyShare(ctx, id); err != nil {
		return mapPgError(err)
	}
	return nil
}

func prepareLibraryForCreate(library *domain.Library) (domain.Library, error) {
	if library == nil {
		return domain.Library{}, errors.New("library cannot be nil")
	}
	candidate := *library
	if candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return domain.Library{}, fmt.Errorf("generating library ID: %w", err)
		}
		candidate.ID = id
	}
	if candidate.ResolverChain == nil {
		candidate.ResolverChain = domain.DefaultResolverChain()
	}
	if candidate.MetadataPrecedence == "" {
		candidate.MetadataPrecedence = domain.MetadataPrecedenceEmbeddedOverOnline
	}
	if candidate.DefaultCoverPolicy == "" {
		candidate.DefaultCoverPolicy = domain.CoverPolicyFirst
	}
	if candidate.CreatedAt.IsZero() {
		candidate.CreatedAt = time.Now().UTC()
	} else {
		candidate.CreatedAt = candidate.CreatedAt.UTC()
	}
	if err := candidate.Validate(); err != nil {
		return domain.Library{}, err
	}
	return candidate, nil
}

func prepareLibraryRootForCreate(root *domain.LibraryRoot) (domain.LibraryRoot, error) {
	if root == nil {
		return domain.LibraryRoot{}, errors.New("library root cannot be nil")
	}
	candidate := *root
	if candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return domain.LibraryRoot{}, fmt.Errorf("generating library root ID: %w", err)
		}
		candidate.ID = id
	}
	if candidate.State == "" {
		candidate.State = domain.LibraryRootStateOnline
	}
	if err := candidate.Validate(); err != nil {
		return domain.LibraryRoot{}, err
	}
	return candidate, nil
}

func splitOptionalPathIdentity(p *domain.PathIdentity) ([]byte, *string, *string) {
	if p == nil {
		return nil, nil, nil
	}
	display := p.Display
	match := p.Match
	return slices.Clone(p.Bytes), &display, &match
}

func resolverChainToStrings(chain []domain.ResolverName) []string {
	out := make([]string, len(chain))
	for i, r := range chain {
		out[i] = string(r)
	}
	return out
}

func stringsToResolverChain(raw []string) []domain.ResolverName {
	out := make([]domain.ResolverName, len(raw))
	for i, r := range raw {
		out[i] = domain.ResolverName(r)
	}
	return out
}

func toDomainLibrary(row Library) domain.Library {
	return toDomainLibraryFields(
		row.ID,
		row.Name,
		row.Mode,
		row.IntakePathBytes,
		row.IntakePathDisplay,
		row.IntakePathMatch,
		row.AutoMetadataWriteback,
		row.ResolverChain,
		row.MetadataPrecedence,
		row.DefaultCoverPolicy,
		row.CreatedAt,
	)
}

func toDomainLibraryFromPagedRow(row ListLibrariesPagedRow) (domain.Library, error) {
	if row.ID == nil || row.Name == nil || row.Mode == nil || !row.AutoMetadataWriteback.Valid ||
		row.MetadataPrecedence == nil || row.DefaultCoverPolicy == nil || row.CreatedAt == nil {
		return domain.Library{}, errors.New("paged library row missing required library metadata")
	}
	return toDomainLibraryFields(
		*row.ID,
		*row.Name,
		*row.Mode,
		row.IntakePathBytes,
		row.IntakePathDisplay,
		row.IntakePathMatch,
		row.AutoMetadataWriteback.Bool,
		row.ResolverChain,
		*row.MetadataPrecedence,
		*row.DefaultCoverPolicy,
		*row.CreatedAt,
	), nil
}

func toDomainLibraryFields(
	id string,
	name string,
	mode string,
	intakeBytes []byte,
	intakeDisplay *string,
	intakeMatch *string,
	autoWriteback bool,
	resolverChain []string,
	metadataPrecedence string,
	defaultCoverPolicy string,
	createdAt time.Time,
) domain.Library {
	var intake *domain.PathIdentity
	if intakeBytes != nil && intakeDisplay != nil && intakeMatch != nil {
		intake = &domain.PathIdentity{
			Bytes:       slices.Clone(intakeBytes),
			Display:     *intakeDisplay,
			Match:       *intakeMatch,
			InvalidUTF8: !utf8.Valid(intakeBytes),
		}
	}
	return domain.Library{
		ID:                    id,
		Name:                  name,
		Mode:                  domain.LibraryMode(mode),
		IntakePath:            intake,
		AutoMetadataWriteback: autoWriteback,
		ResolverChain:         stringsToResolverChain(resolverChain),
		MetadataPrecedence:    domain.MetadataPrecedence(metadataPrecedence),
		DefaultCoverPolicy:    domain.CoverPolicy(defaultCoverPolicy),
		CreatedAt:             createdAt.UTC(),
	}
}

func toDomainLibraries(rows []Library) []domain.Library {
	out := make([]domain.Library, len(rows))
	for i, row := range rows {
		out[i] = toDomainLibrary(row)
	}
	return out
}

func toDomainLibraryRoot(row LibraryRoot) domain.LibraryRoot {
	var checkedAt *time.Time
	if row.LastCheckedAt != nil {
		t := row.LastCheckedAt.UTC()
		checkedAt = &t
	}
	return domain.LibraryRoot{
		ID:        row.ID,
		LibraryID: row.LibraryID,
		Path: domain.PathIdentity{
			Bytes:       slices.Clone(row.PathBytes),
			Display:     row.PathDisplay,
			Match:       row.PathMatch,
			InvalidUTF8: !utf8.Valid(row.PathBytes),
		},
		State:         domain.LibraryRootState(row.State),
		LastCheckedAt: checkedAt,
		LastError:     cloneStringPtr(row.LastError),
	}
}

func toDomainLibraryRoots(rows []LibraryRoot) []domain.LibraryRoot {
	out := make([]domain.LibraryRoot, len(rows))
	for i, row := range rows {
		out[i] = toDomainLibraryRoot(row)
	}
	return out
}

func normalizeOptionalTimeUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

func cloneStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

func mapRootWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return storage.ErrNotFound
	}
	return mapPgError(err)
}

func mapRootDeleteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return storage.ErrLibraryRootInUse
	}
	return mapPgError(err)
}
