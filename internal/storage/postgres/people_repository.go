package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

var (
	_ storage.PersonRepository = (*PersonRepository)(nil)
	_ storage.CreditRepository = (*CreditRepository)(nil)
	_ storage.EntityRepository = (*EntityRepository)(nil)
)

const maxFindOrCreateAttempts = 3

// PersonRepository implements storage.PersonRepository backed by PostgreSQL and sqlc.
type PersonRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
}

// NewPersonRepository creates a new PersonRepository using the provided database handle.
func NewPersonRepository(db DBTX) *PersonRepository {
	beginner, scope := extractTxSupport(db)
	return &PersonRepository{q: New(db), tx: beginner, scope: scope}
}

func (r *PersonRepository) runWriteTx(ctx context.Context, label string, fn func(q *Queries) error) error {
	if r == nil {
		return errors.New("person repository is required")
	}
	return runTx(ctx, r.tx, r.scope, label, func(child *txScope) error {
		return fn(New(child.tx))
	})
}

// CreatePerson persists a new person record (curated or with import_key).
func (r *PersonRepository) CreatePerson(ctx context.Context, person *domain.Person) error {
	if r == nil {
		return errors.New("person repository is required")
	}
	return savePerson(ctx, r.q, person, true)
}

// GetPersonByID retrieves a person by ID.
func (r *PersonRepository) GetPersonByID(ctx context.Context, id string) (*domain.Person, error) {
	if r == nil {
		return nil, errors.New("person repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyPersonID
	}
	row, err := r.q.GetPersonByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := personModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// GetPersonByImportKey retrieves a person by normalized import_key.
func (r *PersonRepository) GetPersonByImportKey(ctx context.Context, importKey string) (*domain.Person, error) {
	if r == nil {
		return nil, errors.New("person repository is required")
	}
	if strings.TrimSpace(importKey) == "" {
		return nil, domain.ErrEmptyImportKey
	}
	row, err := r.q.GetPersonByImportKey(ctx, &importKey)
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := personModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// FindOrCreatePersonByName resolves or atomically creates a shared person for a
// name-only import per M1B-D4 (ADR-0008 §6), preserving the display name on creation.
func (r *PersonRepository) FindOrCreatePersonByName(ctx context.Context, name string) (*domain.Person, error) {
	if r == nil {
		return nil, errors.New("person repository is required")
	}
	importKey := domain.NormalizeImportName(name)
	if importKey == "" {
		return nil, domain.ErrEmptyPersonName
	}
	displayName := strings.TrimSpace(name)

	row, err := findOrCreateByImportKey(
		ctx,
		r.q,
		func() (Person, error) {
			return r.q.GetPersonByImportKey(ctx, &importKey)
		},
		func(id string) (Person, error) {
			return r.q.FindOrInsertPersonByImportKey(ctx, FindOrInsertPersonByImportKeyParams{
				ID:        id,
				Name:      displayName,
				ImportKey: &importKey,
			})
		},
	)
	if err != nil {
		return nil, err
	}
	out, err := personModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdatePerson persists explicit curator edits to all columns of an existing person.
func (r *PersonRepository) UpdatePerson(ctx context.Context, person *domain.Person) error {
	if r == nil {
		return errors.New("person repository is required")
	}
	return savePerson(ctx, r.q, person, false)
}

// SyncPerson updates unlocked metadata fields on an existing person while
// preserving locked fields, import_key, and locked_fields.
func (r *PersonRepository) SyncPerson(ctx context.Context, person *domain.Person) error {
	if r == nil {
		return errors.New("person repository is required")
	}
	if person == nil {
		return errors.New("person cannot be nil")
	}
	if strings.TrimSpace(person.ID) == "" {
		return domain.ErrEmptyPersonID
	}
	if strings.TrimSpace(person.Name) == "" {
		return domain.ErrEmptyPersonName
	}
	if len(person.LockedFields) > 0 {
		return storage.ErrAutomaticSyncLockedFields
	}

	return syncRecord(
		ctx,
		r.runWriteTx,
		"sync person",
		person,
		func(q *Queries) (domain.Person, error) {
			row, err := q.GetPersonByIDForUpdate(ctx, person.ID)
			if err != nil {
				return domain.Person{}, mapPgError(err)
			}
			return personModelToDomain(row)
		},
		mergePersonFields,
		personsEqual,
		savePerson,
	)
}

// DeletePerson deletes a person by ID.
func (r *PersonRepository) DeletePerson(ctx context.Context, id string) error {
	if r == nil {
		return errors.New("person repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return domain.ErrEmptyPersonID
	}
	n, err := r.q.DeletePerson(ctx, id)
	if err != nil {
		return mapPgError(err)
	}
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// CreditRepository implements storage.CreditRepository backed by PostgreSQL and sqlc.
type CreditRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
}

// NewCreditRepository creates a new CreditRepository using the provided database handle.
func NewCreditRepository(db DBTX) *CreditRepository {
	beginner, scope := extractTxSupport(db)
	return &CreditRepository{q: New(db), tx: beginner, scope: scope}
}

func (r *CreditRepository) runWriteTx(ctx context.Context, label string, fn func(q *Queries) error) error {
	if r == nil {
		return errors.New("credit repository is required")
	}
	return runTx(ctx, r.tx, r.scope, label, func(child *txScope) error {
		return fn(New(child.tx))
	})
}

// CreateCredit persists a new series or volume credit.
func (r *CreditRepository) CreateCredit(ctx context.Context, credit *domain.Credit) error {
	if r == nil {
		return errors.New("credit repository is required")
	}
	return saveCredit(ctx, r.q, credit, true)
}

// GetCreditByID retrieves a credit by ID.
func (r *CreditRepository) GetCreditByID(ctx context.Context, id string) (*domain.Credit, error) {
	if r == nil {
		return nil, errors.New("credit repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyCreditID
	}
	row, err := r.q.GetCreditByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := creditModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateCredit persists explicit curator edits to an existing credit.
func (r *CreditRepository) UpdateCredit(ctx context.Context, credit *domain.Credit) error {
	if r == nil {
		return errors.New("credit repository is required")
	}
	return saveCredit(ctx, r.q, credit, false)
}

// DeleteCredit deletes a credit by ID.
func (r *CreditRepository) DeleteCredit(ctx context.Context, id string) error {
	if r == nil {
		return errors.New("credit repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return domain.ErrEmptyCreditID
	}
	n, err := r.q.DeleteCredit(ctx, id)
	if err != nil {
		return mapPgError(err)
	}
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ListCreditsBySeries returns all credits attached to seriesID ordered by position, role, and ID.
func (r *CreditRepository) ListCreditsBySeries(ctx context.Context, seriesID string) ([]domain.Credit, error) {
	if r == nil {
		return nil, errors.New("credit repository is required")
	}
	if strings.TrimSpace(seriesID) == "" {
		return nil, domain.ErrEmptySeriesID
	}
	rows, err := r.q.ListCreditsBySeries(ctx, &seriesID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return creditModelsToDomain(rows)
}

// ListCreditsByVolume returns all credits attached to volumeID ordered by position, role, and ID.
func (r *CreditRepository) ListCreditsByVolume(ctx context.Context, volumeID string) ([]domain.Credit, error) {
	if r == nil {
		return nil, errors.New("credit repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return nil, domain.ErrEmptyVolumeID
	}
	rows, err := r.q.ListCreditsByVolume(ctx, &volumeID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return creditModelsToDomain(rows)
}

// ListCreditsByPerson returns all credits referencing personID ordered by position and ID.
func (r *CreditRepository) ListCreditsByPerson(ctx context.Context, personID string) ([]domain.Credit, error) {
	if r == nil {
		return nil, errors.New("credit repository is required")
	}
	if strings.TrimSpace(personID) == "" {
		return nil, domain.ErrEmptyPersonID
	}
	rows, err := r.q.ListCreditsByPerson(ctx, personID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return creditModelsToDomain(rows)
}

// SetSeriesCredits replaces the full credit set of seriesID inside a transaction.
func (r *CreditRepository) SetSeriesCredits(ctx context.Context, seriesID string, credits []domain.Credit) error {
	return r.setCreditsForTarget(ctx, creditTarget{seriesID: &seriesID}, credits)
}

// SetVolumeCredits replaces the full credit set of volumeID inside a transaction.
func (r *CreditRepository) SetVolumeCredits(ctx context.Context, volumeID string, credits []domain.Credit) error {
	return r.setCreditsForTarget(ctx, creditTarget{volumeID: &volumeID}, credits)
}

// SyncSeriesCredits replaces unlocked credits on seriesID while preserving locked rows untouched.
func (r *CreditRepository) SyncSeriesCredits(ctx context.Context, seriesID string, desired []domain.Credit) error {
	return r.syncCreditsForTarget(ctx, creditTarget{seriesID: &seriesID}, desired)
}

// SyncVolumeCredits replaces unlocked credits on volumeID while preserving locked rows untouched.
func (r *CreditRepository) SyncVolumeCredits(ctx context.Context, volumeID string, desired []domain.Credit) error {
	return r.syncCreditsForTarget(ctx, creditTarget{volumeID: &volumeID}, desired)
}

type creditTarget struct {
	seriesID *string
	volumeID *string
}

func (t creditTarget) validateInput(credits []domain.Credit) ([]domain.Credit, error) {
	normalized := make([]domain.Credit, len(credits))
	for i, c := range credits {
		c.LockedFields = maps.Clone(c.LockedFields)
		if c.ID == "" {
			c.ID = "__pending__"
		}
		if c.Position < math.MinInt32 || c.Position > math.MaxInt32 {
			return nil, fmt.Errorf("credit position %d exceeds int32 range", c.Position)
		}
		normalized[i] = c
	}
	if t.seriesID != nil {
		if err := domain.ValidateSeriesCredits(*t.seriesID, normalized); err != nil {
			return nil, err
		}
	} else if t.volumeID != nil {
		if err := domain.ValidateVolumeCredits(*t.volumeID, normalized); err != nil {
			return nil, err
		}
	}
	for i := range credits {
		normalized[i].ID = credits[i].ID
	}
	return normalized, nil
}

func (t creditTarget) lockAndListExisting(ctx context.Context, q *Queries) ([]domain.Credit, error) {
	var rows []Credit
	var err error
	if t.seriesID != nil {
		if _, err := q.LockSeriesForCreditUpdate(ctx, *t.seriesID); err != nil {
			return nil, mapPgError(err)
		}
		rows, err = q.ListCreditsBySeriesForUpdate(ctx, t.seriesID)
	} else {
		if _, err := q.LockVolumeForCreditUpdate(ctx, *t.volumeID); err != nil {
			return nil, mapPgError(err)
		}
		rows, err = q.ListCreditsByVolumeForUpdate(ctx, t.volumeID)
	}
	if err != nil {
		return nil, mapPgError(err)
	}
	return creditModelsToDomain(rows)
}

func (r *CreditRepository) setCreditsForTarget(ctx context.Context, target creditTarget, credits []domain.Credit) error {
	if r == nil {
		return errors.New("credit repository is required")
	}
	prepared, err := target.validateInput(credits)
	if err != nil {
		return err
	}
	return r.runWriteTx(ctx, "set credits", func(q *Queries) error {
		existing, err := target.lockAndListExisting(ctx, q)
		if err != nil {
			return err
		}
		resolved, err := resolveCreditIDs(existing, prepared)
		if err != nil {
			return err
		}
		return applyCreditDiff(ctx, q, existing, resolved)
	})
}

func (r *CreditRepository) syncCreditsForTarget(ctx context.Context, target creditTarget, desired []domain.Credit) error {
	if r == nil {
		return errors.New("credit repository is required")
	}
	for i := range desired {
		if len(desired[i].LockedFields) > 0 {
			return storage.ErrAutomaticSyncLockedFields
		}
	}
	prepared, err := target.validateInput(desired)
	if err != nil {
		return err
	}
	return r.runWriteTx(ctx, "sync credits", func(q *Queries) error {
		existing, err := target.lockAndListExisting(ctx, q)
		if err != nil {
			return err
		}
		reconciled := reconcileCredits(existing, prepared)
		resolved, err := resolveCreditIDs(existing, reconciled)
		if err != nil {
			return err
		}
		return applyCreditDiff(ctx, q, existing, resolved)
	})
}

// EntityRepository implements storage.EntityRepository backed by PostgreSQL and sqlc.
type EntityRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
}

// NewEntityRepository creates a new EntityRepository using the provided database handle.
func NewEntityRepository(db DBTX) *EntityRepository {
	beginner, scope := extractTxSupport(db)
	return &EntityRepository{q: New(db), tx: beginner, scope: scope}
}

func (r *EntityRepository) runWriteTx(ctx context.Context, label string, fn func(q *Queries) error) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	return runTx(ctx, r.tx, r.scope, label, func(child *txScope) error {
		return fn(New(child.tx))
	})
}

// CreateEntity persists a new entity record (curated or with import_key).
func (r *EntityRepository) CreateEntity(ctx context.Context, entity *domain.Entity) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	return saveEntity(ctx, r.q, entity, true)
}

// GetEntityByID retrieves an entity by ID.
func (r *EntityRepository) GetEntityByID(ctx context.Context, id string) (*domain.Entity, error) {
	if r == nil {
		return nil, errors.New("entity repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyEntityID
	}
	row, err := r.q.GetEntityByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := entityModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// GetEntityByImportKey retrieves an entity by kind and normalized import_key.
func (r *EntityRepository) GetEntityByImportKey(ctx context.Context, kind domain.EntityKind, importKey string) (*domain.Entity, error) {
	if r == nil {
		return nil, errors.New("entity repository is required")
	}
	if !kind.IsValid() {
		return nil, domain.ErrInvalidEntityKind
	}
	if strings.TrimSpace(importKey) == "" {
		return nil, domain.ErrEmptyImportKey
	}
	row, err := r.q.GetEntityByImportKey(ctx, GetEntityByImportKeyParams{
		Kind:      string(kind),
		ImportKey: &importKey,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := entityModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// FindOrCreateEntityByName resolves or atomically creates a shared entity for a
// name-only import keyed by (kind, domain.NormalizeImportName(name)) per M1B-D4.
func (r *EntityRepository) FindOrCreateEntityByName(ctx context.Context, kind domain.EntityKind, name string) (*domain.Entity, error) {
	if r == nil {
		return nil, errors.New("entity repository is required")
	}
	if !kind.IsValid() {
		return nil, domain.ErrInvalidEntityKind
	}
	importKey := domain.NormalizeImportName(name)
	if importKey == "" {
		return nil, domain.ErrEmptyEntityName
	}
	displayName := strings.TrimSpace(name)

	row, err := findOrCreateByImportKey(
		ctx,
		r.q,
		func() (Entity, error) {
			return r.q.GetEntityByImportKey(ctx, GetEntityByImportKeyParams{
				Kind:      string(kind),
				ImportKey: &importKey,
			})
		},
		func(id string) (Entity, error) {
			return r.q.FindOrInsertEntityByImportKey(ctx, FindOrInsertEntityByImportKeyParams{
				ID:        id,
				Kind:      string(kind),
				Name:      displayName,
				ImportKey: &importKey,
			})
		},
	)
	if err != nil {
		return nil, err
	}
	out, err := entityModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateEntity persists explicit curator edits to all columns of an existing entity.
func (r *EntityRepository) UpdateEntity(ctx context.Context, entity *domain.Entity) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	return saveEntity(ctx, r.q, entity, false)
}

// SyncEntity updates unlocked metadata fields on an existing entity while
// preserving locked fields, import_key, and locked_fields.
func (r *EntityRepository) SyncEntity(ctx context.Context, entity *domain.Entity) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	if entity == nil {
		return errors.New("entity cannot be nil")
	}
	if strings.TrimSpace(entity.ID) == "" {
		return domain.ErrEmptyEntityID
	}
	if !entity.Kind.IsValid() {
		return domain.ErrInvalidEntityKind
	}
	if strings.TrimSpace(entity.Name) == "" {
		return domain.ErrEmptyEntityName
	}
	if len(entity.LockedFields) > 0 {
		return storage.ErrAutomaticSyncLockedFields
	}

	return syncRecord(
		ctx,
		r.runWriteTx,
		"sync entity",
		entity,
		func(q *Queries) (domain.Entity, error) {
			row, err := q.GetEntityByIDForUpdate(ctx, entity.ID)
			if err != nil {
				return domain.Entity{}, mapPgError(err)
			}
			return entityModelToDomain(row)
		},
		mergeEntityFields,
		entitiesEqual,
		saveEntity,
	)
}

// DeleteEntity deletes an entity by ID.
func (r *EntityRepository) DeleteEntity(ctx context.Context, id string) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return domain.ErrEmptyEntityID
	}
	n, err := r.q.DeleteEntity(ctx, id)
	if err != nil {
		return mapPgError(err)
	}
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ListVolumeEntities returns all entity links for volumeID ordered by entity_id.
func (r *EntityRepository) ListVolumeEntities(ctx context.Context, volumeID string) ([]domain.VolumeEntity, error) {
	if r == nil {
		return nil, errors.New("entity repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return nil, domain.ErrEmptyVolumeID
	}
	rows, err := r.q.ListVolumeEntities(ctx, volumeID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return volumeEntityModelsToDomain(rows)
}

// ListVolumeEntitiesByEntity returns all volume links for entityID ordered by volume_id.
func (r *EntityRepository) ListVolumeEntitiesByEntity(ctx context.Context, entityID string) ([]domain.VolumeEntity, error) {
	if r == nil {
		return nil, errors.New("entity repository is required")
	}
	if strings.TrimSpace(entityID) == "" {
		return nil, domain.ErrEmptyEntityID
	}
	rows, err := r.q.ListVolumeEntitiesByEntity(ctx, entityID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return volumeEntityModelsToDomain(rows)
}

// SetVolumeEntities replaces the full entity link set of volumeID inside a transaction.
func (r *EntityRepository) SetVolumeEntities(ctx context.Context, volumeID string, links []domain.VolumeEntity) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	if err := domain.ValidateVolumeEntities(volumeID, links); err != nil {
		return err
	}
	return r.runWriteTx(ctx, "set volume entities", func(q *Queries) error {
		if _, err := q.LockVolumeForCreditUpdate(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
		existingRows, err := q.ListVolumeEntitiesForUpdate(ctx, volumeID)
		if err != nil {
			return mapPgError(err)
		}
		existing, err := volumeEntityModelsToDomain(existingRows)
		if err != nil {
			return err
		}
		return applyVolumeEntityDiff(ctx, q, volumeID, existing, links)
	})
}

// SyncVolumeEntities replaces unlocked entity links on volumeID while leaving
// locked link rows untouched.
func (r *EntityRepository) SyncVolumeEntities(ctx context.Context, volumeID string, desired []domain.VolumeEntity) error {
	if r == nil {
		return errors.New("entity repository is required")
	}
	for i := range desired {
		if len(desired[i].LockedFields) > 0 {
			return storage.ErrAutomaticSyncLockedFields
		}
	}
	if err := domain.ValidateVolumeEntities(volumeID, desired); err != nil {
		return err
	}
	return r.runWriteTx(ctx, "sync volume entities", func(q *Queries) error {
		if _, err := q.LockVolumeForCreditUpdate(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
		existingRows, err := q.ListVolumeEntitiesForUpdate(ctx, volumeID)
		if err != nil {
			return mapPgError(err)
		}
		existing, err := volumeEntityModelsToDomain(existingRows)
		if err != nil {
			return err
		}
		target := reconcileVolumeEntities(existing, desired)
		return applyVolumeEntityDiff(ctx, q, volumeID, existing, target)
	})
}

func syncRecord[T any](
	ctx context.Context,
	run func(context.Context, string, func(*Queries) error) error,
	label string,
	target *T,
	fetch func(*Queries) (T, error),
	merge func(existing, proposed T) T,
	equal func(a, b T) (bool, error),
	save func(context.Context, *Queries, *T, bool) error,
) error {
	return run(ctx, label, func(q *Queries) error {
		existing, err := fetch(q)
		if err != nil {
			return err
		}
		merged := merge(existing, *target)
		same, err := equal(existing, merged)
		if err != nil {
			return err
		}
		if same {
			*target = existing
			return nil
		}
		if err := save(ctx, q, &merged, false); err != nil {
			return err
		}
		*target = merged
		return nil
	})
}

func extractTxSupport(db DBTX) (TxBeginner, *txScope) {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	var scope *txScope
	if extTx, ok := db.(pgx.Tx); ok {
		scope = newCallerTxScope(extTx)
	}
	return beginner, scope
}

func findOrCreateByImportKey[T any](
	ctx context.Context,
	q *Queries,
	lookup func() (T, error),
	insert func(id string) (T, error),
) (T, error) {
	var zero T
	row, err := lookup()
	if err == nil {
		return row, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return zero, mapPgError(err)
	}

	for range maxFindOrCreateAttempts {
		id, err := domain.NewID()
		if err != nil {
			return zero, fmt.Errorf("generating ID: %w", err)
		}
		inserted, err := insert(id)
		if err == nil {
			return inserted, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return zero, mapPgError(err)
		}

		existing, err := lookup()
		if err == nil {
			return existing, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return zero, mapPgError(err)
		}

		iso, isoErr := q.GetTransactionIsolation(ctx)
		if isoErr != nil {
			return zero, mapPgError(isoErr)
		}
		if !strings.EqualFold(strings.TrimSpace(iso), "read committed") {
			return zero, fmt.Errorf("%w: got %q", storage.ErrTransactionIsolation, iso)
		}
	}
	return zero, storage.ErrNotFound
}

func prepareMetadataWrite(
	label string,
	id *string,
	isCreate bool,
	validate func() error,
	aliases []string,
	externalIDs map[string]any,
	lockedFields domain.FieldLocks,
) ([]string, []byte, []byte, error) {
	if isCreate && *id == "" {
		newID, err := domain.NewID()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("generating %s ID: %w", label, err)
		}
		*id = newID
	}
	if err := validate(); err != nil {
		return nil, nil, nil, err
	}
	cleanAliases := normalizeStringSlice(aliases)
	externalBytes, err := encodeJSONOrEmptyObject(externalIDs)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encoding %s external_ids: %w", label, err)
	}
	lockedBytes, err := encodeJSONOrEmptyObject(lockedFields)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encoding %s locked_fields: %w", label, err)
	}
	return cleanAliases, externalBytes, lockedBytes, nil
}

func savePerson(ctx context.Context, q *Queries, person *domain.Person, isCreate bool) error {
	if person == nil {
		return errors.New("person cannot be nil")
	}
	candidate := *person
	aliases, externalBytes, lockedBytes, err := prepareMetadataWrite(
		"person", &candidate.ID, isCreate, candidate.Validate,
		candidate.Aliases, candidate.ExternalIDs, candidate.LockedFields,
	)
	if err != nil {
		return err
	}
	var row Person
	if isCreate {
		row, err = q.CreatePerson(ctx, CreatePersonParams{
			ID:            candidate.ID,
			Name:          candidate.Name,
			NameNative:    cloneStringPtr(candidate.NameNative),
			NameRomanized: cloneStringPtr(candidate.NameRomanized),
			Aliases:       aliases,
			ExternalIds:   externalBytes,
			LockedFields:  lockedBytes,
			ImportKey:     cloneStringPtr(candidate.ImportKey),
		})
	} else {
		row, err = q.UpdatePerson(ctx, UpdatePersonParams{
			ID:            candidate.ID,
			Name:          candidate.Name,
			NameNative:    cloneStringPtr(candidate.NameNative),
			NameRomanized: cloneStringPtr(candidate.NameRomanized),
			Aliases:       aliases,
			ExternalIds:   externalBytes,
			LockedFields:  lockedBytes,
			ImportKey:     cloneStringPtr(candidate.ImportKey),
		})
	}
	if err != nil {
		return mapPgError(err)
	}
	out, err := personModelToDomain(row)
	if err != nil {
		return err
	}
	*person = out
	return nil
}

func saveEntity(ctx context.Context, q *Queries, entity *domain.Entity, isCreate bool) error {
	if entity == nil {
		return errors.New("entity cannot be nil")
	}
	candidate := *entity
	aliases, externalBytes, lockedBytes, err := prepareMetadataWrite(
		"entity", &candidate.ID, isCreate, candidate.Validate,
		candidate.Aliases, candidate.ExternalIDs, candidate.LockedFields,
	)
	if err != nil {
		return err
	}
	var row Entity
	if isCreate {
		row, err = q.CreateEntity(ctx, CreateEntityParams{
			ID:           candidate.ID,
			Kind:         string(candidate.Kind),
			Name:         candidate.Name,
			Aliases:      aliases,
			ExternalIds:  externalBytes,
			LockedFields: lockedBytes,
			ImportKey:    cloneStringPtr(candidate.ImportKey),
		})
	} else {
		row, err = q.UpdateEntity(ctx, UpdateEntityParams{
			ID:           candidate.ID,
			Kind:         string(candidate.Kind),
			Name:         candidate.Name,
			Aliases:      aliases,
			ExternalIds:  externalBytes,
			LockedFields: lockedBytes,
			ImportKey:    cloneStringPtr(candidate.ImportKey),
		})
	}
	if err != nil {
		return mapPgError(err)
	}
	out, err := entityModelToDomain(row)
	if err != nil {
		return err
	}
	*entity = out
	return nil
}

func saveCredit(ctx context.Context, q *Queries, credit *domain.Credit, isCreate bool) error {
	if credit == nil {
		return errors.New("credit cannot be nil")
	}
	candidate := *credit
	if isCreate && candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return fmt.Errorf("generating credit ID: %w", err)
		}
		candidate.ID = id
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if candidate.Position < math.MinInt32 || candidate.Position > math.MaxInt32 {
		return fmt.Errorf("credit position %d exceeds int32 range", candidate.Position)
	}
	lockedBytes, err := encodeJSONOrEmptyObject(candidate.LockedFields)
	if err != nil {
		return fmt.Errorf("encoding credit locked_fields: %w", err)
	}

	var row Credit
	if isCreate {
		row, err = q.CreateCredit(ctx, CreateCreditParams{
			ID:           candidate.ID,
			PersonID:     candidate.PersonID,
			SeriesID:     cloneStringPtr(candidate.SeriesID),
			VolumeID:     cloneStringPtr(candidate.VolumeID),
			Role:         string(candidate.Role),
			Position:     int32(candidate.Position),
			LockedFields: lockedBytes,
		})
	} else {
		row, err = q.UpdateCredit(ctx, UpdateCreditParams{
			ID:           candidate.ID,
			PersonID:     candidate.PersonID,
			SeriesID:     cloneStringPtr(candidate.SeriesID),
			VolumeID:     cloneStringPtr(candidate.VolumeID),
			Role:         string(candidate.Role),
			Position:     int32(candidate.Position),
			LockedFields: lockedBytes,
		})
	}
	if err != nil {
		return mapCatalogueWriteError(err)
	}
	out, err := creditModelToDomain(row)
	if err != nil {
		return err
	}
	*credit = out
	return nil
}

func mergePersonFields(existing, proposed domain.Person) domain.Person {
	out := existing
	out.Aliases = slices.Clone(existing.Aliases)
	out.ExternalIDs = maps.Clone(existing.ExternalIDs)
	out.LockedFields = maps.Clone(existing.LockedFields)

	wholeLocked := existing.LockedFields.IsLocked("*")
	if !wholeLocked && !existing.LockedFields.IsLocked("name") {
		out.Name = proposed.Name
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("name_native") {
		out.NameNative = cloneStringPtr(proposed.NameNative)
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("name_romanized") {
		out.NameRomanized = cloneStringPtr(proposed.NameRomanized)
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("aliases") {
		out.Aliases = normalizeStringSlice(proposed.Aliases)
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("external_ids") {
		out.ExternalIDs = normalizeAnyMap(proposed.ExternalIDs)
	}
	return out
}

func mergeEntityFields(existing, proposed domain.Entity) domain.Entity {
	out := existing
	out.Aliases = slices.Clone(existing.Aliases)
	out.ExternalIDs = maps.Clone(existing.ExternalIDs)
	out.LockedFields = maps.Clone(existing.LockedFields)

	wholeLocked := existing.LockedFields.IsLocked("*")
	if !wholeLocked && !existing.LockedFields.IsLocked("kind") {
		out.Kind = proposed.Kind
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("name") {
		out.Name = proposed.Name
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("aliases") {
		out.Aliases = normalizeStringSlice(proposed.Aliases)
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("external_ids") {
		out.ExternalIDs = normalizeAnyMap(proposed.ExternalIDs)
	}
	return out
}

func personsEqual(a, b domain.Person) (bool, error) {
	if a.ID != b.ID ||
		a.Name != b.Name ||
		!optStringEqual(a.ImportKey, b.ImportKey) ||
		!optStringEqual(a.NameNative, b.NameNative) ||
		!optStringEqual(a.NameRomanized, b.NameRomanized) ||
		!slices.Equal(a.Aliases, b.Aliases) ||
		!maps.Equal(a.LockedFields, b.LockedFields) {
		return false, nil
	}
	return jsonMapsEqual(a.ExternalIDs, b.ExternalIDs)
}

func entitiesEqual(a, b domain.Entity) (bool, error) {
	if a.ID != b.ID ||
		a.Kind != b.Kind ||
		a.Name != b.Name ||
		!optStringEqual(a.ImportKey, b.ImportKey) ||
		!slices.Equal(a.Aliases, b.Aliases) ||
		!maps.Equal(a.LockedFields, b.LockedFields) {
		return false, nil
	}
	return jsonMapsEqual(a.ExternalIDs, b.ExternalIDs)
}

func jsonMapsEqual(a, b map[string]any) (bool, error) {
	ab, err := encodeJSONOrEmptyObject(a)
	if err != nil {
		return false, err
	}
	bb, err := encodeJSONOrEmptyObject(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ab, bb), nil
}

func optStringEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func normalizeStringSlice(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	return slices.Clone(in)
}

func normalizeAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	return maps.Clone(in)
}

type creditKey struct {
	personID string
	role     domain.CreditRole
}

func reconcileCredits(existing, desired []domain.Credit) []domain.Credit {
	targetByKey := make(map[creditKey]domain.Credit, len(existing)+len(desired))
	for _, e := range existing {
		if e.LockedFields.HasAnyLock() {
			c := e
			c.LockedFields = maps.Clone(e.LockedFields)
			targetByKey[creditKey{personID: e.PersonID, role: e.Role}] = c
		}
	}
	for _, d := range desired {
		key := creditKey{personID: d.PersonID, role: d.Role}
		if _, locked := targetByKey[key]; locked {
			continue
		}
		c := d
		c.LockedFields = nil
		targetByKey[key] = c
	}
	out := make([]domain.Credit, 0, len(targetByKey))
	for _, c := range targetByKey {
		out = append(out, c)
	}
	return out
}

func resolveCreditIDs(existing, target []domain.Credit) ([]domain.Credit, error) {
	existingByKey := make(map[creditKey]domain.Credit, len(existing))
	for _, e := range existing {
		existingByKey[creditKey{personID: e.PersonID, role: e.Role}] = e
	}
	out := make([]domain.Credit, len(target))
	for i, c := range target {
		if c.ID == "" {
			if e, ok := existingByKey[creditKey{personID: c.PersonID, role: c.Role}]; ok {
				c.ID = e.ID
			} else {
				id, err := domain.NewID()
				if err != nil {
					return nil, fmt.Errorf("generating credit ID: %w", err)
				}
				c.ID = id
			}
		}
		out[i] = c
	}
	return out, nil
}

func applyCreditDiff(ctx context.Context, q *Queries, existing, target []domain.Credit) error {
	existingByID := make(map[string]domain.Credit, len(existing))
	for _, e := range existing {
		existingByID[e.ID] = e
	}
	targetIDs := make(map[string]struct{}, len(target))
	for _, t := range target {
		targetIDs[t.ID] = struct{}{}
	}

	var toDelete []string
	for _, e := range existing {
		if _, kept := targetIDs[e.ID]; !kept {
			toDelete = append(toDelete, e.ID)
		}
	}
	if len(toDelete) > 0 {
		if err := q.DeleteCreditsByIDs(ctx, toDelete); err != nil {
			return mapCatalogueWriteError(err)
		}
	}

	for i := range target {
		t := target[i]
		e, exists := existingByID[t.ID]
		if !exists {
			if err := saveCredit(ctx, q, &t, true); err != nil {
				return err
			}
			continue
		}
		if !creditsEqual(e, t) {
			if err := saveCredit(ctx, q, &t, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func creditsEqual(a, b domain.Credit) bool {
	return a.ID == b.ID &&
		a.PersonID == b.PersonID &&
		optStringEqual(a.SeriesID, b.SeriesID) &&
		optStringEqual(a.VolumeID, b.VolumeID) &&
		a.Role == b.Role &&
		a.Position == b.Position &&
		maps.Equal(a.LockedFields, b.LockedFields)
}

func reconcileVolumeEntities(existing, desired []domain.VolumeEntity) []domain.VolumeEntity {
	targetByEntity := make(map[string]domain.VolumeEntity, len(existing)+len(desired))
	for _, e := range existing {
		if e.LockedFields.HasAnyLock() {
			ve := e
			ve.LockedFields = maps.Clone(e.LockedFields)
			targetByEntity[e.EntityID] = ve
		}
	}
	for _, d := range desired {
		if _, locked := targetByEntity[d.EntityID]; locked {
			continue
		}
		ve := d
		ve.LockedFields = nil
		targetByEntity[d.EntityID] = ve
	}
	out := make([]domain.VolumeEntity, 0, len(targetByEntity))
	for _, ve := range targetByEntity {
		out = append(out, ve)
	}
	return out
}

func applyVolumeEntityDiff(
	ctx context.Context,
	q *Queries,
	volumeID string,
	existing []domain.VolumeEntity,
	target []domain.VolumeEntity,
) error {
	existingByEntity := make(map[string]domain.VolumeEntity, len(existing))
	for _, e := range existing {
		existingByEntity[e.EntityID] = e
	}
	targetByEntity := make(map[string]domain.VolumeEntity, len(target))
	for _, t := range target {
		targetByEntity[t.EntityID] = t
	}

	var toDelete []string
	for _, e := range existing {
		if _, kept := targetByEntity[e.EntityID]; !kept {
			toDelete = append(toDelete, e.EntityID)
		}
	}
	if len(toDelete) > 0 {
		if err := q.DeleteVolumeEntitiesByEntityIDs(ctx, DeleteVolumeEntitiesByEntityIDsParams{
			VolumeID:  volumeID,
			EntityIds: toDelete,
		}); err != nil {
			return mapCatalogueWriteError(err)
		}
	}

	for _, t := range target {
		e, exists := existingByEntity[t.EntityID]
		if !exists {
			lockedBytes, err := encodeJSONOrEmptyObject(t.LockedFields)
			if err != nil {
				return fmt.Errorf("encoding volume_entity locked_fields: %w", err)
			}
			if _, err := q.InsertVolumeEntity(ctx, InsertVolumeEntityParams{
				VolumeID:     volumeID,
				EntityID:     t.EntityID,
				LockedFields: lockedBytes,
			}); err != nil {
				return mapCatalogueWriteError(err)
			}
			continue
		}
		if !maps.Equal(e.LockedFields, t.LockedFields) {
			lockedBytes, err := encodeJSONOrEmptyObject(t.LockedFields)
			if err != nil {
				return fmt.Errorf("encoding volume_entity locked_fields: %w", err)
			}
			if _, err := q.UpdateVolumeEntity(ctx, UpdateVolumeEntityParams{
				VolumeID:     volumeID,
				EntityID:     t.EntityID,
				LockedFields: lockedBytes,
			}); err != nil {
				return mapCatalogueWriteError(err)
			}
		}
	}
	return nil
}

func personModelToDomain(row Person) (domain.Person, error) {
	externalIDs, err := decodeJSONMap(row.ExternalIds)
	if err != nil {
		return domain.Person{}, fmt.Errorf("decoding person external_ids: %w", err)
	}
	lockedFields, err := decodeFieldLocks(row.LockedFields)
	if err != nil {
		return domain.Person{}, fmt.Errorf("decoding person locked_fields: %w", err)
	}
	return domain.Person{
		ID:            row.ID,
		Name:          row.Name,
		ImportKey:     cloneStringPtr(row.ImportKey),
		NameNative:    cloneStringPtr(row.NameNative),
		NameRomanized: cloneStringPtr(row.NameRomanized),
		Aliases:       normalizeStringSlice(row.Aliases),
		ExternalIDs:   externalIDs,
		LockedFields:  lockedFields,
	}, nil
}

func creditModelsToDomain(rows []Credit) ([]domain.Credit, error) {
	out := make([]domain.Credit, len(rows))
	for i, row := range rows {
		c, err := creditModelToDomain(row)
		if err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

func creditModelToDomain(row Credit) (domain.Credit, error) {
	lockedFields, err := decodeFieldLocks(row.LockedFields)
	if err != nil {
		return domain.Credit{}, fmt.Errorf("decoding credit locked_fields: %w", err)
	}
	return domain.Credit{
		ID:           row.ID,
		PersonID:     row.PersonID,
		SeriesID:     cloneStringPtr(row.SeriesID),
		VolumeID:     cloneStringPtr(row.VolumeID),
		Role:         domain.CreditRole(row.Role),
		Position:     int(row.Position),
		LockedFields: lockedFields,
	}, nil
}

func entityModelToDomain(row Entity) (domain.Entity, error) {
	externalIDs, err := decodeJSONMap(row.ExternalIds)
	if err != nil {
		return domain.Entity{}, fmt.Errorf("decoding entity external_ids: %w", err)
	}
	lockedFields, err := decodeFieldLocks(row.LockedFields)
	if err != nil {
		return domain.Entity{}, fmt.Errorf("decoding entity locked_fields: %w", err)
	}
	return domain.Entity{
		ID:           row.ID,
		Kind:         domain.EntityKind(row.Kind),
		Name:         row.Name,
		ImportKey:    cloneStringPtr(row.ImportKey),
		Aliases:      normalizeStringSlice(row.Aliases),
		ExternalIDs:  externalIDs,
		LockedFields: lockedFields,
	}, nil
}

func volumeEntityModelsToDomain(rows []VolumeEntity) ([]domain.VolumeEntity, error) {
	out := make([]domain.VolumeEntity, len(rows))
	for i, row := range rows {
		lockedFields, err := decodeFieldLocks(row.LockedFields)
		if err != nil {
			return nil, fmt.Errorf("decoding volume_entity locked_fields: %w", err)
		}
		out[i] = domain.VolumeEntity{
			VolumeID:     row.VolumeID,
			EntityID:     row.EntityID,
			LockedFields: lockedFields,
		}
	}
	return out, nil
}
