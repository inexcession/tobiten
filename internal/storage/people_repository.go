package storage

import (
	"context"

	"github.com/inexcession/tobiten/internal/domain"
)

// PersonRepository defines system-level persistence operations for people and
// M1B-D4 name-only import matching with field-lock preservation (ADR-0008 §6).
type PersonRepository interface {
	CreatePerson(ctx context.Context, person *domain.Person) error
	GetPersonByID(ctx context.Context, id string) (*domain.Person, error)
	GetPersonByImportKey(ctx context.Context, importKey string) (*domain.Person, error)

	// FindOrCreatePersonByName looks up or atomically creates a shared name-only
	// imported person keyed by domain.NormalizeImportName(name), preserving the
	// original trimmed display name on creation and leaving existing rows untouched.
	FindOrCreatePersonByName(ctx context.Context, name string) (*domain.Person, error)

	// UpdatePerson persists explicit curator edits to all person columns including locked_fields.
	UpdatePerson(ctx context.Context, person *domain.Person) error

	// SyncPerson updates unlocked metadata fields on an existing person while
	// leaving locked fields, import_key, and locked_fields untouched.
	// Supplied locked_fields on the input are rejected with ErrAutomaticSyncLockedFields.
	SyncPerson(ctx context.Context, person *domain.Person) error

	DeletePerson(ctx context.Context, id string) error
}

// CreditRepository defines system-level persistence operations for series and
// volume credits with field-lock preservation (ADR-0008 §6).
type CreditRepository interface {
	CreateCredit(ctx context.Context, credit *domain.Credit) error
	GetCreditByID(ctx context.Context, id string) (*domain.Credit, error)
	UpdateCredit(ctx context.Context, credit *domain.Credit) error
	DeleteCredit(ctx context.Context, id string) error

	ListCreditsBySeries(ctx context.Context, seriesID string) ([]domain.Credit, error)
	ListCreditsByVolume(ctx context.Context, volumeID string) ([]domain.Credit, error)
	ListCreditsByPerson(ctx context.Context, personID string) ([]domain.Credit, error)

	// SetSeriesCredits replaces the full credit set of seriesID inside a transaction.
	SetSeriesCredits(ctx context.Context, seriesID string, credits []domain.Credit) error
	// SetVolumeCredits replaces the full credit set of volumeID inside a transaction.
	SetVolumeCredits(ctx context.Context, volumeID string, credits []domain.Credit) error

	// SyncSeriesCredits replaces unlocked credits on seriesID while preserving
	// locked credit rows untouched. Supplied locked_fields are rejected with
	// ErrAutomaticSyncLockedFields.
	SyncSeriesCredits(ctx context.Context, seriesID string, desired []domain.Credit) error
	// SyncVolumeCredits replaces unlocked credits on volumeID while preserving
	// locked credit rows untouched. Supplied locked_fields are rejected with
	// ErrAutomaticSyncLockedFields.
	SyncVolumeCredits(ctx context.Context, volumeID string, desired []domain.Credit) error
}

// EntityRepository defines system-level persistence operations for typed entities
// (characters, teams, locations) and volume_entities links (ADR-0008 §6).
type EntityRepository interface {
	CreateEntity(ctx context.Context, entity *domain.Entity) error
	GetEntityByID(ctx context.Context, id string) (*domain.Entity, error)
	GetEntityByImportKey(ctx context.Context, kind domain.EntityKind, importKey string) (*domain.Entity, error)

	// FindOrCreateEntityByName looks up or atomically creates a shared name-only
	// imported entity keyed by (kind, domain.NormalizeImportName(name)), preserving
	// the original trimmed display name on creation and leaving existing rows untouched.
	FindOrCreateEntityByName(ctx context.Context, kind domain.EntityKind, name string) (*domain.Entity, error)

	// UpdateEntity persists explicit curator edits to all entity columns including locked_fields.
	UpdateEntity(ctx context.Context, entity *domain.Entity) error

	// SyncEntity updates unlocked metadata fields on an existing entity while
	// leaving locked fields, import_key, and locked_fields untouched.
	// Supplied locked_fields on the input are rejected with ErrAutomaticSyncLockedFields.
	SyncEntity(ctx context.Context, entity *domain.Entity) error

	DeleteEntity(ctx context.Context, id string) error

	ListVolumeEntities(ctx context.Context, volumeID string) ([]domain.VolumeEntity, error)
	ListVolumeEntitiesByEntity(ctx context.Context, entityID string) ([]domain.VolumeEntity, error)

	// SetVolumeEntities replaces the full entity link set of volumeID inside a transaction.
	SetVolumeEntities(ctx context.Context, volumeID string, links []domain.VolumeEntity) error

	// SyncVolumeEntities replaces unlocked entity links on volumeID while preserving
	// locked link rows untouched. Supplied locked_fields are rejected with
	// ErrAutomaticSyncLockedFields.
	SyncVolumeEntities(ctx context.Context, volumeID string, desired []domain.VolumeEntity) error
}
