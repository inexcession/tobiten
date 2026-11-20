package comicinfo

import (
	"context"
	"errors"
	"strings"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

type creditField struct {
	role  domain.CreditRole
	names func(Metadata) []string
}

var creditFields = []creditField{
	{role: domain.CreditRoleWriter, names: func(m Metadata) []string { return m.Writer }},
	{role: domain.CreditRolePenciller, names: func(m Metadata) []string { return m.Penciller }},
	{role: domain.CreditRoleInker, names: func(m Metadata) []string { return m.Inker }},
	{role: domain.CreditRoleColorist, names: func(m Metadata) []string { return m.Colorist }},
	{role: domain.CreditRoleLetterer, names: func(m Metadata) []string { return m.Letterer }},
	{role: domain.CreditRoleCoverArtist, names: func(m Metadata) []string { return m.CoverArtist }},
	{role: domain.CreditRoleEditor, names: func(m Metadata) []string { return m.Editor }},
	{role: domain.CreditRoleTranslator, names: func(m Metadata) []string { return m.Translator }},
}

type entityField struct {
	kind  domain.EntityKind
	names func(Metadata) []string
}

var entityFields = []entityField{
	{kind: domain.EntityKindCharacter, names: func(m Metadata) []string { return m.Characters }},
	{kind: domain.EntityKindTeam, names: func(m Metadata) []string { return m.Teams }},
	{kind: domain.EntityKindLocation, names: func(m Metadata) []string { return m.Locations }},
}

// ImportVolumeCreditsAndEntities imports parsed ComicInfo creator credits
// (Writer, Penciller, Inker, Colorist, Letterer, CoverArtist, Editor, Translator)
// and narrative entities (Characters, Teams, Locations) onto volumeID inside the
// caller-supplied transaction tx.
//
// Comma-separated entries are split, trimmed, and deduplicated while preserving
// per-role credit position order. Existing locked credits, locked entity links,
// and locked person or entity fields are preserved. ComicInfo metadata is
// per-volume, so series-level credits are not synthesized.
func ImportVolumeCreditsAndEntities(
	ctx context.Context,
	tx storage.Tx,
	volumeID string,
	meta Metadata,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil {
		return errors.New("comicinfo: transaction is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return domain.ErrEmptyVolumeID
	}
	peopleRepo := tx.People()
	creditRepo := tx.Credits()
	entityRepo := tx.Entities()
	if peopleRepo == nil || creditRepo == nil || entityRepo == nil {
		return errors.New("comicinfo: people, credit, and entity repositories are required")
	}

	return tx.WithTx(ctx, func(inner storage.Tx) error {
		innerPeople := inner.People()
		innerCredits := inner.Credits()
		innerEntities := inner.Entities()
		if innerPeople == nil || innerCredits == nil || innerEntities == nil {
			return errors.New("comicinfo: people, credit, and entity repositories are required")
		}

		desiredCredits, err := buildVolumeCredits(ctx, innerPeople, volumeID, meta)
		if err != nil {
			return err
		}
		if err := innerCredits.SyncVolumeCredits(ctx, volumeID, desiredCredits); err != nil {
			return err
		}

		desiredEntities, err := buildVolumeEntities(ctx, innerEntities, volumeID, meta)
		if err != nil {
			return err
		}
		return innerEntities.SyncVolumeEntities(ctx, volumeID, desiredEntities)
	})
}

func buildVolumeCredits(
	ctx context.Context,
	peopleRepo storage.PersonRepository,
	volumeID string,
	meta Metadata,
) ([]domain.Credit, error) {
	type creditKey struct {
		personID string
		role     domain.CreditRole
	}

	peopleByKey := make(map[string]string)
	seen := make(map[creditKey]struct{})
	var desired []domain.Credit

	for _, field := range creditFields {
		pos := 0
		for _, name := range splitCommaNames(field.names(meta)) {
			importKey := domain.NormalizeImportName(name)
			if importKey == "" {
				continue
			}
			personID, ok := peopleByKey[importKey]
			if !ok {
				person, err := peopleRepo.FindOrCreatePersonByName(ctx, name)
				if err != nil {
					return nil, err
				}
				personID = person.ID
				peopleByKey[importKey] = personID
			}
			key := creditKey{personID: personID, role: field.role}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			volID := volumeID
			desired = append(desired, domain.Credit{
				PersonID: personID,
				VolumeID: &volID,
				Role:     field.role,
				Position: pos,
			})
			pos++
		}
	}
	return desired, nil
}

func buildVolumeEntities(
	ctx context.Context,
	entityRepo storage.EntityRepository,
	volumeID string,
	meta Metadata,
) ([]domain.VolumeEntity, error) {
	type entityKey struct {
		kind      domain.EntityKind
		importKey string
	}

	entitiesByKey := make(map[entityKey]string)
	seen := make(map[string]struct{})
	var desired []domain.VolumeEntity

	for _, field := range entityFields {
		for _, name := range splitCommaNames(field.names(meta)) {
			importKey := domain.NormalizeImportName(name)
			if importKey == "" {
				continue
			}
			key := entityKey{kind: field.kind, importKey: importKey}
			entityID, ok := entitiesByKey[key]
			if !ok {
				entity, err := entityRepo.FindOrCreateEntityByName(ctx, field.kind, name)
				if err != nil {
					return nil, err
				}
				entityID = entity.ID
				entitiesByKey[key] = entityID
			}
			if _, dup := seen[entityID]; dup {
				continue
			}
			seen[entityID] = struct{}{}
			desired = append(desired, domain.VolumeEntity{
				VolumeID: volumeID,
				EntityID: entityID,
			})
		}
	}
	return desired, nil
}

func splitCommaNames(items []string) []string {
	var out []string
	for _, item := range items {
		for part := range strings.SplitSeq(item, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}
