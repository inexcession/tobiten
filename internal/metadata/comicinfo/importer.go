package comicinfo

import (
	"cmp"
	"context"
	"errors"
	"slices"
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
// per-role credit position order. Shared person and entity import keys are
// resolved in a canonical sorted order to prevent deadlocks across concurrent
// volume imports. Existing locked credits, locked entity links, and locked person
// or entity fields are preserved. ComicInfo metadata is per-volume, so
// series-level credits are not synthesized.
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
	type pendingCredit struct {
		role      domain.CreditRole
		importKey string
	}
	type creditKey struct {
		personID string
		role     domain.CreditRole
	}

	firstDisplayName := make(map[string]string)
	var pending []pendingCredit
	for _, field := range creditFields {
		for _, name := range splitCommaNames(field.names(meta)) {
			importKey := domain.NormalizeImportName(name)
			if importKey == "" {
				continue
			}
			if _, ok := firstDisplayName[importKey]; !ok {
				firstDisplayName[importKey] = name
			}
			pending = append(pending, pendingCredit{
				role:      field.role,
				importKey: importKey,
			})
		}
	}

	sortedKeys := make([]string, 0, len(firstDisplayName))
	for key := range firstDisplayName {
		sortedKeys = append(sortedKeys, key)
	}
	slices.Sort(sortedKeys)

	peopleByKey := make(map[string]string, len(sortedKeys))
	for _, key := range sortedKeys {
		person, err := peopleRepo.FindOrCreatePersonByName(ctx, firstDisplayName[key])
		if err != nil {
			return nil, err
		}
		peopleByKey[key] = person.ID
	}

	seen := make(map[creditKey]struct{}, len(pending))
	posByRole := make(map[domain.CreditRole]int, len(creditFields))
	desired := make([]domain.Credit, 0, len(pending))
	for _, item := range pending {
		personID := peopleByKey[item.importKey]
		key := creditKey{personID: personID, role: item.role}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		pos := posByRole[item.role]
		posByRole[item.role] = pos + 1
		volID := volumeID
		desired = append(desired, domain.Credit{
			PersonID: personID,
			VolumeID: &volID,
			Role:     item.role,
			Position: pos,
		})
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

	firstDisplayName := make(map[entityKey]string)
	for _, field := range entityFields {
		for _, name := range splitCommaNames(field.names(meta)) {
			importKey := domain.NormalizeImportName(name)
			if importKey == "" {
				continue
			}
			key := entityKey{kind: field.kind, importKey: importKey}
			if _, ok := firstDisplayName[key]; !ok {
				firstDisplayName[key] = name
			}
		}
	}

	sortedKeys := make([]entityKey, 0, len(firstDisplayName))
	for key := range firstDisplayName {
		sortedKeys = append(sortedKeys, key)
	}
	slices.SortFunc(sortedKeys, func(a, b entityKey) int {
		if c := cmp.Compare(a.kind, b.kind); c != 0 {
			return c
		}
		return cmp.Compare(a.importKey, b.importKey)
	})

	seen := make(map[string]struct{}, len(sortedKeys))
	desired := make([]domain.VolumeEntity, 0, len(sortedKeys))
	for _, key := range sortedKeys {
		entity, err := entityRepo.FindOrCreateEntityByName(ctx, key.kind, firstDisplayName[key])
		if err != nil {
			return nil, err
		}
		if _, dup := seen[entity.ID]; dup {
			continue
		}
		seen[entity.ID] = struct{}{}
		desired = append(desired, domain.VolumeEntity{
			VolumeID: volumeID,
			EntityID: entity.ID,
		})
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
