package postgres

import (
	"maps"
	"slices"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestReconcileAndResolveCredits(t *testing.T) {
	t.Parallel()

	t.Run("automatic sync strips proposal IDs and protects locked credit ID", func(t *testing.T) {
		volID := "vol-1"
		existing := []domain.Credit{
			{
				ID:           "cred-locked",
				PersonID:     "person-1",
				VolumeID:     &volID,
				Role:         domain.CreditRoleWriter,
				Position:     0,
				LockedFields: domain.FieldLocks{"role": true},
			},
			{
				ID:       "cred-unlocked",
				PersonID: "person-2",
				VolumeID: &volID,
				Role:     domain.CreditRoleTranslator,
				Position: 1,
			},
		}
		desired := []domain.Credit{
			// Conflicting proposal carrying the locked credit's ID under a different role.
			{
				ID:       "cred-locked",
				PersonID: "person-1",
				VolumeID: &volID,
				Role:     domain.CreditRolePenciller,
				Position: 2,
			},
			// Unlocked existing key carrying a stale/wrong ID.
			{
				ID:       "cred-locked",
				PersonID: "person-2",
				VolumeID: &volID,
				Role:     domain.CreditRoleTranslator,
				Position: 4,
			},
		}

		reconciled := reconcileCredits(existing, desired)
		resolved, err := resolveCreditIDs(existing, reconciled)
		if err != nil {
			t.Fatalf("resolveCreditIDs: %v", err)
		}
		if len(resolved) != 3 {
			t.Fatalf("len(resolved) = %d, want 3", len(resolved))
		}

		seenIDs := make(map[string]struct{}, len(resolved))
		byRole := make(map[domain.CreditRole]domain.Credit, len(resolved))
		for _, c := range resolved {
			if c.ID == "" {
				t.Fatalf("resolved credit missing ID: %+v", c)
			}
			if _, dup := seenIDs[c.ID]; dup {
				t.Fatalf("duplicate resolved ID %q in %+v", c.ID, resolved)
			}
			seenIDs[c.ID] = struct{}{}
			byRole[c.Role] = c
		}

		locked := byRole[domain.CreditRoleWriter]
		if locked.ID != "cred-locked" || locked.Position != 0 || !maps.Equal(locked.LockedFields, domain.FieldLocks{"role": true}) {
			t.Fatalf("locked credit altered: %+v", locked)
		}
		unlocked := byRole[domain.CreditRoleTranslator]
		if unlocked.ID != "cred-unlocked" || unlocked.Position != 4 {
			t.Fatalf("unlocked credit did not resolve persisted ID: %+v", unlocked)
		}
		added := byRole[domain.CreditRolePenciller]
		if added.ID == "cred-locked" || added.ID == "cred-unlocked" {
			t.Fatalf("new credit reused existing ID: %+v", added)
		}
	})

	t.Run("resolveCreditIDs rejects duplicate explicit IDs", func(t *testing.T) {
		seriesID := "series-1"
		target := []domain.Credit{
			{ID: "dup-id", PersonID: "p1", SeriesID: &seriesID, Role: domain.CreditRoleWriter},
			{ID: "dup-id", PersonID: "p2", SeriesID: &seriesID, Role: domain.CreditRoleArtist},
		}
		if _, err := resolveCreditIDs(nil, target); err == nil {
			t.Fatal("expected error for duplicate explicit credit IDs, got nil")
		}
	})

	t.Run("resolveCreditIDs avoids reusing an ID claimed explicitly by another row", func(t *testing.T) {
		seriesID := "series-1"
		existing := []domain.Credit{
			{ID: "cred-1", PersonID: "p1", SeriesID: &seriesID, Role: domain.CreditRoleWriter},
		}
		target := []domain.Credit{
			{ID: "cred-1", PersonID: "p1", SeriesID: &seriesID, Role: domain.CreditRoleAuthor},
			{ID: "", PersonID: "p1", SeriesID: &seriesID, Role: domain.CreditRoleWriter},
		}
		resolved, err := resolveCreditIDs(existing, target)
		if err != nil {
			t.Fatalf("resolveCreditIDs: %v", err)
		}
		if resolved[0].ID != "cred-1" {
			t.Fatalf("resolved[0].ID = %q, want cred-1", resolved[0].ID)
		}
		if resolved[1].ID == "" || resolved[1].ID == "cred-1" {
			t.Fatalf("resolved[1].ID = %q, want distinct generated ID", resolved[1].ID)
		}
	})
}

func TestMergeEntityFields_KeyedEntityKindStability(t *testing.T) {
	t.Parallel()

	key := "monkey d. luffy"
	existingKeyed := domain.Entity{
		ID:          "ent-1",
		Kind:        domain.EntityKindCharacter,
		Name:        "Monkey D. Luffy",
		ImportKey:   &key,
		Aliases:     []string{"Luffy"},
		ExternalIDs: map[string]any{"anilist": 40},
	}
	proposedConflict := domain.Entity{
		ID:          "ent-1",
		Kind:        domain.EntityKindTeam,
		Name:        "Monkey D. Luffy",
		Aliases:     []string{"Straw Hat Luffy"},
		ExternalIDs: map[string]any{"anilist": 40, "mu": 1},
	}
	merged := mergeEntityFields(existingKeyed, proposedConflict)
	if merged.Kind != domain.EntityKindCharacter {
		t.Fatalf("keyed entity Kind changed to %q, want %q", merged.Kind, domain.EntityKindCharacter)
	}
	if merged.ImportKey == nil || *merged.ImportKey != key {
		t.Fatalf("keyed entity ImportKey = %v, want %q", merged.ImportKey, key)
	}
	if !slices.Equal(merged.Aliases, []string{"Straw Hat Luffy"}) {
		t.Fatalf("keyed entity Aliases = %v, want [Straw Hat Luffy]", merged.Aliases)
	}

	existingUnkeyed := domain.Entity{
		ID:   "ent-2",
		Kind: domain.EntityKindCharacter,
		Name: "Curated",
	}
	mergedUnkeyed := mergeEntityFields(existingUnkeyed, domain.Entity{
		ID:   "ent-2",
		Kind: domain.EntityKindTeam,
		Name: "Curated",
	})
	if mergedUnkeyed.Kind != domain.EntityKindTeam {
		t.Fatalf("unkeyed unlocked entity Kind = %q, want %q", mergedUnkeyed.Kind, domain.EntityKindTeam)
	}

	existingUnkeyedLocked := domain.Entity{
		ID:           "ent-3",
		Kind:         domain.EntityKindCharacter,
		Name:         "Curated Locked",
		LockedFields: domain.FieldLocks{"kind": true},
	}
	mergedUnkeyedLocked := mergeEntityFields(existingUnkeyedLocked, domain.Entity{
		ID:   "ent-3",
		Kind: domain.EntityKindLocation,
		Name: "Curated Locked",
	})
	if mergedUnkeyedLocked.Kind != domain.EntityKindCharacter {
		t.Fatalf("unkeyed kind-locked entity Kind = %q, want %q", mergedUnkeyedLocked.Kind, domain.EntityKindCharacter)
	}
}
