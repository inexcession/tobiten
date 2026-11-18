package postgres

import (
	"maps"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestReconcileAutomaticMemberships(t *testing.T) {
	t.Parallel()

	fPtr := func(v float64) *float64 { return &v }
	const volID = "vol-1"

	assertSet := func(t *testing.T, got, want []domain.SeriesMembership) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d\ngot:  %+v\nwant: %+v", len(got), len(want), got, want)
		}
		for i := range want {
			g, w := got[i], want[i]
			if g.VolumeID != w.VolumeID || g.SeriesID != w.SeriesID || g.IsPrimary != w.IsPrimary ||
				g.Kind != w.Kind || g.Source != w.Source {
				t.Fatalf("item[%d] = %+v, want %+v", i, g, w)
			}
			if (g.Position == nil) != (w.Position == nil) || (g.Position != nil && *g.Position != *w.Position) {
				t.Fatalf("item[%d].Position = %v, want %v", i, g.Position, w.Position)
			}
			if !maps.Equal(g.LockedFields, w.LockedFields) {
				t.Fatalf("item[%d].LockedFields = %v, want %v", i, g.LockedFields, w.LockedFields)
			}
		}
	}

	t.Run("strips any locks from desired rows on new or unlocked replacements", func(t *testing.T) {
		existing := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-old", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(1), Source: domain.MembershipSourceResolver},
		}
		desired := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-old", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(2), Source: domain.MembershipSourceResolver, LockedFields: domain.FieldLocks{"is_primary": true}},
			{VolumeID: volID, SeriesID: "s-new", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: fPtr(3), Source: domain.MembershipSourceMetadata, LockedFields: domain.FieldLocks{"position": true}},
		}
		got := reconcileAutomaticMemberships(volID, existing, desired)
		assertSet(t, got, []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-old", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(2), Source: domain.MembershipSourceResolver},
			{VolumeID: volID, SeriesID: "s-new", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: fPtr(3), Source: domain.MembershipSourceMetadata},
		})
	})

	t.Run("position-only lock on primary allows replacement without old series in desired", func(t *testing.T) {
		existing := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-1", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(5), Source: domain.MembershipSourceResolver, LockedFields: domain.FieldLocks{"position": true}},
		}
		desired := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-2", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(1), Source: domain.MembershipSourceResolver},
		}
		got := reconcileAutomaticMemberships(volID, existing, desired)
		assertSet(t, got, []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-2", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(1), Source: domain.MembershipSourceResolver},
			{VolumeID: volID, SeriesID: "s-1", IsPrimary: false, Kind: domain.MembershipKindSeries, Position: fPtr(5), Source: domain.MembershipSourceResolver, LockedFields: domain.FieldLocks{"position": true}},
		})
	})

	t.Run("position-only lock on primary allows replacement with old series as desired secondary", func(t *testing.T) {
		existing := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-1", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(5), Source: domain.MembershipSourceResolver, LockedFields: domain.FieldLocks{"position": true}},
		}
		desired := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-2", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(1), Source: domain.MembershipSourceResolver},
			{VolumeID: volID, SeriesID: "s-1", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: fPtr(99), Source: domain.MembershipSourceMetadata},
		}
		got := reconcileAutomaticMemberships(volID, existing, desired)
		assertSet(t, got, []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-2", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(1), Source: domain.MembershipSourceResolver},
			{VolumeID: volID, SeriesID: "s-1", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: fPtr(5), Source: domain.MembershipSourceMetadata, LockedFields: domain.FieldLocks{"position": true}},
		})
	})

	t.Run("position-only lock on primary preserves primary when standalone desired", func(t *testing.T) {
		existing := []domain.SeriesMembership{
			{VolumeID: volID, SeriesID: "s-1", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(5), Source: domain.MembershipSourceResolver, LockedFields: domain.FieldLocks{"position": true}},
		}
		got := reconcileAutomaticMemberships(volID, existing, nil)
		assertSet(t, got, existing)
	})

	t.Run("primary-status locks keep primary pinned across divergent replacement", func(t *testing.T) {
		for _, lockKey := range []string{"is_primary", "membership", "*"} {
			existing := []domain.SeriesMembership{
				{VolumeID: volID, SeriesID: "s-1", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(5), Source: domain.MembershipSourceResolver, LockedFields: domain.FieldLocks{lockKey: true}},
			}
			desired := []domain.SeriesMembership{
				{VolumeID: volID, SeriesID: "s-2", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: fPtr(1), Source: domain.MembershipSourceResolver},
			}
			got := reconcileAutomaticMemberships(volID, existing, desired)
			assertSet(t, got, existing)
		}
	})
}
