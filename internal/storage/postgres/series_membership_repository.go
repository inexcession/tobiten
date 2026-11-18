package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

var _ storage.SeriesMembershipRepository = (*SeriesMembershipRepository)(nil)

// SeriesMembershipRepository implements storage.SeriesMembershipRepository backed by PostgreSQL and sqlc.
// It executes multi-statement updates inside a transaction so 0004_catalogue.sql deferred
// constraint triggers enforce primary uniqueness, same-library ownership, and cover-volume integrity.
type SeriesMembershipRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
}

// NewSeriesMembershipRepository creates a new SeriesMembershipRepository using the provided database handle.
func NewSeriesMembershipRepository(db DBTX) *SeriesMembershipRepository {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	var scope *txScope
	if extTx, ok := db.(pgx.Tx); ok {
		scope = newCallerTxScope(extTx)
	}
	return &SeriesMembershipRepository{
		q:     New(db),
		tx:    beginner,
		scope: scope,
	}
}

func (r *SeriesMembershipRepository) runWriteTx(ctx context.Context, label string, fn func(q *Queries) error) error {
	if r == nil {
		return errors.New("series membership repository is required")
	}
	return runTx(ctx, r.tx, r.scope, label, func(child *txScope) error {
		return fn(New(child.tx))
	})
}

// ListMembershipsByVolume returns all series memberships for volumeID, primary first then by series_id.
func (r *SeriesMembershipRepository) ListMembershipsByVolume(ctx context.Context, volumeID string) ([]domain.SeriesMembership, error) {
	if r == nil {
		return nil, errors.New("series membership repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return nil, domain.ErrEmptyVolumeID
	}
	rows, err := r.q.ListSeriesMembershipsByVolumeForWrite(ctx, volumeID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return seriesMembershipModelsToDomain(rows)
}

// ListMembershipsBySeries returns all memberships for seriesID, ordered by primary status, position, and volume_id.
func (r *SeriesMembershipRepository) ListMembershipsBySeries(ctx context.Context, seriesID string) ([]domain.SeriesMembership, error) {
	if r == nil {
		return nil, errors.New("series membership repository is required")
	}
	if strings.TrimSpace(seriesID) == "" {
		return nil, domain.ErrEmptySeriesID
	}
	rows, err := r.q.ListSeriesMembershipsBySeriesForWrite(ctx, seriesID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return seriesMembershipModelsToDomain(rows)
}

// GetMembership returns the membership for (volumeID, seriesID), or storage.ErrNotFound.
func (r *SeriesMembershipRepository) GetMembership(ctx context.Context, volumeID, seriesID string) (*domain.SeriesMembership, error) {
	if r == nil {
		return nil, errors.New("series membership repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return nil, domain.ErrEmptyVolumeID
	}
	if strings.TrimSpace(seriesID) == "" {
		return nil, domain.ErrEmptySeriesID
	}
	row, err := r.q.GetSeriesMembershipForWrite(ctx, GetSeriesMembershipForWriteParams{
		VolumeID: volumeID,
		SeriesID: seriesID,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	out, err := seriesMembershipModelToDomain(row)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// SetVolumeMemberships replaces the full membership set of volumeID inside a deferred-constraint transaction.
// An empty slice detaches volumeID to standalone (zero memberships).
func (r *SeriesMembershipRepository) SetVolumeMemberships(
	ctx context.Context,
	volumeID string,
	memberships []domain.SeriesMembership,
) error {
	if r == nil {
		return errors.New("series membership repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return domain.ErrEmptyVolumeID
	}
	if err := domain.ValidateSeriesMemberships(volumeID, memberships); err != nil {
		return err
	}

	return r.runWriteTx(ctx, "set volume memberships", func(q *Queries) error {
		if _, err := q.LockVolumeForMembershipUpdate(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
		existingRows, err := q.ListSeriesMembershipsByVolumeForUpdate(ctx, volumeID)
		if err != nil {
			return mapPgError(err)
		}
		existing, err := seriesMembershipModelsToDomain(existingRows)
		if err != nil {
			return err
		}
		return applyMembershipDiff(ctx, q, volumeID, existing, memberships)
	})
}

// DetachVolumeToStandalone removes all series memberships for volumeID inside a deferred-constraint transaction.
func (r *SeriesMembershipRepository) DetachVolumeToStandalone(ctx context.Context, volumeID string) error {
	return r.SetVolumeMemberships(ctx, volumeID, nil)
}

// PromotePrimary atomically promotes an existing membership (volumeID, seriesID) to primary
// and demotes the prior primary within a single deferred-constraint transaction.
func (r *SeriesMembershipRepository) PromotePrimary(ctx context.Context, volumeID, seriesID string) error {
	if r == nil {
		return errors.New("series membership repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return domain.ErrEmptyVolumeID
	}
	if strings.TrimSpace(seriesID) == "" {
		return domain.ErrEmptySeriesID
	}

	return r.runWriteTx(ctx, "promote primary membership", func(q *Queries) error {
		if _, err := q.LockVolumeForMembershipUpdate(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
		existingRows, err := q.ListSeriesMembershipsByVolumeForUpdate(ctx, volumeID)
		if err != nil {
			return mapPgError(err)
		}

		var found bool
		var alreadyPrimary bool
		for _, row := range existingRows {
			if row.SeriesID == seriesID {
				found = true
				alreadyPrimary = row.IsPrimary
				break
			}
		}
		if !found {
			return storage.ErrNotFound
		}
		if alreadyPrimary {
			return nil
		}

		// Demote before promoting so the non-deferrable partial unique index
		// idx_memberships_one_primary is never transiently violated.
		if err := q.DemotePrimarySeriesMembership(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
		if _, err := q.PromotePrimarySeriesMembership(ctx, PromotePrimarySeriesMembershipParams{
			VolumeID: volumeID,
			SeriesID: seriesID,
		}); err != nil {
			return mapCatalogueWriteError(err)
		}
		return nil
	})
}

// SyncResolverMemberships reconciles automatically resolved memberships for volumeID
// while preserving existing MANUAL and locked memberships and locked fields.
func (r *SeriesMembershipRepository) SyncResolverMemberships(
	ctx context.Context,
	volumeID string,
	desired []domain.SeriesMembership,
) error {
	if r == nil {
		return errors.New("series membership repository is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return domain.ErrEmptyVolumeID
	}
	if err := domain.ValidateSeriesMemberships(volumeID, desired); err != nil {
		return err
	}
	for i := range desired {
		if desired[i].Source == domain.MembershipSourceManual {
			return fmt.Errorf("%w: automatic resolver sync cannot supply MANUAL source", domain.ErrInvalidMembershipSource)
		}
	}

	return r.runWriteTx(ctx, "sync resolver memberships", func(q *Queries) error {
		if _, err := q.LockVolumeForMembershipUpdate(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
		existingRows, err := q.ListSeriesMembershipsByVolumeForUpdate(ctx, volumeID)
		if err != nil {
			return mapPgError(err)
		}
		existing, err := seriesMembershipModelsToDomain(existingRows)
		if err != nil {
			return err
		}
		target := reconcileAutomaticMemberships(volumeID, existing, desired)
		if err := domain.ValidateSeriesMemberships(volumeID, target); err != nil {
			return err
		}
		return applyMembershipDiff(ctx, q, volumeID, existing, target)
	})
}

func applyMembershipDiff(
	ctx context.Context,
	q *Queries,
	volumeID string,
	existing []domain.SeriesMembership,
	target []domain.SeriesMembership,
) error {
	existingBySeries := indexMembershipsBySeries(existing)
	targetBySeries := indexMembershipsBySeries(target)

	var toDelete []string
	var needDemotePrimary bool
	for _, e := range existing {
		t, kept := targetBySeries[e.SeriesID]
		if !kept {
			toDelete = append(toDelete, e.SeriesID)
			continue
		}
		if e.IsPrimary && !t.IsPrimary {
			needDemotePrimary = true
		}
	}

	var toUpdate []domain.SeriesMembership
	var toInsert []domain.SeriesMembership
	for _, t := range target {
		e, exists := existingBySeries[t.SeriesID]
		if !exists {
			toInsert = append(toInsert, t)
			continue
		}
		if !seriesMembershipsEqual(e, t) {
			toUpdate = append(toUpdate, t)
		}
	}

	if len(toDelete) > 0 {
		if err := q.DeleteSeriesMembershipsBySeriesIDs(ctx, DeleteSeriesMembershipsBySeriesIDsParams{
			VolumeID:  volumeID,
			SeriesIds: toDelete,
		}); err != nil {
			return mapCatalogueWriteError(err)
		}
	}

	// Demote any kept primary before promoting/inserting the replacement primary so
	// idx_memberships_one_primary is satisfied at every statement boundary.
	if needDemotePrimary {
		if err := q.DemotePrimarySeriesMembership(ctx, volumeID); err != nil {
			return mapPgError(err)
		}
	}

	for _, m := range toUpdate {
		lockedBytes, err := encodeJSONOrEmptyObject(m.LockedFields)
		if err != nil {
			return fmt.Errorf("encoding series_membership locked_fields: %w", err)
		}
		if _, err := q.UpdateSeriesMembership(ctx, UpdateSeriesMembershipParams{
			VolumeID:     volumeID,
			SeriesID:     m.SeriesID,
			IsPrimary:    m.IsPrimary,
			Kind:         string(m.Kind),
			Position:     optFloat64ToFloat8(m.Position),
			Source:       string(m.Source),
			LockedFields: lockedBytes,
		}); err != nil {
			return mapCatalogueWriteError(err)
		}
	}

	for _, m := range toInsert {
		lockedBytes, err := encodeJSONOrEmptyObject(m.LockedFields)
		if err != nil {
			return fmt.Errorf("encoding series_membership locked_fields: %w", err)
		}
		if _, err := q.InsertSeriesMembership(ctx, InsertSeriesMembershipParams{
			VolumeID:     volumeID,
			SeriesID:     m.SeriesID,
			IsPrimary:    m.IsPrimary,
			Kind:         string(m.Kind),
			Position:     optFloat64ToFloat8(m.Position),
			Source:       string(m.Source),
			LockedFields: lockedBytes,
		}); err != nil {
			return mapCatalogueWriteError(err)
		}
	}

	return nil
}

func reconcileAutomaticMemberships(
	volumeID string,
	existing []domain.SeriesMembership,
	desired []domain.SeriesMembership,
) []domain.SeriesMembership {
	existingBySeries := indexMembershipsBySeries(existing)
	desiredBySeries := indexMembershipsBySeries(desired)
	existingPrimary := findPrimaryMembership(existing)
	desiredPrimary := findPrimaryMembership(desired)

	primarySeriesID := resolveTargetPrimarySeriesID(existing, existingBySeries, desiredBySeries, existingPrimary, desiredPrimary)

	targetBySeries := make(map[string]domain.SeriesMembership, len(existing)+len(desired))
	for _, e := range existing {
		if e.Source == domain.MembershipSourceManual {
			targetBySeries[e.SeriesID] = cloneSeriesMembership(e)
			continue
		}
		if e.LockedFields.HasAnyLock() {
			if d, ok := desiredBySeries[e.SeriesID]; ok {
				targetBySeries[e.SeriesID] = mergeLockedMembership(e, d, primarySeriesID == e.SeriesID)
			} else {
				targetBySeries[e.SeriesID] = cloneSeriesMembership(e)
			}
			continue
		}
		// Keep an unprotected existing primary when automatic sync could not take
		// the primary slot (e.g. standalone desired with surviving protected secondaries,
		// or desired primary colliding with a MANUAL/is_primary-locked secondary).
		if e.SeriesID == primarySeriesID && (desiredPrimary == nil || desiredPrimary.SeriesID != e.SeriesID) {
			targetBySeries[e.SeriesID] = cloneSeriesMembership(e)
		}
	}

	for _, d := range desired {
		if _, exists := targetBySeries[d.SeriesID]; exists {
			continue
		}
		if d.IsPrimary && d.SeriesID != primarySeriesID {
			// A protected or retained primary already owns the primary slot; do not
			// demote the overridden automatic primary into a spurious secondary.
			continue
		}
		m := cloneSeriesMembership(d)
		m.VolumeID = volumeID
		m.IsPrimary = d.SeriesID == primarySeriesID
		targetBySeries[d.SeriesID] = m
	}

	out := make([]domain.SeriesMembership, 0, len(targetBySeries))
	for _, m := range targetBySeries {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b domain.SeriesMembership) int {
		if a.IsPrimary != b.IsPrimary {
			if a.IsPrimary {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.SeriesID, b.SeriesID)
	})
	return out
}

func resolveTargetPrimarySeriesID(
	existing []domain.SeriesMembership,
	existingBySeries map[string]domain.SeriesMembership,
	desiredBySeries map[string]domain.SeriesMembership,
	existingPrimary *domain.SeriesMembership,
	desiredPrimary *domain.SeriesMembership,
) string {
	if existingPrimary != nil {
		_, inDesired := desiredBySeries[existingPrimary.SeriesID]
		if isImmutablePrimaryMembership(*existingPrimary, inDesired) {
			return existingPrimary.SeriesID
		}
	}

	if desiredPrimary != nil {
		e, exists := existingBySeries[desiredPrimary.SeriesID]
		if !exists || !isPrimaryPromotionForbidden(e) {
			return desiredPrimary.SeriesID
		}
		if existingPrimary != nil {
			return existingPrimary.SeriesID
		}
		return ""
	}

	if existingPrimary != nil && hasProtectedMembership(existing) {
		return existingPrimary.SeriesID
	}
	return ""
}

func hasProtectedMembership(memberships []domain.SeriesMembership) bool {
	for i := range memberships {
		if memberships[i].IsProtectedFromAutoSync() {
			return true
		}
	}
	return false
}

func isPrimaryFieldLocked(locks domain.FieldLocks) bool {
	return locks.IsLocked("is_primary") || locks.IsLocked("membership") || locks.IsLocked("*")
}

func isImmutablePrimaryMembership(m domain.SeriesMembership, inDesired bool) bool {
	if !m.IsPrimary {
		return false
	}
	if m.Source == domain.MembershipSourceManual || isPrimaryFieldLocked(m.LockedFields) {
		return true
	}
	return m.LockedFields.HasAnyLock() && !inDesired
}

func isPrimaryPromotionForbidden(m domain.SeriesMembership) bool {
	return !m.IsPrimary && (m.Source == domain.MembershipSourceManual || isPrimaryFieldLocked(m.LockedFields))
}

func mergeLockedMembership(existing, desired domain.SeriesMembership, becomePrimary bool) domain.SeriesMembership {
	out := cloneSeriesMembership(existing)
	out.IsPrimary = becomePrimary

	wholeLocked := existing.LockedFields.IsLocked("*")
	if !wholeLocked && !existing.LockedFields.IsLocked("kind") {
		out.Kind = desired.Kind
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("position") {
		out.Position = cloneFloatPtr(desired.Position)
	}
	if !wholeLocked && !existing.LockedFields.IsLocked("source") {
		out.Source = desired.Source
	}
	return out
}

func indexMembershipsBySeries(memberships []domain.SeriesMembership) map[string]domain.SeriesMembership {
	bySeries := make(map[string]domain.SeriesMembership, len(memberships))
	for _, m := range memberships {
		bySeries[m.SeriesID] = m
	}
	return bySeries
}

func findPrimaryMembership(memberships []domain.SeriesMembership) *domain.SeriesMembership {
	for i := range memberships {
		if memberships[i].IsPrimary {
			m := memberships[i]
			return &m
		}
	}
	return nil
}

func cloneSeriesMembership(m domain.SeriesMembership) domain.SeriesMembership {
	out := m
	out.Position = cloneFloatPtr(m.Position)
	out.LockedFields = maps.Clone(m.LockedFields)
	return out
}

func cloneFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func seriesMembershipsEqual(a, b domain.SeriesMembership) bool {
	return a.VolumeID == b.VolumeID &&
		a.SeriesID == b.SeriesID &&
		a.IsPrimary == b.IsPrimary &&
		a.Kind == b.Kind &&
		optFloatEqual(a.Position, b.Position) &&
		a.Source == b.Source &&
		maps.Equal(a.LockedFields, b.LockedFields)
}

func optFloatEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func seriesMembershipModelsToDomain(rows []SeriesMembership) ([]domain.SeriesMembership, error) {
	out := make([]domain.SeriesMembership, len(rows))
	for i, row := range rows {
		m, err := seriesMembershipModelToDomain(row)
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

func seriesMembershipModelToDomain(row SeriesMembership) (domain.SeriesMembership, error) {
	lockedFields, err := decodeFieldLocks(row.LockedFields)
	if err != nil {
		return domain.SeriesMembership{}, fmt.Errorf("decoding series_membership locked_fields: %w", err)
	}
	var pos *float64
	if row.Position.Valid {
		p := row.Position.Float64
		pos = &p
	}
	return domain.SeriesMembership{
		VolumeID:     row.VolumeID,
		SeriesID:     row.SeriesID,
		IsPrimary:    row.IsPrimary,
		Kind:         domain.MembershipKind(row.Kind),
		Position:     pos,
		Source:       domain.MembershipSource(row.Source),
		LockedFields: lockedFields,
	}, nil
}
