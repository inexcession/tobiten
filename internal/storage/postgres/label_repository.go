package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

var _ storage.LabelRepository = (*LabelRepository)(nil)

// LabelRepository implements storage.LabelRepository backed by PostgreSQL.
type LabelRepository struct {
	q  *Queries
	tx TxBeginner
}

// NewLabelRepository creates a new LabelRepository using the provided database handle.
func NewLabelRepository(db DBTX) *LabelRepository {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	return &LabelRepository{
		q:  New(db),
		tx: beginner,
	}
}

// WithTx executes fn within a PostgreSQL transaction (or nested savepoint when already in a tx).
func (r *LabelRepository) WithTx(ctx context.Context, fn func(repo storage.LabelRepository) error) error {
	if r.tx == nil {
		return errors.New("db connection does not support transactions")
	}
	tx, err := r.tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	txRepo := &LabelRepository{
		q:  r.q.WithTx(tx),
		tx: tx,
	}
	if err := fn(txRepo); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func toDomainLabel(row Label) domain.Label {
	return domain.Label{
		ID:      row.ID,
		Scope:   domain.LabelScope(row.Scope),
		OwnerID: row.OwnerID,
		Name:    row.Name,
		BuiltIn: row.BuiltIn,
	}
}

func toDomainLabels(rows []Label) []domain.Label {
	out := make([]domain.Label, len(rows))
	for i, row := range rows {
		out[i] = toDomainLabel(row)
	}
	return out
}

func toDomainPolicies(rows []UserLabelPolicy) []domain.UserLabelPolicy {
	out := make([]domain.UserLabelPolicy, len(rows))
	for i, row := range rows {
		out[i] = domain.UserLabelPolicy{
			UserID:  row.UserID,
			LabelID: row.LabelID,
			Effect:  domain.PolicyEffect(row.Effect),
		}
	}
	return out
}

func mapLabelWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return storage.ErrNotFound
	}
	return mapPgError(err)
}

func mapDeleteLabelError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "23001" || pgErr.Code == "23503") {
		return storage.ErrLabelInUse
	}
	return mapPgError(err)
}

func mapPolicyWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503":
			return storage.ErrNotFound
		case "P0001":
			if strings.Contains(pgErr.Message, "got <NULL>") {
				return storage.ErrNotFound
			}
			if strings.Contains(pgErr.Message, "only permit SERVER labels") {
				return storage.ErrInvalidLabelPolicy
			}
		}
	}
	return mapPgError(err)
}

func ensureUserBuiltInLabels(ctx context.Context, q *Queries, ownerID string) error {
	if strings.TrimSpace(ownerID) == "" {
		return domain.ErrEmptyUserID
	}
	ownerPtr := ownerID
	for _, name := range domain.BuiltInUserLabelNames() {
		id, err := domain.NewID()
		if err != nil {
			return fmt.Errorf("generating built-in user label ID: %w", err)
		}
		if err := q.EnsureUserBuiltInLabel(ctx, EnsureUserBuiltInLabelParams{
			ID:      id,
			OwnerID: &ownerPtr,
			Name:    name,
		}); err != nil {
			return mapLabelWriteError(err)
		}
	}
	return nil
}

// CreateLabel persists a new label (SERVER or USER scope).
func (r *LabelRepository) CreateLabel(ctx context.Context, label *domain.Label) error {
	if label == nil {
		return errors.New("label must not be nil")
	}
	if err := label.Validate(); err != nil {
		return fmt.Errorf("validating label: %w", err)
	}
	err := r.q.CreateLabel(ctx, CreateLabelParams{
		ID:      label.ID,
		Scope:   string(label.Scope),
		OwnerID: label.OwnerID,
		Name:    label.Name,
		BuiltIn: label.BuiltIn,
	})
	return mapLabelWriteError(err)
}

// GetLabelByID retrieves a label by its ID.
func (r *LabelRepository) GetLabelByID(ctx context.Context, id string) (*domain.Label, error) {
	row, err := r.q.GetLabelByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	lbl := toDomainLabel(row)
	return &lbl, nil
}

// GetServerLabelByName retrieves a SERVER-scoped label by name.
func (r *LabelRepository) GetServerLabelByName(ctx context.Context, name string) (*domain.Label, error) {
	row, err := r.q.GetServerLabelByName(ctx, name)
	if err != nil {
		return nil, mapPgError(err)
	}
	lbl := toDomainLabel(row)
	return &lbl, nil
}

// GetUserLabelByName retrieves a USER-scoped label owned by ownerID by name.
func (r *LabelRepository) GetUserLabelByName(ctx context.Context, ownerID, name string) (*domain.Label, error) {
	if strings.TrimSpace(ownerID) == "" {
		return nil, domain.ErrEmptyUserID
	}
	ownerPtr := ownerID
	row, err := r.q.GetUserLabelByName(ctx, GetUserLabelByNameParams{
		OwnerID: &ownerPtr,
		Name:    name,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	lbl := toDomainLabel(row)
	return &lbl, nil
}

// ListServerLabels returns all SERVER-scoped labels ordered deterministically by name.
func (r *LabelRepository) ListServerLabels(ctx context.Context) ([]domain.Label, error) {
	rows, err := r.q.ListServerLabels(ctx)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLabels(rows), nil
}

// ListUserLabels returns all USER-scoped labels owned by ownerID ordered by name.
func (r *LabelRepository) ListUserLabels(ctx context.Context, ownerID string) ([]domain.Label, error) {
	if strings.TrimSpace(ownerID) == "" {
		return nil, domain.ErrEmptyUserID
	}
	ownerPtr := ownerID
	rows, err := r.q.ListUserLabels(ctx, &ownerPtr)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLabels(rows), nil
}

// RenameLabel updates the display name of a label while preserving its ID,
// item_labels assignments, and user_label_policies references.
func (r *LabelRepository) RenameLabel(ctx context.Context, id, newName string) (*domain.Label, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("label id must not be empty")
	}
	trimmed := strings.TrimSpace(newName)
	if trimmed == "" {
		return nil, errors.New("label name must not be empty")
	}
	row, err := r.q.RenameLabel(ctx, RenameLabelParams{
		ID:   id,
		Name: trimmed,
	})
	if err != nil {
		return nil, mapLabelWriteError(err)
	}
	lbl := toDomainLabel(row)
	return &lbl, nil
}

// DeleteLabel removes a label by ID. Returns ErrLabelInUse if any user_label_policies
// still reference the label (ON DELETE RESTRICT), or ErrNotFound if no such label exists.
func (r *LabelRepository) DeleteLabel(ctx context.Context, id string) error {
	rows, err := r.q.DeleteLabel(ctx, id)
	if err != nil {
		return mapDeleteLabelError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// EnsureBuiltInServerLabels idempotently ensures built-in SERVER labels
// (such as marked_for_deletion) exist in storage.
func (r *LabelRepository) EnsureBuiltInServerLabels(ctx context.Context) error {
	for _, lbl := range domain.BuiltInServerLabels() {
		if err := lbl.Validate(); err != nil {
			return fmt.Errorf("validating built-in server label: %w", err)
		}
		if err := r.q.UpsertBuiltInServerLabel(ctx, UpsertBuiltInServerLabelParams{
			ID:   lbl.ID,
			Name: lbl.Name,
		}); err != nil {
			return mapLabelWriteError(err)
		}
	}
	return nil
}

// EnsureUserBuiltInLabels idempotently ensures personal built-in USER labels
// (want-to-read, favorite, dnf) exist for ownerID.
func (r *LabelRepository) EnsureUserBuiltInLabels(ctx context.Context, ownerID string) error {
	if r.tx != nil {
		return r.WithTx(ctx, func(txRepo storage.LabelRepository) error {
			return ensureUserBuiltInLabels(ctx, txRepo.(*LabelRepository).q, ownerID)
		})
	}
	return ensureUserBuiltInLabels(ctx, r.q, ownerID)
}

// AssignItemLabel attaches a label to a SERIES, VOLUME, or ASSET idempotently.
func (r *LabelRepository) AssignItemLabel(ctx context.Context, itemLabel *domain.ItemLabel) error {
	if itemLabel == nil {
		return errors.New("item label must not be nil")
	}
	if err := itemLabel.Validate(); err != nil {
		return fmt.Errorf("validating item label: %w", err)
	}
	err := r.q.AssignItemLabel(ctx, AssignItemLabelParams{
		LabelID:  itemLabel.LabelID,
		ItemType: string(itemLabel.ItemType),
		ItemID:   itemLabel.ItemID,
	})
	return mapLabelWriteError(err)
}

// RemoveItemLabel detaches a label from a SERIES, VOLUME, or ASSET.
// Returns ErrNotFound if the assignment does not exist.
func (r *LabelRepository) RemoveItemLabel(ctx context.Context, itemLabel *domain.ItemLabel) error {
	if itemLabel == nil {
		return errors.New("item label must not be nil")
	}
	if err := itemLabel.Validate(); err != nil {
		return fmt.Errorf("validating item label: %w", err)
	}
	rows, err := r.q.RemoveItemLabel(ctx, RemoveItemLabelParams{
		LabelID:  itemLabel.LabelID,
		ItemType: string(itemLabel.ItemType),
		ItemID:   itemLabel.ItemID,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ListItemLabels returns all labels attached to the specified item ordered by scope and name.
func (r *LabelRepository) ListItemLabels(ctx context.Context, itemType domain.ItemType, itemID string) ([]domain.Label, error) {
	rows, err := r.q.ListItemLabels(ctx, ListItemLabelsParams{
		ItemType: string(itemType),
		ItemID:   itemID,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainLabels(rows), nil
}

// ListItemIDsByLabel returns all item IDs of itemType tagged with labelID ordered by item_id.
func (r *LabelRepository) ListItemIDsByLabel(ctx context.Context, labelID string, itemType domain.ItemType) ([]string, error) {
	ids, err := r.q.ListItemIDsByLabel(ctx, ListItemIDsByLabelParams{
		LabelID:  labelID,
		ItemType: string(itemType),
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	if ids == nil {
		return []string{}, nil
	}
	return ids, nil
}

// RemoveAllItemLabelsForItem removes all label assignments for the specified item.
func (r *LabelRepository) RemoveAllItemLabelsForItem(ctx context.Context, itemType domain.ItemType, itemID string) error {
	err := r.q.RemoveAllItemLabelsForItem(ctx, RemoveAllItemLabelsForItemParams{
		ItemType: string(itemType),
		ItemID:   itemID,
	})
	return mapPgError(err)
}

// SetUserLabelPolicy creates or updates a user's policy (ALLOW or RESTRICT) for a SERVER label.
// Returns ErrInvalidLabelPolicy if labelID does not refer to a SERVER-scoped label.
func (r *LabelRepository) SetUserLabelPolicy(ctx context.Context, policy *domain.UserLabelPolicy) error {
	if policy == nil {
		return errors.New("user label policy must not be nil")
	}
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("validating user label policy: %w", err)
	}
	err := r.q.SetUserLabelPolicy(ctx, SetUserLabelPolicyParams{
		UserID:  policy.UserID,
		LabelID: policy.LabelID,
		Effect:  string(policy.Effect),
	})
	return mapPolicyWriteError(err)
}

// RemoveUserLabelPolicy removes a user's policy for a label.
// Returns ErrNotFound if no matching policy exists.
func (r *LabelRepository) RemoveUserLabelPolicy(ctx context.Context, userID, labelID string) error {
	rows, err := r.q.RemoveUserLabelPolicy(ctx, RemoveUserLabelPolicyParams{
		UserID:  userID,
		LabelID: labelID,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ListUserLabelPolicies returns all label policies configured for userID ordered by label_id.
func (r *LabelRepository) ListUserLabelPolicies(ctx context.Context, userID string) ([]domain.UserLabelPolicy, error) {
	rows, err := r.q.ListUserLabelPolicies(ctx, userID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainPolicies(rows), nil
}

// ListLabelPoliciesByLabel returns all user policies referencing labelID ordered by user_id.
func (r *LabelRepository) ListLabelPoliciesByLabel(ctx context.Context, labelID string) ([]domain.UserLabelPolicy, error) {
	rows, err := r.q.ListLabelPoliciesByLabel(ctx, labelID)
	if err != nil {
		return nil, mapPgError(err)
	}
	return toDomainPolicies(rows), nil
}
