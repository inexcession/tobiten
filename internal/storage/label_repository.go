package storage

import (
	"context"

	"github.com/inexcession/tobiten/internal/domain"
)

// LabelRepository manages labels, item label assignments, and user label policies.
// SERVER labels live in the global server namespace (owner_id IS NULL); USER labels
// live in per-user personal namespaces (owner_id = user.id). Only SERVER labels may
// be referenced by user_label_policies.
type LabelRepository interface {
	// WithTx executes fn inside a transaction (or savepoint when already inside one).
	WithTx(ctx context.Context, fn func(repo LabelRepository) error) error

	// CreateLabel persists a new label (SERVER or USER scope).
	CreateLabel(ctx context.Context, label *domain.Label) error

	// GetLabelByID retrieves a label by its ID.
	GetLabelByID(ctx context.Context, id string) (*domain.Label, error)

	// GetServerLabelByName retrieves a SERVER-scoped label by name.
	GetServerLabelByName(ctx context.Context, name string) (*domain.Label, error)

	// GetUserLabelByName retrieves a USER-scoped label owned by ownerID by name.
	GetUserLabelByName(ctx context.Context, ownerID, name string) (*domain.Label, error)

	// ListServerLabels returns all SERVER-scoped labels ordered deterministically by name.
	ListServerLabels(ctx context.Context) ([]domain.Label, error)

	// ListUserLabels returns all USER-scoped labels owned by ownerID ordered by name.
	ListUserLabels(ctx context.Context, ownerID string) ([]domain.Label, error)

	// RenameLabel updates the display name of a label while preserving its ID,
	// item_labels assignments, and user_label_policies references.
	// Returns ErrProtectedLabel if the target is the canonical trash label.
	RenameLabel(ctx context.Context, id, newName string) (*domain.Label, error)

	// DeleteLabel removes a label by ID. Returns ErrProtectedLabel for the canonical
	// trash label, ErrLabelInUse if any user_label_policies still reference the label
	// (ON DELETE RESTRICT), or ErrNotFound if no such label exists.
	DeleteLabel(ctx context.Context, id string) error

	// EnsureBuiltInServerLabels idempotently ensures built-in SERVER labels
	// (such as marked_for_deletion) exist in storage.
	EnsureBuiltInServerLabels(ctx context.Context) error

	// EnsureUserBuiltInLabels idempotently ensures personal built-in USER labels
	// (want-to-read, favorite, dnf) exist for ownerID while preserving existing IDs
	// and item assignments.
	EnsureUserBuiltInLabels(ctx context.Context, ownerID string) error

	// AssignItemLabel attaches a label to a SERIES, VOLUME, or ASSET idempotently.
	AssignItemLabel(ctx context.Context, itemLabel *domain.ItemLabel) error

	// RemoveItemLabel detaches a label from a SERIES, VOLUME, or ASSET.
	// Returns ErrNotFound if the assignment does not exist.
	RemoveItemLabel(ctx context.Context, itemLabel *domain.ItemLabel) error

	// ListItemLabels returns all labels attached to (itemType, itemID) visible to
	// viewerUserID: all SERVER labels plus USER labels owned by viewerUserID,
	// ordered by scope and name.
	ListItemLabels(ctx context.Context, viewerUserID string, itemType domain.ItemType, itemID string) ([]domain.Label, error)

	// ListServerItemLabels returns only SERVER-scoped labels attached to (itemType, itemID)
	// ordered by name, without exposing any user's private USER labels.
	ListServerItemLabels(ctx context.Context, itemType domain.ItemType, itemID string) ([]domain.Label, error)

	// ListAllItemLabelsForMaintenance returns every label (SERVER and all USER scopes)
	// attached to (itemType, itemID) for privileged maintenance/purge operations.
	ListAllItemLabelsForMaintenance(ctx context.Context, itemType domain.ItemType, itemID string) ([]domain.Label, error)

	// ListItemIDsByLabel returns all item IDs of itemType tagged with labelID visible
	// to viewerUserID (a SERVER label or a USER label owned by viewerUserID) ordered by item_id.
	// Returns an empty slice when labelID is another user's private USER label.
	ListItemIDsByLabel(ctx context.Context, viewerUserID, labelID string, itemType domain.ItemType) ([]string, error)

	// RemoveAllItemLabelsForItem removes all label assignments for the specified item
	// (e.g. during item purge).
	RemoveAllItemLabelsForItem(ctx context.Context, itemType domain.ItemType, itemID string) error

	// SetUserLabelPolicy creates or updates a user's policy (ALLOW or RESTRICT) for a SERVER label.
	// Returns ErrInvalidLabelPolicy if labelID does not refer to a SERVER-scoped label.
	SetUserLabelPolicy(ctx context.Context, policy *domain.UserLabelPolicy) error

	// RemoveUserLabelPolicy removes a user's policy for a label.
	// Returns ErrNotFound if no matching policy exists.
	RemoveUserLabelPolicy(ctx context.Context, userID, labelID string) error

	// ListUserLabelPolicies returns all label policies configured for userID ordered by label_id.
	ListUserLabelPolicies(ctx context.Context, userID string) ([]domain.UserLabelPolicy, error)

	// ListLabelPoliciesByLabel returns all user policies referencing labelID ordered by user_id.
	ListLabelPoliciesByLabel(ctx context.Context, labelID string) ([]domain.UserLabelPolicy, error)
}
