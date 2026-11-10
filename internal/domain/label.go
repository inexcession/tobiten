package domain

import (
	"errors"
	"slices"
)

// Label domain errors.
var (
	ErrInvalidLabelScope     = errors.New("invalid label scope (must be SERVER or USER)")
	ErrServerLabelWithOwner  = errors.New("SERVER label must not have an owner ID")
	ErrUserLabelWithoutOwner = errors.New("USER label must have an owner ID")
	ErrEmptyLabelName        = errors.New("label name cannot be empty")
	ErrInvalidItemType       = errors.New("invalid item type (must be SERIES, VOLUME, or ASSET)")
	ErrInvalidPolicyEffect   = errors.New("invalid policy effect (must be ALLOW or RESTRICT)")
)

// LabelScope represents the scope of a label (ADR-0006 §5).
// SERVER labels drive access restriction and trash ('marked_for_deletion').
// USER labels are owner-private personal state ('want-to-read', 'favorite', 'dnf') and NEVER affect access.
type LabelScope string

const (
	LabelScopeServer LabelScope = "SERVER"
	LabelScopeUser   LabelScope = "USER"
)

// ValidLabelScopes returns all valid label scopes.
func ValidLabelScopes() []LabelScope {
	return []LabelScope{LabelScopeServer, LabelScopeUser}
}

// Label represents a scoped label (ADR-0006 §5).
type Label struct {
	ID      string     `json:"id"`
	Scope   LabelScope `json:"scope"`
	OwnerID *string    `json:"owner_id,omitempty"`
	Name    string     `json:"name"`
	BuiltIn bool       `json:"built_in"`
}

// Built-in label names.
const (
	// Server labels
	LabelMarkedForDeletion = "marked_for_deletion"

	// User labels
	LabelWantToRead = "want-to-read"
	LabelFavorite   = "favorite"
	LabelDNF        = "dnf"
)

// Validate checks the invariants of a Label matching database constraints.
func (l *Label) Validate() error {
	if l.Name == "" {
		return ErrEmptyLabelName
	}
	switch l.Scope {
	case LabelScopeServer:
		if l.OwnerID != nil {
			return ErrServerLabelWithOwner
		}
	case LabelScopeUser:
		if l.OwnerID == nil || *l.OwnerID == "" {
			return ErrUserLabelWithoutOwner
		}
	default:
		return ErrInvalidLabelScope
	}
	return nil
}

// ServerLabelIDs extracts label IDs only for labels whose Scope is LabelScopeServer.
// USER labels are ignored as they never affect access (ADR-0006 §5).
func ServerLabelIDs(labels []Label) []string {
	var ids []string
	for _, l := range labels {
		if l.Scope == LabelScopeServer {
			ids = append(ids, l.ID)
		}
	}
	return ids
}

// ItemType represents the polymorphic entity type targeted by an ItemLabel (ADR-0006 §5, M0 S-12).
type ItemType string

const (
	ItemTypeSeries ItemType = "SERIES"
	ItemTypeVolume ItemType = "VOLUME"
	ItemTypeAsset  ItemType = "ASSET"
)

// ValidItemTypes returns all valid item types.
func ValidItemTypes() []ItemType {
	return []ItemType{ItemTypeSeries, ItemTypeVolume, ItemTypeAsset}
}

// ItemLabel associates a label with a polymorphic item (ADR-0006 §5).
type ItemLabel struct {
	LabelID  string   `json:"label_id"`
	ItemType ItemType `json:"item_type"`
	ItemID   string   `json:"item_id"`
}

// Validate checks the invariants of an ItemLabel.
func (il *ItemLabel) Validate() error {
	if !slices.Contains(ValidItemTypes(), il.ItemType) {
		return ErrInvalidItemType
	}
	if il.LabelID == "" || il.ItemID == "" {
		return errors.New("label_id and item_id cannot be empty")
	}
	return nil
}

// PolicyEffect represents the effect of a label policy (ADR-0006 §5).
type PolicyEffect string

const (
	PolicyEffectAllow    PolicyEffect = "ALLOW"
	PolicyEffectRestrict PolicyEffect = "RESTRICT"
)

// UserLabelPolicy represents a per-user label access policy row (ADR-0006 §5).
type UserLabelPolicy struct {
	UserID  string       `json:"user_id"`
	LabelID string       `json:"label_id"`
	Effect  PolicyEffect `json:"effect"`
}

// Validate checks UserLabelPolicy invariants.
func (p *UserLabelPolicy) Validate() error {
	if p.UserID == "" {
		return ErrEmptyUserID
	}
	if p.LabelID == "" {
		return errors.New("label ID cannot be empty")
	}
	if p.Effect != PolicyEffectAllow && p.Effect != PolicyEffectRestrict {
		return ErrInvalidPolicyEffect
	}
	return nil
}

// GroupLabelPolicy represents an OIDC group-claim label access policy row (ADR-0006 §5).
type GroupLabelPolicy struct {
	GroupClaim string       `json:"group_claim"`
	LabelID    string       `json:"label_id"`
	Effect     PolicyEffect `json:"effect"`
}

// Validate checks GroupLabelPolicy invariants.
func (p *GroupLabelPolicy) Validate() error {
	if p.GroupClaim == "" {
		return errors.New("group claim cannot be empty")
	}
	if p.LabelID == "" {
		return errors.New("label ID cannot be empty")
	}
	if p.Effect != PolicyEffectAllow && p.Effect != PolicyEffectRestrict {
		return ErrInvalidPolicyEffect
	}
	return nil
}

// EffectiveLabelPolicy represents the merged, effective SERVER label policy for a subject
// (ADR-0006 §5).
//
// Rules:
//   - Only SERVER-scope labels drive policy.
//   - A subject holds one effect per label.
//   - Precedence between a user's own rows and groups' rows resolves by "RESTRICT wins" (M0 S-13).
//   - No ALLOW policy rows means all content is allowed, subject to RESTRICT rows (unless requiresAllowLabels is true).
//   - RESTRICT always wins over ALLOW.
//   - If requiresAllowLabels is true (decision I-5), the policy fails closed (denies everything) when no ALLOW rows exist.
//   - Top-down ALLOW (decision I-4): series must be allowed; volume with series is allowed only via series;
//     standalone volume uses its own labels; asset labels only restrict, never grant.
type EffectiveLabelPolicy struct {
	allowLabels         map[string]struct{}
	restrictLabels      map[string]struct{}
	requiresAllowLabels bool
}

// NewEffectiveLabelPolicy builds an EffectiveLabelPolicy from user and/or group policies,
// enforcing the "RESTRICT wins across subjects/groups" rule for conflicting policies on the same label ID.
// If requiresAllowLabels is true (e.g. for a Restricted reader, decision I-5), label evaluation fails
// closed (denies everything) when there are no ALLOW policy rows.
func NewEffectiveLabelPolicy(userPolicies []UserLabelPolicy, groupPolicies []GroupLabelPolicy, requiresAllowLabels bool) *EffectiveLabelPolicy {
	effects := make(map[string]PolicyEffect)

	apply := func(labelID string, effect PolicyEffect) {
		if current, exists := effects[labelID]; exists {
			// If already RESTRICT, RESTRICT stays.
			// If current is ALLOW and new is RESTRICT, RESTRICT wins.
			if effect == PolicyEffectRestrict || current == PolicyEffectRestrict {
				effects[labelID] = PolicyEffectRestrict
			}
		} else {
			effects[labelID] = effect
		}
	}

	for _, p := range userPolicies {
		apply(p.LabelID, p.Effect)
	}
	for _, p := range groupPolicies {
		apply(p.LabelID, p.Effect)
	}

	allowMap := make(map[string]struct{})
	restrictMap := make(map[string]struct{})

	for labelID, effect := range effects {
		switch effect {
		case PolicyEffectRestrict:
			restrictMap[labelID] = struct{}{}
		case PolicyEffectAllow:
			allowMap[labelID] = struct{}{}
		}
	}

	return &EffectiveLabelPolicy{
		allowLabels:         allowMap,
		restrictLabels:      restrictMap,
		requiresAllowLabels: requiresAllowLabels,
	}
}

// NewEffectiveLabelPolicyForPrincipal builds an EffectiveLabelPolicy for a principal,
// automatically determining requiresAllowLabels from the principal's roles (decision I-5).
// A nil principal must fail closed: return a policy that denies everything.
func NewEffectiveLabelPolicyForPrincipal(principal *Principal, userPolicies []UserLabelPolicy, groupPolicies []GroupLabelPolicy) *EffectiveLabelPolicy {
	if principal == nil {
		return NewEffectiveLabelPolicy(nil, nil, true)
	}
	return NewEffectiveLabelPolicy(userPolicies, groupPolicies, principal.RequiresAllowLabels())
}

// AllowCount returns the number of ALLOW policies in effect.
func (p *EffectiveLabelPolicy) AllowCount() int {
	return len(p.allowLabels)
}

// RestrictCount returns the number of RESTRICT policies in effect.
func (p *EffectiveLabelPolicy) RestrictCount() int {
	return len(p.restrictLabels)
}

// HasAllowLabels reports whether there are any ALLOW policy rows.
func (p *EffectiveLabelPolicy) HasAllowLabels() bool {
	return len(p.allowLabels) > 0
}

// RequiresAllowLabels reports whether this policy requires allow labels to grant any access (decision I-5).
func (p *EffectiveLabelPolicy) RequiresAllowLabels() bool {
	return p.requiresAllowLabels
}

// IsLabelRestricted reports whether the given label ID has a RESTRICT policy.
func (p *EffectiveLabelPolicy) IsLabelRestricted(labelID string) bool {
	_, ok := p.restrictLabels[labelID]
	return ok
}

// IsLabelAllowed reports whether the given label ID has an ALLOW policy.
func (p *EffectiveLabelPolicy) IsLabelAllowed(labelID string) bool {
	_, ok := p.allowLabels[labelID]
	return ok
}

// EvaluateLabels evaluates whether a set of applied SERVER label IDs is permitted
// under this policy (ADR-0006 §5, decision I-5).
//
// Evaluation rules:
//  1. RESTRICT always wins: if ANY label is in restrictLabels, access is DENIED (returns false).
//  2. Fail closed (decision I-5): if requiresAllowLabels is true and no ALLOW rows exist, access is DENIED (returns false).
//  3. If there are NO allowLabels: all content is allowed (returns true).
//  4. If there ARE allowLabels: at least one label must be in allowLabels (returns true if any match, false otherwise).
func (p *EffectiveLabelPolicy) EvaluateLabels(labelIDs []string) bool {
	// 1. Check restrictions: RESTRICT always wins
	for _, id := range labelIDs {
		if _, restricted := p.restrictLabels[id]; restricted {
			return false
		}
	}

	// 2. Fail closed for label-required roles (decision I-5)
	if p.requiresAllowLabels && len(p.allowLabels) == 0 {
		return false
	}

	// 3. If no ALLOW rows exist, all non-restricted content is allowed
	if len(p.allowLabels) == 0 {
		return true
	}

	// 4. If ALLOW rows exist, at least one label must match ALLOW
	for _, id := range labelIDs {
		if _, allowed := p.allowLabels[id]; allowed {
			return true
		}
	}

	return false
}

// EvaluateSeries evaluates whether a series with the given SERVER labels is accessible.
func (p *EffectiveLabelPolicy) EvaluateSeries(seriesLabels []string) bool {
	return p.EvaluateLabels(seriesLabels)
}

// EvaluateVolume evaluates whether a volume with its own SERVER labels and
// memberships in series (each having their own SERVER labels) is accessible.
//
// Top-down ALLOW semantics (ADR-0006 §5, decisions I-4, I-5):
//   - RESTRICT on the volume or any parent series always hides it.
//   - Fail closed (I-5): if requiresAllowLabels is true and no ALLOW rows exist, hidden.
//   - If no ALLOW rows exist: visible.
//   - When ALLOW rows exist (I-4):
//   - A volume with series memberships is visible only if at least one of its series
//     carries an allowed label itself; the volume's own ALLOW labels do not count.
//   - A standalone volume (no memberships) uses its own ALLOW labels.
func (p *EffectiveLabelPolicy) EvaluateVolume(volumeLabels []string, seriesLabelsList [][]string) bool {
	// 1. Check RESTRICT on volume itself
	for _, id := range volumeLabels {
		if _, restricted := p.restrictLabels[id]; restricted {
			return false
		}
	}

	// 2. Check RESTRICT on any series membership
	for _, sLabels := range seriesLabelsList {
		for _, id := range sLabels {
			if _, restricted := p.restrictLabels[id]; restricted {
				return false
			}
		}
	}

	// 3. Fail closed for label-required roles (decision I-5)
	if p.requiresAllowLabels && len(p.allowLabels) == 0 {
		return false
	}

	// 4. If no ALLOW policies exist, all non-restricted content is allowed
	if len(p.allowLabels) == 0 {
		return true
	}

	// 5. When ALLOW policies exist (top-down ALLOW, decision I-4):
	// A standalone volume (no memberships) uses its own labels
	if len(seriesLabelsList) == 0 {
		for _, id := range volumeLabels {
			if _, allowed := p.allowLabels[id]; allowed {
				return true
			}
		}
		return false
	}

	// A volume with series memberships is visible only if at least one of its series
	// carries an allowed label itself; the volume's own ALLOW labels do not count.
	for _, sLabels := range seriesLabelsList {
		for _, id := range sLabels {
			if _, allowed := p.allowLabels[id]; allowed {
				return true
			}
		}
	}

	return false
}

// EvaluateAsset evaluates whether an asset is accessible (ADR-0006 §5, decisions I-4, I-5).
//
// Rules:
//   - Asset labels can only restrict, never grant (I-4).
//   - An asset is visible only if its parent volume is visible (EvaluateVolume).
//   - An asset with its own RESTRICT label is hidden.
//   - An allowed asset in a non-allowed volume is hidden.
func (p *EffectiveLabelPolicy) EvaluateAsset(assetLabels []string, volumeLabels []string, seriesLabelsList [][]string) bool {
	if !p.EvaluateVolume(volumeLabels, seriesLabelsList) {
		return false
	}
	for _, id := range assetLabels {
		if _, restricted := p.restrictLabels[id]; restricted {
			return false
		}
	}
	return true
}
