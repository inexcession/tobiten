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

// GroupLabelPolicy represents an OIDC group-claim label access policy row (ADR-0006 §5).
type GroupLabelPolicy struct {
	GroupClaim string       `json:"group_claim"`
	LabelID    string       `json:"label_id"`
	Effect     PolicyEffect `json:"effect"`
}

// EffectiveLabelPolicy represents the merged, effective SERVER label policy for a subject
// (ADR-0006 §5).
//
// Rules:
//   - Only SERVER-scope labels drive policy.
//   - A subject holds one effect per label.
//   - Precedence between a user's own rows and groups' rows resolves by "RESTRICT wins" (M0 S-13).
//   - No ALLOW policy rows means all content is allowed, subject to RESTRICT rows.
//   - RESTRICT always wins over ALLOW.
type EffectiveLabelPolicy struct {
	allowLabels    map[string]struct{}
	restrictLabels map[string]struct{}
}

// NewEffectiveLabelPolicy builds an EffectiveLabelPolicy from user and/or group policies,
// enforcing the "RESTRICT wins across subjects/groups" rule for conflicting policies on the same label ID.
func NewEffectiveLabelPolicy(userPolicies []UserLabelPolicy, groupPolicies []GroupLabelPolicy) *EffectiveLabelPolicy {
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
		allowLabels:    allowMap,
		restrictLabels: restrictMap,
	}
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
// under this policy (ADR-0006 §5).
//
// Evaluation rules:
//  1. RESTRICT always wins: if ANY label is in restrictLabels, access is DENIED (returns false).
//  2. If there are NO allowLabels: all content is allowed (returns true).
//  3. If there ARE allowLabels: at least one label must be in allowLabels (returns true if any match, false otherwise).
func (p *EffectiveLabelPolicy) EvaluateLabels(labelIDs []string) bool {
	// 1. Check restrictions: RESTRICT always wins
	for _, id := range labelIDs {
		if _, restricted := p.restrictLabels[id]; restricted {
			return false
		}
	}

	// 2. If no ALLOW rows exist, all non-restricted content is allowed
	if len(p.allowLabels) == 0 {
		return true
	}

	// 3. If ALLOW rows exist, at least one label must match ALLOW
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
// ADR-0006 §5 / ARCHITECTURE §7.1:
//   - "A restricted series or volume hides that bibliographic item and every file variant."
//   - "Series-label inheritance follows all memberships, so an alternate membership path cannot bypass a restriction."
//
// Therefore:
//   - If ANY parent series is restricted, the volume is restricted (returns false).
//   - If the volume itself is restricted, it is restricted (returns false).
//   - If ALLOW policies exist: either the volume itself or at least one of its series must have an ALLOW label.
func (p *EffectiveLabelPolicy) EvaluateVolume(volumeLabels []string, seriesLabelsList [][]string) bool {
	// First check: does the volume itself or any series carry a RESTRICT label?
	for _, id := range volumeLabels {
		if _, restricted := p.restrictLabels[id]; restricted {
			return false
		}
	}
	for _, sLabels := range seriesLabelsList {
		for _, id := range sLabels {
			if _, restricted := p.restrictLabels[id]; restricted {
				return false
			}
		}
	}

	// If no ALLOW policies exist, it is accessible
	if len(p.allowLabels) == 0 {
		return true
	}

	// If ALLOW policies exist, at least one label on the volume or on any parent series must be allowed
	for _, id := range volumeLabels {
		if _, allowed := p.allowLabels[id]; allowed {
			return true
		}
	}
	for _, sLabels := range seriesLabelsList {
		for _, id := range sLabels {
			if _, allowed := p.allowLabels[id]; allowed {
				return true
			}
		}
	}

	return false
}

// EvaluateAsset evaluates whether an asset is accessible.
//
// ADR-0006 §5:
//   - "A restricted series or volume hides that bibliographic item and every file variant."
//   - "A restricted asset hides only that file variant; an adapter may select another permitted manifestation of the same volume."
//
// Therefore:
//   - The parent volume must be accessible (EvaluateVolume).
//   - The asset itself must not carry any RESTRICT label.
//   - If ALLOW policies exist: since the volume is already permitted (satisfying the whitelist),
//     the asset itself is permitted unless specifically restricted.
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
