package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/inexcession/tobiten/internal/domain"
)

// LabelPredicateOptions controls optional visibility behavior when building a label predicate.
type LabelPredicateOptions struct {
	// IncludeTrashed permits viewing items marked with marked_for_deletion ONLY when
	// the principal holds domain.PermissionTrashPurge. Principals lacking that permission
	// always have trashed items filtered out even when IncludeTrashed is true.
	IncludeTrashed bool
}

// SQLFragment represents a parameterized PostgreSQL WHERE condition and its positional arguments.
type SQLFragment struct {
	SQL  string
	Args []any
}

// LabelPredicate encapsulates a principal's resolved SERVER-label access policy and trash filter,
// producing parameterized PostgreSQL WHERE fragments for series, volumes, assets, and reading progress.
type LabelPredicate struct {
	denyAll        bool
	allowIDs       []string
	restrictIDs    []string
	includeTrashed bool
}

// NewLabelPredicate constructs a LabelPredicate from a principal, user policies, and group policies.
// Missing principals and label-required roles with zero effective ALLOW policies fail closed (deny all).
// Personal USER labels never participate in access decisions; trash is filtered unless IncludeTrashed
// is requested AND the principal holds domain.PermissionTrashPurge.
func NewLabelPredicate(
	principal *domain.Principal,
	userPolicies []domain.UserLabelPolicy,
	groupPolicies []domain.GroupLabelPolicy,
	opts LabelPredicateOptions,
) *LabelPredicate {
	if principal == nil || strings.TrimSpace(principal.User.ID) == "" {
		return &LabelPredicate{denyAll: true}
	}

	filteredUser := filterValidUserPolicies(userPolicies)
	filteredGroup := filterValidGroupPolicies(groupPolicies)

	effective := domain.NewEffectiveLabelPolicyForPrincipal(principal, filteredUser, filteredGroup)
	if effective.RequiresAllowLabels() && !effective.HasAllowLabels() {
		return &LabelPredicate{denyAll: true}
	}

	allowIDs := effective.AllowLabelIDs()
	restrictIDs := effective.RestrictLabelIDs()

	includeTrashed := opts.IncludeTrashed && principal.HasPermission(domain.PermissionTrashPurge)
	if !includeTrashed && !slices.Contains(restrictIDs, domain.LabelMarkedForDeletionID) {
		restrictIDs = append(restrictIDs, domain.LabelMarkedForDeletionID)
		slices.Sort(restrictIDs)
	}

	return &LabelPredicate{
		denyAll:        false,
		allowIDs:       allowIDs,
		restrictIDs:    restrictIDs,
		includeTrashed: includeTrashed,
	}
}

// LoadLabelPredicate loads the principal's user_label_policies from PostgreSQL once and constructs
// a LabelPredicate combining them with any supplied group policies and options.
func LoadLabelPredicate(
	ctx context.Context,
	db DBTX,
	principal *domain.Principal,
	groupPolicies []domain.GroupLabelPolicy,
	opts LabelPredicateOptions,
) (*LabelPredicate, error) {
	if principal == nil || strings.TrimSpace(principal.User.ID) == "" {
		return &LabelPredicate{denyAll: true}, nil
	}
	if db == nil {
		return &LabelPredicate{denyAll: true}, errors.New("database handle must not be nil")
	}

	rows, err := New(db).ListUserLabelPolicies(ctx, principal.User.ID)
	if err != nil {
		return &LabelPredicate{denyAll: true}, mapPgError(err)
	}
	return NewLabelPredicate(principal, toDomainPolicies(rows), groupPolicies, opts), nil
}

// LabelPredicate loads the principal's user_label_policies from PostgreSQL once and constructs
// a LabelPredicate combining them with any supplied group policies and options.
func (r *LabelRepository) LabelPredicate(
	ctx context.Context,
	principal *domain.Principal,
	groupPolicies []domain.GroupLabelPolicy,
	opts LabelPredicateOptions,
) (*LabelPredicate, error) {
	if principal == nil || strings.TrimSpace(principal.User.ID) == "" {
		return &LabelPredicate{denyAll: true}, nil
	}
	if r == nil || r.q == nil {
		return &LabelPredicate{denyAll: true}, errors.New("label repository is not initialized")
	}

	policies, err := r.ListUserLabelPolicies(ctx, principal.User.ID)
	if err != nil {
		return &LabelPredicate{denyAll: true}, err
	}
	return NewLabelPredicate(principal, policies, groupPolicies, opts), nil
}

// IsDenyAll reports whether the predicate unconditionally denies every row.
func (p *LabelPredicate) IsDenyAll() bool {
	return p == nil || p.denyAll
}

// IncludesTrashed reports whether items tagged marked_for_deletion are permitted by this predicate.
func (p *LabelPredicate) IncludesTrashed() bool {
	return p != nil && !p.denyAll && p.includeTrashed
}

// AllowIDs returns a defensive copy of the effective ALLOW SERVER label IDs.
func (p *LabelPredicate) AllowIDs() []string {
	if p == nil || p.denyAll || len(p.allowIDs) == 0 {
		return nil
	}
	return slices.Clone(p.allowIDs)
}

// RestrictIDs returns a defensive copy of the effective RESTRICT SERVER label IDs
// (including the protected trash label when trash filtering is active).
func (p *LabelPredicate) RestrictIDs() []string {
	if p == nil || p.denyAll || len(p.restrictIDs) == 0 {
		return nil
	}
	return slices.Clone(p.restrictIDs)
}

// SeriesFragment returns the parameterized SQL WHERE condition for a series table row.
// When alias is empty, columns qualify against "series". Positional parameters begin at $startParam.
func (p *LabelPredicate) SeriesFragment(alias string, startParam int) (SQLFragment, error) {
	idExpr, err := qualifyIdentifier(alias, "series", "id")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	return p.assembleFragment(startParam, func(allowParam, restrictParam int) []string {
		var clauses []string
		if restrictParam > 0 {
			clauses = append(clauses, buildItemRestrictClause(domain.ItemTypeSeries, idExpr, restrictParam))
		}
		if allowParam > 0 {
			clauses = append(clauses, buildSeriesAllowClause(idExpr, allowParam))
		}
		return clauses
	})
}

// VolumeFragment returns the parameterized SQL WHERE condition for a volumes table row.
// When alias is empty, columns qualify against "volumes". Positional parameters begin at $startParam.
func (p *LabelPredicate) VolumeFragment(alias string, startParam int) (SQLFragment, error) {
	idExpr, err := qualifyIdentifier(alias, "volumes", "id")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	return p.volumeFragmentForIDExpr(idExpr, startParam)
}

// AssetFragment returns the parameterized SQL WHERE condition for a media_assets table row.
// It enforces asset-level restrictions as well as full parent-volume and series visibility rules.
// When alias is empty, columns qualify against "media_assets". Positional parameters begin at $startParam.
func (p *LabelPredicate) AssetFragment(alias string, startParam int) (SQLFragment, error) {
	assetIDExpr, err := qualifyIdentifier(alias, "media_assets", "id")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	volumeIDExpr, err := qualifyIdentifier(alias, "media_assets", "volume_id")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	return p.assembleFragment(startParam, func(allowParam, restrictParam int) []string {
		var clauses []string
		if restrictParam > 0 {
			clauses = append(clauses, buildItemRestrictClause(domain.ItemTypeAsset, assetIDExpr, restrictParam))
		}
		clauses = append(clauses, buildVolumeClauses(volumeIDExpr, allowParam, restrictParam)...)
		return clauses
	})
}

// ReadingProgressFragment returns the parameterized SQL WHERE condition for a reading_progress table row,
// enforcing visibility of its referenced volume. When alias is empty, columns qualify against "reading_progress".
func (p *LabelPredicate) ReadingProgressFragment(alias string, startParam int) (SQLFragment, error) {
	volumeIDExpr, err := qualifyIdentifier(alias, "reading_progress", "volume_id")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	return p.volumeFragmentForIDExpr(volumeIDExpr, startParam)
}

func (p *LabelPredicate) volumeFragmentForIDExpr(volumeIDExpr string, startParam int) (SQLFragment, error) {
	return p.assembleFragment(startParam, func(allowParam, restrictParam int) []string {
		return buildVolumeClauses(volumeIDExpr, allowParam, restrictParam)
	})
}

func (p *LabelPredicate) assembleFragment(
	startParam int,
	clausesFn func(allowParam, restrictParam int) []string,
) (SQLFragment, error) {
	if p == nil || p.denyAll {
		return SQLFragment{SQL: "FALSE"}, nil
	}
	if startParam < 1 {
		return SQLFragment{SQL: "FALSE"}, fmt.Errorf("startParam must be >= 1, got %d", startParam)
	}

	nextParam := startParam
	var args []any

	allowParam := 0
	if len(p.allowIDs) > 0 {
		allowParam = nextParam
		nextParam++
		args = append(args, slices.Clone(p.allowIDs))
	}

	restrictParam := 0
	if len(p.restrictIDs) > 0 {
		restrictParam = nextParam
		args = append(args, slices.Clone(p.restrictIDs))
	}

	if allowParam == 0 && restrictParam == 0 {
		return SQLFragment{SQL: "TRUE"}, nil
	}

	clauses := clausesFn(allowParam, restrictParam)
	if len(clauses) == 0 {
		return SQLFragment{SQL: "TRUE"}, nil
	}
	return SQLFragment{
		SQL:  "(" + strings.Join(clauses, " AND ") + ")",
		Args: args,
	}, nil
}

func buildVolumeClauses(volumeIDExpr string, allowParam, restrictParam int) []string {
	var clauses []string
	if restrictParam > 0 {
		clauses = append(clauses,
			buildItemRestrictClause(domain.ItemTypeVolume, volumeIDExpr, restrictParam),
			buildSeriesMembershipRestrictClause(volumeIDExpr, restrictParam),
		)
	}
	if allowParam > 0 {
		clauses = append(clauses, buildVolumeAllowClause(volumeIDExpr, allowParam))
	}
	return clauses
}

func buildItemRestrictClause(itemType domain.ItemType, itemIDExpr string, restrictParam int) string {
	return fmt.Sprintf(
		"NOT EXISTS (SELECT 1 FROM item_labels il JOIN labels l ON l.id = il.label_id WHERE il.item_type = '%s' AND il.item_id = %s AND l.scope = 'SERVER' AND il.label_id = ANY($%d::text[]))",
		itemType, itemIDExpr, restrictParam,
	)
}

func buildSeriesMembershipRestrictClause(volumeIDExpr string, restrictParam int) string {
	return fmt.Sprintf(
		"NOT EXISTS (SELECT 1 FROM series_memberships sm JOIN item_labels il ON il.item_type = 'SERIES' AND il.item_id = sm.series_id JOIN labels l ON l.id = il.label_id WHERE sm.volume_id = %s AND l.scope = 'SERVER' AND il.label_id = ANY($%d::text[]))",
		volumeIDExpr, restrictParam,
	)
}

func buildSeriesAllowClause(seriesIDExpr string, allowParam int) string {
	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM item_labels il JOIN labels l ON l.id = il.label_id WHERE il.item_type = 'SERIES' AND il.item_id = %s AND l.scope = 'SERVER' AND il.label_id = ANY($%d::text[]))",
		seriesIDExpr, allowParam,
	)
}

func buildVolumeAllowClause(volumeIDExpr string, allowParam int) string {
	return fmt.Sprintf(
		"((EXISTS (SELECT 1 FROM series_memberships sm WHERE sm.volume_id = %s) AND EXISTS (SELECT 1 FROM series_memberships sm JOIN item_labels il ON il.item_type = 'SERIES' AND il.item_id = sm.series_id JOIN labels l ON l.id = il.label_id WHERE sm.volume_id = %s AND l.scope = 'SERVER' AND il.label_id = ANY($%d::text[]))) OR (NOT EXISTS (SELECT 1 FROM series_memberships sm WHERE sm.volume_id = %s) AND EXISTS (SELECT 1 FROM item_labels il JOIN labels l ON l.id = il.label_id WHERE il.item_type = 'VOLUME' AND il.item_id = %s AND l.scope = 'SERVER' AND il.label_id = ANY($%d::text[]))))",
		volumeIDExpr, volumeIDExpr, allowParam, volumeIDExpr, volumeIDExpr, allowParam,
	)
}

func filterValidUserPolicies(in []domain.UserLabelPolicy) []domain.UserLabelPolicy {
	if len(in) == 0 {
		return nil
	}
	out := make([]domain.UserLabelPolicy, 0, len(in))
	for _, p := range in {
		if strings.TrimSpace(p.LabelID) == "" {
			continue
		}
		if p.Effect != domain.PolicyEffectAllow && p.Effect != domain.PolicyEffectRestrict {
			continue
		}
		out = append(out, p)
	}
	return out
}

func filterValidGroupPolicies(in []domain.GroupLabelPolicy) []domain.GroupLabelPolicy {
	if len(in) == 0 {
		return nil
	}
	out := make([]domain.GroupLabelPolicy, 0, len(in))
	for _, p := range in {
		if strings.TrimSpace(p.LabelID) == "" {
			continue
		}
		if p.Effect != domain.PolicyEffectAllow && p.Effect != domain.PolicyEffectRestrict {
			continue
		}
		out = append(out, p)
	}
	return out
}

func qualifyIdentifier(alias, defaultTable, column string) (string, error) {
	if alias == "" {
		return defaultTable + "." + column, nil
	}
	if !isValidSQLIdentifier(alias) {
		return "", fmt.Errorf("invalid SQL alias %q", alias)
	}
	return alias + "." + column, nil
}

func isValidSQLIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		isDigit := c >= '0' && c <= '9'
		if i == 0 && !isLetter {
			return false
		}
		if !isLetter && !isDigit {
			return false
		}
	}
	return true
}
