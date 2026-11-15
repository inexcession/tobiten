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
// The zero value is uninitialized and denies all rows.
type LabelPredicate struct {
	initialized    bool
	allowIDs       []string
	restrictIDs    []string
	includeTrashed bool
}

// NewLabelPredicate constructs a LabelPredicate from a principal, user policies, and group policies.
// Missing principals and label-required roles with zero effective ALLOW policies fail closed (deny all).
// Supplied user-policy rows whose UserID does not match principal.User.ID are ignored.
// Personal USER labels never participate in access decisions; trash is filtered unless IncludeTrashed
// is requested AND the principal holds domain.PermissionTrashPurge.
func NewLabelPredicate(
	principal *domain.Principal,
	userPolicies []domain.UserLabelPolicy,
	groupPolicies []domain.GroupLabelPolicy,
	opts LabelPredicateOptions,
) *LabelPredicate {
	if principal == nil || strings.TrimSpace(principal.User.ID) == "" {
		return &LabelPredicate{}
	}

	filteredUser := filterValidUserPolicies(principal.User.ID, userPolicies)
	filteredGroup := filterValidGroupPolicies(groupPolicies)

	effective := domain.NewEffectiveLabelPolicyForPrincipal(principal, filteredUser, filteredGroup)
	if effective.RequiresAllowLabels() && !effective.HasAllowLabels() {
		return &LabelPredicate{}
	}

	allowIDs := effective.AllowLabelIDs()
	restrictIDs := effective.RestrictLabelIDs()

	includeTrashed := opts.IncludeTrashed && principal.HasPermission(domain.PermissionTrashPurge)
	if !includeTrashed && !slices.Contains(restrictIDs, domain.LabelMarkedForDeletionID) {
		restrictIDs = append(restrictIDs, domain.LabelMarkedForDeletionID)
		slices.Sort(restrictIDs)
	}

	return &LabelPredicate{
		initialized:    true,
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
		return &LabelPredicate{}, nil
	}
	if db == nil {
		return &LabelPredicate{}, errors.New("database handle must not be nil")
	}

	rows, err := New(db).ListUserLabelPolicies(ctx, principal.User.ID)
	if err != nil {
		return &LabelPredicate{}, mapPgError(err)
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
		return &LabelPredicate{}, nil
	}
	if r == nil || r.q == nil {
		return &LabelPredicate{}, errors.New("label repository is not initialized")
	}

	policies, err := r.ListUserLabelPolicies(ctx, principal.User.ID)
	if err != nil {
		return &LabelPredicate{}, err
	}
	return NewLabelPredicate(principal, policies, groupPolicies, opts), nil
}

// IsDenyAll reports whether the predicate unconditionally denies every row.
func (p *LabelPredicate) IsDenyAll() bool {
	return p == nil || !p.initialized
}

// IncludesTrashed reports whether items tagged marked_for_deletion are permitted by this predicate.
func (p *LabelPredicate) IncludesTrashed() bool {
	return p != nil && p.initialized && p.includeTrashed
}

// AllowIDs returns a defensive copy of the effective ALLOW SERVER label IDs.
func (p *LabelPredicate) AllowIDs() []string {
	if p.IsDenyAll() || len(p.allowIDs) == 0 {
		return nil
	}
	return slices.Clone(p.allowIDs)
}

// RestrictIDs returns a defensive copy of the effective RESTRICT SERVER label IDs
// (including the protected trash label when trash filtering is active).
func (p *LabelPredicate) RestrictIDs() []string {
	if p.IsDenyAll() || len(p.restrictIDs) == 0 {
		return nil
	}
	return slices.Clone(p.restrictIDs)
}

// SeriesFragment returns the parameterized SQL WHERE condition for a series table row.
// When alias is empty, columns qualify against "series". Positional parameters begin at $startParam.
func (p *LabelPredicate) SeriesFragment(alias string, startParam int) (SQLFragment, error) {
	qualifier, err := resolveOuterQualifier(alias, "series")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	idExpr := qualifier + ".id"
	sa := makeSubqueryAliases(qualifier)
	return p.assembleFragment(startParam, func(allowParam, restrictParam int) []string {
		var clauses []string
		if restrictParam > 0 {
			clauses = append(clauses, buildItemRestrictClause(sa, domain.ItemTypeSeries, idExpr, restrictParam))
		}
		if allowParam > 0 {
			clauses = append(clauses, buildSeriesAllowClause(sa, idExpr, allowParam))
		}
		return clauses
	})
}

// VolumeFragment returns the parameterized SQL WHERE condition for a volumes table row.
// When alias is empty, columns qualify against "volumes". Positional parameters begin at $startParam.
func (p *LabelPredicate) VolumeFragment(alias string, startParam int) (SQLFragment, error) {
	qualifier, err := resolveOuterQualifier(alias, "volumes")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	sa := makeSubqueryAliases(qualifier)
	return p.volumeFragmentForIDExpr(sa, qualifier+".id", startParam)
}

// AssetFragment returns the parameterized SQL WHERE condition for a media_assets table row.
// It enforces asset-level restrictions as well as full parent-volume and series visibility rules.
// When alias is empty, columns qualify against "media_assets". Positional parameters begin at $startParam.
func (p *LabelPredicate) AssetFragment(alias string, startParam int) (SQLFragment, error) {
	qualifier, err := resolveOuterQualifier(alias, "media_assets")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	assetIDExpr := qualifier + ".id"
	volumeIDExpr := qualifier + ".volume_id"
	sa := makeSubqueryAliases(qualifier)
	return p.assembleFragment(startParam, func(allowParam, restrictParam int) []string {
		var clauses []string
		if restrictParam > 0 {
			clauses = append(clauses, buildItemRestrictClause(sa, domain.ItemTypeAsset, assetIDExpr, restrictParam))
		}
		clauses = append(clauses, buildVolumeClauses(sa, volumeIDExpr, allowParam, restrictParam)...)
		return clauses
	})
}

// ReadingProgressFragment returns the parameterized SQL WHERE condition for a reading_progress table row,
// enforcing visibility of its referenced volume. When alias is empty, columns qualify against "reading_progress".
func (p *LabelPredicate) ReadingProgressFragment(alias string, startParam int) (SQLFragment, error) {
	qualifier, err := resolveOuterQualifier(alias, "reading_progress")
	if err != nil {
		return SQLFragment{SQL: "FALSE"}, err
	}
	sa := makeSubqueryAliases(qualifier)
	return p.volumeFragmentForIDExpr(sa, qualifier+".volume_id", startParam)
}

func (p *LabelPredicate) volumeFragmentForIDExpr(sa subqueryAliases, volumeIDExpr string, startParam int) (SQLFragment, error) {
	return p.assembleFragment(startParam, func(allowParam, restrictParam int) []string {
		return buildVolumeClauses(sa, volumeIDExpr, allowParam, restrictParam)
	})
}

func (p *LabelPredicate) assembleFragment(
	startParam int,
	clausesFn func(allowParam, restrictParam int) []string,
) (SQLFragment, error) {
	if p.IsDenyAll() {
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

type subqueryAliases struct {
	il string
	l  string
	sm string
}

func makeSubqueryAliases(outerQualifier string) subqueryAliases {
	return subqueryAliases{
		il: pickSafeAlias("_lp_il", outerQualifier),
		l:  pickSafeAlias("_lp_l", outerQualifier),
		sm: pickSafeAlias("_lp_sm", outerQualifier),
	}
}

func pickSafeAlias(base, outerQualifier string) string {
	candidate := base
	for strings.EqualFold(candidate, outerQualifier) {
		candidate += "_"
	}
	return candidate
}

func buildVolumeClauses(sa subqueryAliases, volumeIDExpr string, allowParam, restrictParam int) []string {
	var clauses []string
	if restrictParam > 0 {
		clauses = append(clauses,
			buildItemRestrictClause(sa, domain.ItemTypeVolume, volumeIDExpr, restrictParam),
			buildSeriesMembershipRestrictClause(sa, volumeIDExpr, restrictParam),
		)
	}
	if allowParam > 0 {
		clauses = append(clauses, buildVolumeAllowClause(sa, volumeIDExpr, allowParam))
	}
	return clauses
}

func buildItemRestrictClause(sa subqueryAliases, itemType domain.ItemType, itemIDExpr string, restrictParam int) string {
	return fmt.Sprintf(
		"NOT EXISTS (SELECT 1 FROM item_labels %s JOIN labels %s ON %s.id = %s.label_id WHERE %s.item_type = '%s' AND %s.item_id = %s AND %s.scope = 'SERVER' AND %s.label_id = ANY($%d::text[]))",
		sa.il, sa.l, sa.l, sa.il, sa.il, itemType, sa.il, itemIDExpr, sa.l, sa.il, restrictParam,
	)
}

func buildSeriesMembershipRestrictClause(sa subqueryAliases, volumeIDExpr string, restrictParam int) string {
	return fmt.Sprintf(
		"NOT EXISTS (SELECT 1 FROM series_memberships %s JOIN item_labels %s ON %s.item_type = 'SERIES' AND %s.item_id = %s.series_id JOIN labels %s ON %s.id = %s.label_id WHERE %s.volume_id = %s AND %s.scope = 'SERVER' AND %s.label_id = ANY($%d::text[]))",
		sa.sm, sa.il, sa.il, sa.il, sa.sm, sa.l, sa.l, sa.il, sa.sm, volumeIDExpr, sa.l, sa.il, restrictParam,
	)
}

func buildSeriesAllowClause(sa subqueryAliases, seriesIDExpr string, allowParam int) string {
	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM item_labels %s JOIN labels %s ON %s.id = %s.label_id WHERE %s.item_type = 'SERIES' AND %s.item_id = %s AND %s.scope = 'SERVER' AND %s.label_id = ANY($%d::text[]))",
		sa.il, sa.l, sa.l, sa.il, sa.il, sa.il, seriesIDExpr, sa.l, sa.il, allowParam,
	)
}

func buildVolumeAllowClause(sa subqueryAliases, volumeIDExpr string, allowParam int) string {
	return fmt.Sprintf(
		"((EXISTS (SELECT 1 FROM series_memberships %s WHERE %s.volume_id = %s) AND EXISTS (SELECT 1 FROM series_memberships %s JOIN item_labels %s ON %s.item_type = 'SERIES' AND %s.item_id = %s.series_id JOIN labels %s ON %s.id = %s.label_id WHERE %s.volume_id = %s AND %s.scope = 'SERVER' AND %s.label_id = ANY($%d::text[]))) OR (NOT EXISTS (SELECT 1 FROM series_memberships %s WHERE %s.volume_id = %s) AND EXISTS (SELECT 1 FROM item_labels %s JOIN labels %s ON %s.id = %s.label_id WHERE %s.item_type = 'VOLUME' AND %s.item_id = %s AND %s.scope = 'SERVER' AND %s.label_id = ANY($%d::text[]))))",
		sa.sm, sa.sm, volumeIDExpr,
		sa.sm, sa.il, sa.il, sa.il, sa.sm, sa.l, sa.l, sa.il, sa.sm, volumeIDExpr, sa.l, sa.il, allowParam,
		sa.sm, sa.sm, volumeIDExpr,
		sa.il, sa.l, sa.l, sa.il, sa.il, sa.il, volumeIDExpr, sa.l, sa.il, allowParam,
	)
}

func filterValidUserPolicies(principalUserID string, in []domain.UserLabelPolicy) []domain.UserLabelPolicy {
	if len(in) == 0 || principalUserID == "" {
		return nil
	}
	out := make([]domain.UserLabelPolicy, 0, len(in))
	for _, p := range in {
		if p.UserID != principalUserID {
			continue
		}
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

func resolveOuterQualifier(alias, defaultTable string) (string, error) {
	if alias == "" {
		return defaultTable, nil
	}
	if !isValidSQLIdentifier(alias) {
		return "", fmt.Errorf("invalid SQL alias %q", alias)
	}
	return alias, nil
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
