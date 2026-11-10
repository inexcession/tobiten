package domain_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestLabelValidation(t *testing.T) {
	ownerID := "019318a0-0000-7000-8000-000000000010"

	tests := []struct {
		name    string
		label   domain.Label
		wantErr error
	}{
		{
			name: "valid server label",
			label: domain.Label{
				ID:      "lbl_1",
				Scope:   domain.LabelScopeServer,
				Name:    domain.LabelMarkedForDeletion,
				BuiltIn: true,
			},
			wantErr: nil,
		},
		{
			name: "server label with owner ID is rejected",
			label: domain.Label{
				ID:      "lbl_2",
				Scope:   domain.LabelScopeServer,
				OwnerID: &ownerID,
				Name:    "nsfw",
			},
			wantErr: domain.ErrServerLabelWithOwner,
		},
		{
			name: "valid user label with owner ID",
			label: domain.Label{
				ID:      "lbl_3",
				Scope:   domain.LabelScopeUser,
				OwnerID: &ownerID,
				Name:    domain.LabelWantToRead,
				BuiltIn: true,
			},
			wantErr: nil,
		},
		{
			name: "user label without owner ID is rejected",
			label: domain.Label{
				ID:    "lbl_4",
				Scope: domain.LabelScopeUser,
				Name:  domain.LabelFavorite,
			},
			wantErr: domain.ErrUserLabelWithoutOwner,
		},
		{
			name: "label with empty name is rejected",
			label: domain.Label{
				ID:    "lbl_5",
				Scope: domain.LabelScopeServer,
				Name:  "",
			},
			wantErr: domain.ErrEmptyLabelName,
		},
		{
			name: "label with invalid scope is rejected",
			label: domain.Label{
				ID:    "lbl_6",
				Scope: domain.LabelScope("CUSTOM"),
				Name:  "custom",
			},
			wantErr: domain.ErrInvalidLabelScope,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.label.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("label.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestItemLabelValidation(t *testing.T) {
	tests := []struct {
		name    string
		il      domain.ItemLabel
		wantErr bool
	}{
		{
			name: "valid series item label",
			il: domain.ItemLabel{
				LabelID:  "lbl_1",
				ItemType: domain.ItemTypeSeries,
				ItemID:   "series_1",
			},
			wantErr: false,
		},
		{
			name: "valid volume item label",
			il: domain.ItemLabel{
				LabelID:  "lbl_1",
				ItemType: domain.ItemTypeVolume,
				ItemID:   "volume_1",
			},
			wantErr: false,
		},
		{
			name: "valid asset item label",
			il: domain.ItemLabel{
				LabelID:  "lbl_1",
				ItemType: domain.ItemTypeAsset,
				ItemID:   "asset_1",
			},
			wantErr: false,
		},
		{
			name: "invalid item type",
			il: domain.ItemLabel{
				LabelID:  "lbl_1",
				ItemType: domain.ItemType("UNKNOWN"),
				ItemID:   "item_1",
			},
			wantErr: true,
		},
		{
			name: "empty label ID",
			il: domain.ItemLabel{
				LabelID:  "",
				ItemType: domain.ItemTypeSeries,
				ItemID:   "series_1",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.il.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ItemLabel.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServerLabelIDs(t *testing.T) {
	ownerID := "user_1"
	labels := []domain.Label{
		{ID: "lbl_server_1", Scope: domain.LabelScopeServer, Name: "18+"},
		{ID: "lbl_user_1", Scope: domain.LabelScopeUser, OwnerID: &ownerID, Name: "favorite"},
		{ID: "lbl_server_2", Scope: domain.LabelScopeServer, Name: "marked_for_deletion"},
		{ID: "lbl_user_2", Scope: domain.LabelScopeUser, OwnerID: &ownerID, Name: "want-to-read"},
	}

	serverIDs := domain.ServerLabelIDs(labels)
	expected := []string{"lbl_server_1", "lbl_server_2"}
	if !slices.Equal(serverIDs, expected) {
		t.Errorf("ServerLabelIDs() = %v, want %v", serverIDs, expected)
	}
}

func TestEffectiveLabelPolicyMerging(t *testing.T) {
	t.Run("restrict wins over allow when user and group conflict (M0 S-13)", func(t *testing.T) {
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_shared_1", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_user_only", Effect: domain.PolicyEffectAllow},
		}
		groupPolicies := []domain.GroupLabelPolicy{
			{GroupClaim: "kids", LabelID: "lbl_shared_1", Effect: domain.PolicyEffectRestrict},
			{GroupClaim: "kids", LabelID: "lbl_group_only", Effect: domain.PolicyEffectRestrict},
		}

		policy := domain.NewEffectiveLabelPolicy(userPolicies, groupPolicies, false)

		// lbl_shared_1 had user ALLOW and group RESTRICT -> RESTRICT wins
		if !policy.IsLabelRestricted("lbl_shared_1") {
			t.Errorf("expected lbl_shared_1 to be restricted because RESTRICT wins")
		}
		if policy.IsLabelAllowed("lbl_shared_1") {
			t.Errorf("lbl_shared_1 should not be allowed")
		}

		// lbl_user_only remains ALLOW
		if !policy.IsLabelAllowed("lbl_user_only") {
			t.Errorf("expected lbl_user_only to be allowed")
		}

		// lbl_group_only remains RESTRICT
		if !policy.IsLabelRestricted("lbl_group_only") {
			t.Errorf("expected lbl_group_only to be restricted")
		}
	})

	t.Run("restrict wins when user RESTRICT and group ALLOW", func(t *testing.T) {
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_shared_2", Effect: domain.PolicyEffectRestrict},
		}
		groupPolicies := []domain.GroupLabelPolicy{
			{GroupClaim: "general", LabelID: "lbl_shared_2", Effect: domain.PolicyEffectAllow},
		}

		policy := domain.NewEffectiveLabelPolicy(userPolicies, groupPolicies, false)
		if !policy.IsLabelRestricted("lbl_shared_2") {
			t.Errorf("expected lbl_shared_2 to be restricted")
		}
		if policy.IsLabelAllowed("lbl_shared_2") {
			t.Errorf("lbl_shared_2 should not be allowed")
		}
	})
}

func TestEffectiveLabelPolicyEvaluationRules(t *testing.T) {
	t.Run("no policies configured: all content is allowed", func(t *testing.T) {
		policy := domain.NewEffectiveLabelPolicy(nil, nil, false)

		if !policy.EvaluateLabels(nil) {
			t.Errorf("content with no labels should be allowed")
		}
		if !policy.EvaluateLabels([]string{"lbl_any"}) {
			t.Errorf("content with arbitrary label should be allowed")
		}
	})

	t.Run("blacklist mode (only RESTRICT policies)", func(t *testing.T) {
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_nsfw", Effect: domain.PolicyEffectRestrict},
			{UserID: "u1", LabelID: "lbl_trash", Effect: domain.PolicyEffectRestrict},
		}
		policy := domain.NewEffectiveLabelPolicy(userPolicies, nil, false)

		if policy.HasAllowLabels() {
			t.Errorf("expected HasAllowLabels=false")
		}

		// Content with no labels: allowed
		if !policy.EvaluateLabels(nil) {
			t.Errorf("unlabeled content should be allowed in blacklist mode")
		}
		// Content with clean label: allowed
		if !policy.EvaluateLabels([]string{"lbl_clean"}) {
			t.Errorf("content with non-restricted label should be allowed")
		}
		// Content with restricted label: denied
		if policy.EvaluateLabels([]string{"lbl_nsfw"}) {
			t.Errorf("content with restricted label must be denied")
		}
		// Content with both clean and restricted label: denied (RESTRICT wins)
		if policy.EvaluateLabels([]string{"lbl_clean", "lbl_nsfw"}) {
			t.Errorf("content with restricted label must be denied even if it has clean labels")
		}
	})

	t.Run("whitelist mode (only ALLOW policies)", func(t *testing.T) {
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_kids", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_shonen", Effect: domain.PolicyEffectAllow},
		}
		policy := domain.NewEffectiveLabelPolicy(userPolicies, nil, false)

		if !policy.HasAllowLabels() {
			t.Errorf("expected HasAllowLabels=true")
		}

		// Content with no labels: denied (must match whitelist)
		if policy.EvaluateLabels(nil) {
			t.Errorf("unlabeled content must be denied when ALLOW policies exist")
		}
		// Content with non-matching label: denied
		if policy.EvaluateLabels([]string{"lbl_seinen"}) {
			t.Errorf("content with unlisted label must be denied in whitelist mode")
		}
		// Content with matching label: allowed
		if !policy.EvaluateLabels([]string{"lbl_kids"}) {
			t.Errorf("content with matching allow label must be allowed")
		}
		// Content with matching label and unlisted label: allowed
		if !policy.EvaluateLabels([]string{"lbl_kids", "lbl_comedy"}) {
			t.Errorf("content with matching allow label must be allowed")
		}
	})

	t.Run("combined whitelist and blacklist mode (ALLOW and RESTRICT)", func(t *testing.T) {
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_general", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_teen", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_explicit", Effect: domain.PolicyEffectRestrict},
		}
		policy := domain.NewEffectiveLabelPolicy(userPolicies, nil, false)

		// Unlabeled: denied (because ALLOW exists)
		if policy.EvaluateLabels(nil) {
			t.Errorf("unlabeled content must be denied when ALLOW policies exist")
		}
		// Allowed label: allowed
		if !policy.EvaluateLabels([]string{"lbl_teen"}) {
			t.Errorf("content with allowed label should be permitted")
		}
		// Restricted label: denied
		if policy.EvaluateLabels([]string{"lbl_explicit"}) {
			t.Errorf("content with restricted label must be denied")
		}
		// Allowed label AND restricted label: denied (RESTRICT always wins over ALLOW)
		if policy.EvaluateLabels([]string{"lbl_teen", "lbl_explicit"}) {
			t.Errorf("content with both ALLOW and RESTRICT label must be denied (RESTRICT wins)")
		}
		// Unrelated label: denied
		if policy.EvaluateLabels([]string{"lbl_other"}) {
			t.Errorf("content with unlisted label must be denied")
		}
	})
}

func TestTopDownAllowEvaluation(t *testing.T) {
	// Setup policy with ALLOW on "lbl_allowed_series", "lbl_allowed_vol", "lbl_allowed_asset"
	// and RESTRICT on "lbl_restricted"
	userPolicies := []domain.UserLabelPolicy{
		{UserID: "u1", LabelID: "lbl_allowed_series", Effect: domain.PolicyEffectAllow},
		{UserID: "u1", LabelID: "lbl_allowed_vol", Effect: domain.PolicyEffectAllow},
		{UserID: "u1", LabelID: "lbl_allowed_asset", Effect: domain.PolicyEffectAllow},
		{UserID: "u1", LabelID: "lbl_restricted", Effect: domain.PolicyEffectRestrict},
	}
	policy := domain.NewEffectiveLabelPolicy(userPolicies, nil, false)

	t.Run("allowed volume in a non-allowed series is hidden (I-4)", func(t *testing.T) {
		// Volume carries lbl_allowed_vol, but belongs to a series with lbl_other_series.
		// The volume's own ALLOW labels do not count when it belongs to a series.
		nonAllowedSeries := [][]string{{"lbl_other_series"}}
		if policy.EvaluateVolume([]string{"lbl_allowed_vol"}, nonAllowedSeries) {
			t.Errorf("allowed volume in a non-allowed series must be hidden (I-4)")
		}
	})

	t.Run("allowed asset in a non-allowed volume is hidden (I-4)", func(t *testing.T) {
		// Asset carries lbl_allowed_asset, but its parent volume belongs to a non-allowed series.
		// Since the volume is hidden, the asset must also be hidden (asset labels only restrict, never grant).
		nonAllowedSeries := [][]string{{"lbl_other_series"}}
		if policy.EvaluateAsset([]string{"lbl_allowed_asset"}, []string{"lbl_allowed_vol"}, nonAllowedSeries) {
			t.Errorf("allowed asset in a non-allowed volume must be hidden (I-4)")
		}
	})

	t.Run("standalone volume with an allowed label is visible (I-4)", func(t *testing.T) {
		// Standalone volume has no series memberships (nil/empty seriesLabelsList)
		// It uses its own labels.
		if !policy.EvaluateVolume([]string{"lbl_allowed_vol"}, nil) {
			t.Errorf("standalone volume with an allowed label must be visible (I-4)")
		}
		if !policy.EvaluateVolume([]string{"lbl_allowed_vol"}, [][]string{}) {
			t.Errorf("standalone volume with an allowed label must be visible (I-4)")
		}
		// Standalone volume without an allowed label is hidden
		if policy.EvaluateVolume([]string{"lbl_other_vol"}, nil) {
			t.Errorf("standalone volume without an allowed label must be hidden")
		}
	})

	t.Run("volume with series membership where series has allowed label is visible", func(t *testing.T) {
		// Volume itself has no labels (or unrelated label), but series has lbl_allowed_series
		allowedSeries := [][]string{{"lbl_allowed_series"}}
		if !policy.EvaluateVolume(nil, allowedSeries) {
			t.Errorf("volume belonging to an allowed series must be visible")
		}
		if !policy.EvaluateVolume([]string{"lbl_other"}, allowedSeries) {
			t.Errorf("volume belonging to an allowed series must be visible regardless of volume labels")
		}
	})

	t.Run("RESTRICT on the volume or any series still hides it", func(t *testing.T) {
		allowedSeries := [][]string{{"lbl_allowed_series"}}

		// Volume itself has RESTRICT label
		if policy.EvaluateVolume([]string{"lbl_restricted"}, allowedSeries) {
			t.Errorf("volume with restricted label must be hidden even if series is allowed")
		}

		// A parent series has RESTRICT label
		restrictedSeries := [][]string{
			{"lbl_allowed_series"},
			{"lbl_restricted"},
		}
		if policy.EvaluateVolume(nil, restrictedSeries) {
			t.Errorf("volume must be hidden if any parent series has a restricted label")
		}

		// Asset has RESTRICT label
		if policy.EvaluateAsset([]string{"lbl_restricted"}, nil, allowedSeries) {
			t.Errorf("asset with restricted label must be hidden even if volume is allowed")
		}
	})
}

func TestFailClosedRestrictedReader(t *testing.T) {
	restrictedRole, ok := domain.BuiltInRoleByName(domain.RoleRestrictedReaderName)
	if !ok {
		t.Fatalf("Restricted reader role not found")
	}
	if !restrictedRole.RequiresAllowLabels {
		t.Fatalf("Restricted reader must have RequiresAllowLabels=true")
	}

	readerRole, ok := domain.BuiltInRoleByName(domain.RoleReaderName)
	if !ok {
		t.Fatalf("Reader role not found")
	}

	user := domain.User{ID: "child_1", Username: "timmy"}
	childPrincipal := domain.NewPrincipal(user, []domain.Role{restrictedRole}, domain.CredentialKindPassword, "cred_child", nil)
	regularPrincipal := domain.NewPrincipal(user, []domain.Role{readerRole}, domain.CredentialKindPassword, "cred_reg", nil)

	t.Run("Restricted reader with zero policy rows sees nothing (I-5)", func(t *testing.T) {
		policy := domain.NewEffectiveLabelPolicyForPrincipal(childPrincipal, nil, nil)

		if !policy.RequiresAllowLabels() {
			t.Errorf("expected policy to have RequiresAllowLabels=true")
		}

		// Everything denied
		if policy.EvaluateLabels(nil) {
			t.Errorf("unlabeled content must be hidden for restricted reader with 0 policy rows")
		}
		if policy.EvaluateLabels([]string{"lbl_any"}) {
			t.Errorf("labeled content must be hidden for restricted reader with 0 policy rows")
		}
		if policy.EvaluateSeries([]string{"lbl_any"}) {
			t.Errorf("series must be hidden for restricted reader with 0 policy rows")
		}
		if policy.EvaluateVolume([]string{"lbl_any"}, nil) {
			t.Errorf("standalone volume must be hidden for restricted reader with 0 policy rows")
		}
		if policy.EvaluateVolume(nil, [][]string{{"lbl_any"}}) {
			t.Errorf("volume with series must be hidden for restricted reader with 0 policy rows")
		}
		if policy.EvaluateAsset(nil, nil, [][]string{{"lbl_any"}}) {
			t.Errorf("asset must be hidden for restricted reader with 0 policy rows")
		}
	})

	t.Run("regular Reader with zero policy rows sees all non-restricted content", func(t *testing.T) {
		policy := domain.NewEffectiveLabelPolicyForPrincipal(regularPrincipal, nil, nil)

		if policy.RequiresAllowLabels() {
			t.Errorf("regular reader policy must have RequiresAllowLabels=false")
		}
		if !policy.EvaluateLabels(nil) {
			t.Errorf("regular reader with 0 policy rows should see unlabeled content")
		}
		if !policy.EvaluateSeries([]string{"lbl_clean"}) {
			t.Errorf("regular reader with 0 policy rows should see clean series")
		}
	})

	t.Run("Restricted reader with one ALLOW row sees only that (I-5)", func(t *testing.T) {
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "child_1", LabelID: "lbl_kids", Effect: domain.PolicyEffectAllow},
		}
		policy := domain.NewEffectiveLabelPolicyForPrincipal(childPrincipal, userPolicies, nil)

		// Series with allowed label: visible
		if !policy.EvaluateSeries([]string{"lbl_kids"}) {
			t.Errorf("series with lbl_kids should be visible")
		}
		// Series without allowed label: hidden
		if policy.EvaluateSeries([]string{"lbl_other"}) {
			t.Errorf("series with lbl_other should be hidden")
		}
		// Volume in allowed series: visible
		if !policy.EvaluateVolume(nil, [][]string{{"lbl_kids"}}) {
			t.Errorf("volume in lbl_kids series should be visible")
		}
		// Volume in non-allowed series: hidden
		if policy.EvaluateVolume(nil, [][]string{{"lbl_other"}}) {
			t.Errorf("volume in non-allowed series should be hidden")
		}
		// Standalone volume with allowed label: visible
		if !policy.EvaluateVolume([]string{"lbl_kids"}, nil) {
			t.Errorf("standalone volume with lbl_kids should be visible")
		}
		// Standalone volume without allowed label: hidden
		if policy.EvaluateVolume([]string{"lbl_other"}, nil) {
			t.Errorf("standalone volume without lbl_kids should be hidden")
		}
		// Asset in allowed series: visible
		if !policy.EvaluateAsset(nil, nil, [][]string{{"lbl_kids"}}) {
			t.Errorf("asset in allowed series should be visible")
		}
		// Asset in non-allowed series: hidden
		if policy.EvaluateAsset(nil, nil, [][]string{{"lbl_other"}}) {
			t.Errorf("asset in non-allowed series should be hidden")
		}
	})
}

func TestPolicyValidation(t *testing.T) {
	t.Run("UserLabelPolicy.Validate", func(t *testing.T) {
		p := domain.UserLabelPolicy{}
		if err := p.Validate(); !errors.Is(err, domain.ErrEmptyUserID) {
			t.Errorf("expected ErrEmptyUserID, got %v", err)
		}
		p.UserID = "u1"
		if err := p.Validate(); err == nil {
			t.Errorf("expected error on empty label ID")
		}
		p.LabelID = "lbl1"
		p.Effect = "UNKNOWN"
		if err := p.Validate(); !errors.Is(err, domain.ErrInvalidPolicyEffect) {
			t.Errorf("expected ErrInvalidPolicyEffect, got %v", err)
		}
		p.Effect = domain.PolicyEffectAllow
		if err := p.Validate(); err != nil {
			t.Errorf("expected valid policy, got %v", err)
		}
	})

	t.Run("GroupLabelPolicy.Validate", func(t *testing.T) {
		p := domain.GroupLabelPolicy{}
		if err := p.Validate(); err == nil {
			t.Errorf("expected error on empty group claim")
		}
		p.GroupClaim = "group1"
		if err := p.Validate(); err == nil {
			t.Errorf("expected error on empty label ID")
		}
		p.LabelID = "lbl1"
		p.Effect = "INVALID"
		if err := p.Validate(); !errors.Is(err, domain.ErrInvalidPolicyEffect) {
			t.Errorf("expected ErrInvalidPolicyEffect, got %v", err)
		}
		p.Effect = domain.PolicyEffectRestrict
		if err := p.Validate(); err != nil {
			t.Errorf("expected valid policy, got %v", err)
		}
	})
}
