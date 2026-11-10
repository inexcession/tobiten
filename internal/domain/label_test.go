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

		policy := domain.NewEffectiveLabelPolicy(userPolicies, groupPolicies)

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

		policy := domain.NewEffectiveLabelPolicy(userPolicies, groupPolicies)
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
		policy := domain.NewEffectiveLabelPolicy(nil, nil)

		if !policy.EvaluateLabels(nil) {
			t.Errorf("content with no labels should be allowed")
		}
		if !policy.EvaluateLabels([]string{"lbl_any"}) {
			t.Errorf("content with arbitrary label should be allowed")
		}
	})

	t.Run("blacklist mode (only RESTRICT policies)", func(t *testing.T) {
		// User has restriction on "lbl_nsfw" and "lbl_trash"
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_nsfw", Effect: domain.PolicyEffectRestrict},
			{UserID: "u1", LabelID: "lbl_trash", Effect: domain.PolicyEffectRestrict},
		}
		policy := domain.NewEffectiveLabelPolicy(userPolicies, nil)

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
		// User has allow on "lbl_kids" and "lbl_shonen"
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_kids", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_shonen", Effect: domain.PolicyEffectAllow},
		}
		policy := domain.NewEffectiveLabelPolicy(userPolicies, nil)

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
		// Allowed: "lbl_general", "lbl_teen"
		// Restricted: "lbl_explicit"
		userPolicies := []domain.UserLabelPolicy{
			{UserID: "u1", LabelID: "lbl_general", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_teen", Effect: domain.PolicyEffectAllow},
			{UserID: "u1", LabelID: "lbl_explicit", Effect: domain.PolicyEffectRestrict},
		}
		policy := domain.NewEffectiveLabelPolicy(userPolicies, nil)

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

func TestHierarchicalItemEvaluation(t *testing.T) {
	// Setup policy:
	// Allow: "lbl_allowed_series", "lbl_allowed_volume"
	// Restrict: "lbl_restricted"
	userPolicies := []domain.UserLabelPolicy{
		{UserID: "u1", LabelID: "lbl_allowed_series", Effect: domain.PolicyEffectAllow},
		{UserID: "u1", LabelID: "lbl_allowed_volume", Effect: domain.PolicyEffectAllow},
		{UserID: "u1", LabelID: "lbl_restricted", Effect: domain.PolicyEffectRestrict},
	}
	policy := domain.NewEffectiveLabelPolicy(userPolicies, nil)

	t.Run("series evaluation", func(t *testing.T) {
		if !policy.EvaluateSeries([]string{"lbl_allowed_series"}) {
			t.Errorf("series with allowed label should be permitted")
		}
		if policy.EvaluateSeries([]string{"lbl_restricted"}) {
			t.Errorf("series with restricted label must be denied")
		}
		if policy.EvaluateSeries([]string{"lbl_allowed_series", "lbl_restricted"}) {
			t.Errorf("series with both allowed and restricted label must be denied")
		}
	})

	t.Run("volume evaluation with series memberships", func(t *testing.T) {
		// Standalone volume with allowed label: permitted
		if !policy.EvaluateVolume([]string{"lbl_allowed_volume"}, nil) {
			t.Errorf("standalone volume with allowed label should be permitted")
		}

		// Volume with no labels whose series has allowed label: permitted via inheritance
		seriesMemberships := [][]string{
			{"lbl_allowed_series"},
		}
		if !policy.EvaluateVolume(nil, seriesMemberships) {
			t.Errorf("volume should inherit allowed status from its series")
		}

		// Volume in multiple series where one series is restricted:
		// Alternate membership path CANNOT bypass a restriction (ADR-0006 §5)
		multiSeriesWithOneRestricted := [][]string{
			{"lbl_allowed_series"},
			{"lbl_restricted"},
		}
		if policy.EvaluateVolume(nil, multiSeriesWithOneRestricted) {
			t.Errorf("volume must be restricted if ANY parent series is restricted (alternate membership cannot bypass)")
		}

		// Volume itself is restricted, even if parent series is allowed: denied
		if policy.EvaluateVolume([]string{"lbl_restricted"}, seriesMemberships) {
			t.Errorf("volume with restricted label must be denied regardless of parent series")
		}

		// Volume with no allowed labels and series with no allowed labels: denied
		unrelatedSeries := [][]string{{"lbl_unrelated"}}
		if policy.EvaluateVolume([]string{"lbl_unrelated_vol"}, unrelatedSeries) {
			t.Errorf("volume with unlisted labels must be denied in whitelist mode")
		}
	})

	t.Run("asset evaluation", func(t *testing.T) {
		seriesMemberships := [][]string{{"lbl_allowed_series"}}

		// Asset of permitted volume with no asset labels: permitted
		if !policy.EvaluateAsset(nil, nil, seriesMemberships) {
			t.Errorf("asset of permitted volume should be accessible")
		}

		// Asset of restricted volume: denied
		deniedSeries := [][]string{{"lbl_restricted"}}
		if policy.EvaluateAsset(nil, nil, deniedSeries) {
			t.Errorf("asset of restricted volume must be inaccessible")
		}

		// Asset with its own RESTRICT label (e.g. marked_for_deletion or variant restriction):
		// denied, while the volume remains permitted for another variant (ADR-0006 §5)
		if policy.EvaluateAsset([]string{"lbl_restricted"}, nil, seriesMemberships) {
			t.Errorf("asset with restricted label must be inaccessible")
		}

		// The volume itself is still accessible for a clean variant
		if !policy.EvaluateAsset([]string{"lbl_clean_variant"}, nil, seriesMemberships) {
			t.Errorf("clean variant of permitted volume should be accessible")
		}
	})
}
