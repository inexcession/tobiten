package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupLabelRepos(t *testing.T) (*postgres.LabelRepository, *postgres.UserRepository) {
	t.Helper()
	tdb := testdb.New(t)
	if err := migrations.RunFS(context.Background(), tdb.Pool, migrations.CoreFS); err != nil {
		t.Fatalf("RunFS(CoreFS) failed: %v", err)
	}
	return postgres.NewLabelRepository(tdb.Pool), postgres.NewUserRepository(tdb.Pool)
}

func createSeededTestUser(ctx context.Context, t *testing.T, userRepo *postgres.UserRepository, username string) *domain.User {
	t.Helper()
	user := &domain.User{
		ID:       domain.MustNewID(),
		Username: username,
	}
	if err := userRepo.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser(%s): %v", username, err)
	}
	return user
}

func TestLabelRepository_BuiltInSeedsAndUserCreation(t *testing.T) {
	ctx := context.Background()
	labelRepo, userRepo := setupLabelRepos(t)

	// Server built-in marked_for_deletion must exist from migration 0005.
	serverLabels, err := labelRepo.ListServerLabels(ctx)
	if err != nil {
		t.Fatalf("ListServerLabels: %v", err)
	}
	if len(serverLabels) != 1 || serverLabels[0].ID != domain.LabelMarkedForDeletionID ||
		serverLabels[0].Name != domain.LabelMarkedForDeletion || !serverLabels[0].BuiltIn ||
		serverLabels[0].Scope != domain.LabelScopeServer || serverLabels[0].OwnerID != nil {
		t.Fatalf("unexpected initial SERVER labels: %+v", serverLabels)
	}

	// EnsureBuiltInServerLabels is idempotent.
	if err := labelRepo.EnsureBuiltInServerLabels(ctx); err != nil {
		t.Fatalf("EnsureBuiltInServerLabels: %v", err)
	}
	serverLabelsAfter, err := labelRepo.ListServerLabels(ctx)
	if err != nil {
		t.Fatalf("ListServerLabels after ensure: %v", err)
	}
	if len(serverLabelsAfter) != 1 {
		t.Fatalf("expected 1 SERVER label after idempotent ensure, got %d", len(serverLabelsAfter))
	}

	// Creating users automatically seeds owner-scoped personal built-ins: want-to-read, favorite, dnf.
	alice := createSeededTestUser(ctx, t, userRepo, "alice")
	bob := createSeededTestUser(ctx, t, userRepo, "bob")

	aliceLabels, err := labelRepo.ListUserLabels(ctx, alice.ID)
	if err != nil {
		t.Fatalf("ListUserLabels(alice): %v", err)
	}
	bobLabels, err := labelRepo.ListUserLabels(ctx, bob.ID)
	if err != nil {
		t.Fatalf("ListUserLabels(bob): %v", err)
	}

	wantNames := map[string]bool{
		domain.LabelWantToRead: true,
		domain.LabelFavorite:   true,
		domain.LabelDNF:        true,
	}
	if len(aliceLabels) != len(wantNames) || len(bobLabels) != len(wantNames) {
		t.Fatalf("expected %d personal built-ins each, got alice=%d bob=%d", len(wantNames), len(aliceLabels), len(bobLabels))
	}

	aliceIDsByName := make(map[string]string, len(aliceLabels))
	for _, lbl := range aliceLabels {
		if !wantNames[lbl.Name] || !lbl.BuiltIn || lbl.Scope != domain.LabelScopeUser || lbl.OwnerID == nil || *lbl.OwnerID != alice.ID {
			t.Fatalf("unexpected alice built-in label: %+v", lbl)
		}
		aliceIDsByName[lbl.Name] = lbl.ID
	}
	for _, lbl := range bobLabels {
		if !wantNames[lbl.Name] || !lbl.BuiltIn || lbl.Scope != domain.LabelScopeUser || lbl.OwnerID == nil || *lbl.OwnerID != bob.ID {
			t.Fatalf("unexpected bob built-in label: %+v", lbl)
		}
		if lbl.ID == aliceIDsByName[lbl.Name] {
			t.Fatalf("expected distinct per-user built-in label IDs for %q", lbl.Name)
		}
	}

	// EnsureUserBuiltInLabels preserves existing IDs and row counts.
	if err := labelRepo.EnsureUserBuiltInLabels(ctx, alice.ID); err != nil {
		t.Fatalf("EnsureUserBuiltInLabels(alice): %v", err)
	}
	aliceLabelsAfter, err := labelRepo.ListUserLabels(ctx, alice.ID)
	if err != nil {
		t.Fatalf("ListUserLabels(alice) after ensure: %v", err)
	}
	if len(aliceLabelsAfter) != len(wantNames) {
		t.Fatalf("expected %d labels after idempotent ensure, got %d", len(wantNames), len(aliceLabelsAfter))
	}
	for _, lbl := range aliceLabelsAfter {
		if lbl.ID != aliceIDsByName[lbl.Name] {
			t.Fatalf("EnsureUserBuiltInLabels replaced existing label ID for %q: %s -> %s", lbl.Name, aliceIDsByName[lbl.Name], lbl.ID)
		}
	}

	if err := labelRepo.EnsureUserBuiltInLabels(ctx, domain.MustNewID()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound seeding nonexistent user, got: %v", err)
	}
}

func TestLabelRepository_LabelCRUDAndOwnerIsolation(t *testing.T) {
	ctx := context.Background()
	labelRepo, userRepo := setupLabelRepos(t)

	alice := createSeededTestUser(ctx, t, userRepo, "alice-crud")
	bob := createSeededTestUser(ctx, t, userRepo, "bob-crud")

	// Same label name is allowed across SERVER and distinct USER owners.
	srvLbl := &domain.Label{
		ID:    domain.MustNewID(),
		Scope: domain.LabelScopeServer,
		Name:  "archive",
	}
	aliceLbl := &domain.Label{
		ID:      domain.MustNewID(),
		Scope:   domain.LabelScopeUser,
		OwnerID: &alice.ID,
		Name:    "archive",
	}
	bobLbl := &domain.Label{
		ID:      domain.MustNewID(),
		Scope:   domain.LabelScopeUser,
		OwnerID: &bob.ID,
		Name:    "archive",
	}

	for _, lbl := range []*domain.Label{srvLbl, aliceLbl, bobLbl} {
		if err := labelRepo.CreateLabel(ctx, lbl); err != nil {
			t.Fatalf("CreateLabel(%s/%s): %v", lbl.Scope, lbl.Name, err)
		}
	}

	// Duplicates in the same scope/owner fail with ErrAlreadyExists.
	dupSrv := &domain.Label{ID: domain.MustNewID(), Scope: domain.LabelScopeServer, Name: "archive"}
	if err := labelRepo.CreateLabel(ctx, dupSrv); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists for duplicate SERVER label, got: %v", err)
	}
	dupAlice := &domain.Label{ID: domain.MustNewID(), Scope: domain.LabelScopeUser, OwnerID: &alice.ID, Name: "archive"}
	if err := labelRepo.CreateLabel(ctx, dupAlice); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists for duplicate USER label, got: %v", err)
	}

	gotSrv, err := labelRepo.GetServerLabelByName(ctx, "archive")
	if err != nil || gotSrv.ID != srvLbl.ID {
		t.Fatalf("GetServerLabelByName: got=%+v err=%v", gotSrv, err)
	}
	gotAlice, err := labelRepo.GetUserLabelByName(ctx, alice.ID, "archive")
	if err != nil || gotAlice.ID != aliceLbl.ID {
		t.Fatalf("GetUserLabelByName(alice): got=%+v err=%v", gotAlice, err)
	}
	gotByID, err := labelRepo.GetLabelByID(ctx, bobLbl.ID)
	if err != nil || gotByID.Name != "archive" || *gotByID.OwnerID != bob.ID {
		t.Fatalf("GetLabelByID(bob): got=%+v err=%v", gotByID, err)
	}

	// Renaming preserves label ID.
	renamed, err := labelRepo.RenameLabel(ctx, aliceLbl.ID, "reading-archive")
	if err != nil {
		t.Fatalf("RenameLabel: %v", err)
	}
	if renamed.ID != aliceLbl.ID || renamed.Name != "reading-archive" {
		t.Fatalf("unexpected renamed label: %+v", renamed)
	}
	if _, err := labelRepo.RenameLabel(ctx, aliceLbl.ID, domain.LabelFavorite); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists renaming to existing personal label, got: %v", err)
	}
	if _, err := labelRepo.RenameLabel(ctx, domain.MustNewID(), "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound renaming unknown label, got: %v", err)
	}

	// Deleting bob cascades only bob's USER labels.
	if err := userRepo.DeleteUser(ctx, bob.ID); err != nil {
		t.Fatalf("DeleteUser(bob): %v", err)
	}
	if _, err := labelRepo.GetLabelByID(ctx, bobLbl.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected bob label to cascade delete, got: %v", err)
	}
	if _, err := labelRepo.GetLabelByID(ctx, aliceLbl.ID); err != nil {
		t.Fatalf("expected alice label to survive bob deletion, got: %v", err)
	}
}

func TestLabelRepository_ItemLabelsCRUD(t *testing.T) {
	ctx := context.Background()
	labelRepo, userRepo := setupLabelRepos(t)

	alice := createSeededTestUser(ctx, t, userRepo, "alice-items")
	fav, err := labelRepo.GetUserLabelByName(ctx, alice.ID, domain.LabelFavorite)
	if err != nil {
		t.Fatalf("GetUserLabelByName: %v", err)
	}
	del, err := labelRepo.GetServerLabelByName(ctx, domain.LabelMarkedForDeletion)
	if err != nil {
		t.Fatalf("GetServerLabelByName: %v", err)
	}

	seriesID := domain.MustNewID()
	volumeID := domain.MustNewID()
	assetID := domain.MustNewID()

	assignments := []*domain.ItemLabel{
		{LabelID: fav.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: del.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: fav.ID, ItemType: domain.ItemTypeVolume, ItemID: volumeID},
		{LabelID: del.ID, ItemType: domain.ItemTypeAsset, ItemID: assetID},
	}
	for _, a := range assignments {
		if err := labelRepo.AssignItemLabel(ctx, a); err != nil {
			t.Fatalf("AssignItemLabel(%+v): %v", a, err)
		}
	}
	// Assigning again is idempotent.
	if err := labelRepo.AssignItemLabel(ctx, assignments[0]); err != nil {
		t.Fatalf("idempotent AssignItemLabel: %v", err)
	}
	// Unknown label maps to ErrNotFound.
	if err := labelRepo.AssignItemLabel(ctx, &domain.ItemLabel{
		LabelID:  domain.MustNewID(),
		ItemType: domain.ItemTypeSeries,
		ItemID:   seriesID,
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown label assignment, got: %v", err)
	}

	seriesLabels, err := labelRepo.ListItemLabels(ctx, domain.ItemTypeSeries, seriesID)
	if err != nil {
		t.Fatalf("ListItemLabels: %v", err)
	}
	if len(seriesLabels) != 2 {
		t.Fatalf("expected 2 labels on series, got %d", len(seriesLabels))
	}

	favSeriesIDs, err := labelRepo.ListItemIDsByLabel(ctx, fav.ID, domain.ItemTypeSeries)
	if err != nil {
		t.Fatalf("ListItemIDsByLabel: %v", err)
	}
	if len(favSeriesIDs) != 1 || favSeriesIDs[0] != seriesID {
		t.Fatalf("unexpected series IDs for favorite: %v", favSeriesIDs)
	}

	// Remove one assignment.
	if err := labelRepo.RemoveItemLabel(ctx, assignments[0]); err != nil {
		t.Fatalf("RemoveItemLabel: %v", err)
	}
	if err := labelRepo.RemoveItemLabel(ctx, assignments[0]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound removing already-deleted item label, got: %v", err)
	}

	// RemoveAllItemLabelsForItem cleans remaining assignments and succeeds when empty.
	if err := labelRepo.RemoveAllItemLabelsForItem(ctx, domain.ItemTypeSeries, seriesID); err != nil {
		t.Fatalf("RemoveAllItemLabelsForItem: %v", err)
	}
	if err := labelRepo.RemoveAllItemLabelsForItem(ctx, domain.ItemTypeSeries, seriesID); err != nil {
		t.Fatalf("idempotent RemoveAllItemLabelsForItem: %v", err)
	}
	remaining, err := labelRepo.ListItemLabels(ctx, domain.ItemTypeSeries, seriesID)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("expected 0 remaining series labels, got=%d err=%v", len(remaining), err)
	}
}

func TestLabelRepository_UserLabelPoliciesAndDeleteRestrict(t *testing.T) {
	ctx := context.Background()
	labelRepo, userRepo := setupLabelRepos(t)

	alice := createSeededTestUser(ctx, t, userRepo, "alice-policy")
	fav, err := labelRepo.GetUserLabelByName(ctx, alice.ID, domain.LabelFavorite)
	if err != nil {
		t.Fatalf("GetUserLabelByName: %v", err)
	}

	srvLabel := &domain.Label{
		ID:    domain.MustNewID(),
		Scope: domain.LabelScopeServer,
		Name:  "nsfw",
	}
	if err := labelRepo.CreateLabel(ctx, srvLabel); err != nil {
		t.Fatalf("CreateLabel: %v", err)
	}

	// Policy pointing at USER label is rejected with ErrInvalidLabelPolicy.
	err = labelRepo.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{
		UserID:  alice.ID,
		LabelID: fav.ID,
		Effect:  domain.PolicyEffectRestrict,
	})
	if !errors.Is(err, storage.ErrInvalidLabelPolicy) {
		t.Fatalf("expected ErrInvalidLabelPolicy for USER label, got: %v", err)
	}

	// Nonexistent label/user yields ErrNotFound.
	err = labelRepo.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{
		UserID:  alice.ID,
		LabelID: domain.MustNewID(),
		Effect:  domain.PolicyEffectAllow,
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing label policy, got: %v", err)
	}

	// Valid SERVER label policy succeeds and can be updated from ALLOW to RESTRICT.
	if err := labelRepo.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{
		UserID:  alice.ID,
		LabelID: srvLabel.ID,
		Effect:  domain.PolicyEffectAllow,
	}); err != nil {
		t.Fatalf("SetUserLabelPolicy(ALLOW): %v", err)
	}
	if err := labelRepo.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{
		UserID:  alice.ID,
		LabelID: srvLabel.ID,
		Effect:  domain.PolicyEffectRestrict,
	}); err != nil {
		t.Fatalf("SetUserLabelPolicy(RESTRICT): %v", err)
	}

	byUser, err := labelRepo.ListUserLabelPolicies(ctx, alice.ID)
	if err != nil || len(byUser) != 1 || byUser[0].Effect != domain.PolicyEffectRestrict {
		t.Fatalf("ListUserLabelPolicies: got=%+v err=%v", byUser, err)
	}
	byLabel, err := labelRepo.ListLabelPoliciesByLabel(ctx, srvLabel.ID)
	if err != nil || len(byLabel) != 1 || byLabel[0].UserID != alice.ID {
		t.Fatalf("ListLabelPoliciesByLabel: got=%+v err=%v", byLabel, err)
	}

	// Renaming a referenced SERVER label preserves policy links.
	if _, err := labelRepo.RenameLabel(ctx, srvLabel.ID, "adult-18"); err != nil {
		t.Fatalf("RenameLabel: %v", err)
	}

	// Referenced SERVER label cannot be deleted until its policies are removed.
	if err := labelRepo.DeleteLabel(ctx, srvLabel.ID); !errors.Is(err, storage.ErrLabelInUse) {
		t.Fatalf("expected ErrLabelInUse deleting referenced SERVER label, got: %v", err)
	}
	if _, err := labelRepo.GetLabelByID(ctx, srvLabel.ID); err != nil {
		t.Fatalf("referenced SERVER label should still exist: %v", err)
	}

	// Removing policy allows deleting the SERVER label.
	if err := labelRepo.RemoveUserLabelPolicy(ctx, alice.ID, srvLabel.ID); err != nil {
		t.Fatalf("RemoveUserLabelPolicy: %v", err)
	}
	if err := labelRepo.RemoveUserLabelPolicy(ctx, alice.ID, srvLabel.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound removing deleted policy, got: %v", err)
	}
	if err := labelRepo.DeleteLabel(ctx, srvLabel.ID); err != nil {
		t.Fatalf("DeleteLabel after policy removal: %v", err)
	}
	if err := labelRepo.DeleteLabel(ctx, srvLabel.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound deleting missing label, got: %v", err)
	}
}

func TestLabelRepository_WithTxAndNestedSavepoint(t *testing.T) {
	ctx := context.Background()
	labelRepo, _ := setupLabelRepos(t)

	outerLbl := &domain.Label{
		ID:    domain.MustNewID(),
		Scope: domain.LabelScopeServer,
		Name:  "outer-committed",
	}
	innerLbl := &domain.Label{
		ID:    domain.MustNewID(),
		Scope: domain.LabelScopeServer,
		Name:  "inner-rolled-back",
	}
	sentinelErr := errors.New("abort nested savepoint")

	err := labelRepo.WithTx(ctx, func(tx storage.LabelRepository) error {
		if err := tx.CreateLabel(ctx, outerLbl); err != nil {
			return err
		}
		nestedErr := tx.WithTx(ctx, func(nested storage.LabelRepository) error {
			if err := nested.CreateLabel(ctx, innerLbl); err != nil {
				return err
			}
			return sentinelErr
		})
		if !errors.Is(nestedErr, sentinelErr) {
			return nestedErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer WithTx failed: %v", err)
	}

	if _, err := labelRepo.GetLabelByID(ctx, outerLbl.ID); err != nil {
		t.Fatalf("expected outer label to commit, got: %v", err)
	}
	if _, err := labelRepo.GetLabelByID(ctx, innerLbl.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected nested rolled-back label not to exist, got: %v", err)
	}
}
