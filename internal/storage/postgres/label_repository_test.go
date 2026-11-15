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

	// EnsureUserBuiltInLabels preserves existing IDs and item assignments when completing partial sets.
	seriesID := domain.MustNewID()
	if err := labelRepo.AssignItemLabel(ctx, &domain.ItemLabel{
		LabelID:  aliceIDsByName[domain.LabelFavorite],
		ItemType: domain.ItemTypeSeries,
		ItemID:   seriesID,
	}); err != nil {
		t.Fatalf("AssignItemLabel(favorite): %v", err)
	}
	if err := labelRepo.DeleteLabel(ctx, aliceIDsByName[domain.LabelDNF]); err != nil {
		t.Fatalf("DeleteLabel(dnf): %v", err)
	}
	if err := labelRepo.EnsureUserBuiltInLabels(ctx, alice.ID); err != nil {
		t.Fatalf("EnsureUserBuiltInLabels(alice): %v", err)
	}
	aliceLabelsAfter, err := labelRepo.ListUserLabels(ctx, alice.ID)
	if err != nil {
		t.Fatalf("ListUserLabels(alice) after ensure: %v", err)
	}
	if len(aliceLabelsAfter) != len(wantNames) {
		t.Fatalf("expected %d labels after partial completion, got %d", len(wantNames), len(aliceLabelsAfter))
	}
	for _, lbl := range aliceLabelsAfter {
		if lbl.Name != domain.LabelDNF && lbl.ID != aliceIDsByName[lbl.Name] {
			t.Fatalf("EnsureUserBuiltInLabels replaced existing label ID for %q: %s -> %s", lbl.Name, aliceIDsByName[lbl.Name], lbl.ID)
		}
	}
	assigned, err := labelRepo.ListItemIDsByLabel(ctx, alice.ID, aliceIDsByName[domain.LabelFavorite], domain.ItemTypeSeries)
	if err != nil || len(assigned) != 1 || assigned[0] != seriesID {
		t.Fatalf("expected favorite item assignment to survive EnsureUserBuiltInLabels, got=%v err=%v", assigned, err)
	}

	if err := labelRepo.EnsureUserBuiltInLabels(ctx, domain.MustNewID()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound seeding nonexistent user, got: %v", err)
	}
}

func TestLabelRepository_CanonicalTrashLabelProtection(t *testing.T) {
	ctx := context.Background()
	labelRepo, userRepo := setupLabelRepos(t)

	alice := createSeededTestUser(ctx, t, userRepo, "alice-trash")
	seriesID := domain.MustNewID()
	volumeID := domain.MustNewID()

	// Stage items with the canonical trash label.
	for _, il := range []*domain.ItemLabel{
		{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeVolume, ItemID: volumeID},
	} {
		if err := labelRepo.AssignItemLabel(ctx, il); err != nil {
			t.Fatalf("AssignItemLabel(trash): %v", err)
		}
	}

	// Generic RenameLabel and DeleteLabel must refuse to modify or delete the canonical trash label.
	if _, err := labelRepo.RenameLabel(ctx, domain.LabelMarkedForDeletionID, "custom-trash"); !errors.Is(err, storage.ErrProtectedLabel) {
		t.Fatalf("expected ErrProtectedLabel renaming canonical trash label, got: %v", err)
	}
	if _, err := labelRepo.RenameLabel(ctx, domain.LabelMarkedForDeletionID, domain.LabelMarkedForDeletion); !errors.Is(err, storage.ErrProtectedLabel) {
		t.Fatalf("expected ErrProtectedLabel on same-name RenameLabel of canonical trash label, got: %v", err)
	}
	if err := labelRepo.DeleteLabel(ctx, domain.LabelMarkedForDeletionID); !errors.Is(err, storage.ErrProtectedLabel) {
		t.Fatalf("expected ErrProtectedLabel deleting canonical trash label, got: %v", err)
	}

	// Reseeding must stay idempotent and preserve the canonical ID and staged marks.
	if err := labelRepo.EnsureBuiltInServerLabels(ctx); err != nil {
		t.Fatalf("EnsureBuiltInServerLabels after rejected mutations: %v", err)
	}
	trashLbl, err := labelRepo.GetServerLabelByName(ctx, domain.LabelMarkedForDeletion)
	if err != nil || trashLbl.ID != domain.LabelMarkedForDeletionID || !trashLbl.BuiltIn {
		t.Fatalf("unexpected canonical trash label state: lbl=%+v err=%v", trashLbl, err)
	}

	seriesLabels, err := labelRepo.ListItemLabels(ctx, alice.ID, domain.ItemTypeSeries, seriesID)
	if err != nil || len(seriesLabels) != 1 || seriesLabels[0].ID != domain.LabelMarkedForDeletionID {
		t.Fatalf("expected staged series trash mark to remain intact, got=%+v err=%v", seriesLabels, err)
	}
	trashedVolumes, err := labelRepo.ListItemIDsByLabel(ctx, alice.ID, domain.LabelMarkedForDeletionID, domain.ItemTypeVolume)
	if err != nil || len(trashedVolumes) != 1 || trashedVolumes[0] != volumeID {
		t.Fatalf("expected staged volume trash mark to remain intact, got=%v err=%v", trashedVolumes, err)
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

	// Renaming ordinary labels preserves ID.
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

func TestLabelRepository_ItemLabelsCRUDAndViewerScoping(t *testing.T) {
	ctx := context.Background()
	labelRepo, userRepo := setupLabelRepos(t)

	alice := createSeededTestUser(ctx, t, userRepo, "alice-items")
	bob := createSeededTestUser(ctx, t, userRepo, "bob-items")

	aliceFav, err := labelRepo.GetUserLabelByName(ctx, alice.ID, domain.LabelFavorite)
	if err != nil {
		t.Fatalf("GetUserLabelByName(alice): %v", err)
	}
	bobDNF, err := labelRepo.GetUserLabelByName(ctx, bob.ID, domain.LabelDNF)
	if err != nil {
		t.Fatalf("GetUserLabelByName(bob): %v", err)
	}

	mature := &domain.Label{
		ID:    domain.MustNewID(),
		Scope: domain.LabelScopeServer,
		Name:  "mature-content",
	}
	if err := labelRepo.CreateLabel(ctx, mature); err != nil {
		t.Fatalf("CreateLabel(mature): %v", err)
	}
	aliceCustom := &domain.Label{
		ID:      domain.MustNewID(),
		Scope:   domain.LabelScopeUser,
		OwnerID: &alice.ID,
		Name:    "alice-weekend",
	}
	bobCustom := &domain.Label{
		ID:      domain.MustNewID(),
		Scope:   domain.LabelScopeUser,
		OwnerID: &bob.ID,
		Name:    "bob-backlog",
	}
	for _, lbl := range []*domain.Label{aliceCustom, bobCustom} {
		if err := labelRepo.CreateLabel(ctx, lbl); err != nil {
			t.Fatalf("CreateLabel(%s): %v", lbl.Name, err)
		}
	}

	seriesID := domain.MustNewID()
	volumeID := domain.MustNewID()
	assetID := domain.MustNewID()

	// Both Alice and Bob tag the same shared series, plus SERVER labels mature and marked_for_deletion.
	assignments := []*domain.ItemLabel{
		{LabelID: aliceFav.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: aliceCustom.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: bobDNF.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: bobCustom.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: mature.ID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeSeries, ItemID: seriesID},
		{LabelID: aliceFav.ID, ItemType: domain.ItemTypeVolume, ItemID: volumeID},
		{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeAsset, ItemID: assetID},
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

	// Empty viewer is rejected on viewer-scoped reads.
	if _, err := labelRepo.ListItemLabels(ctx, "", domain.ItemTypeSeries, seriesID); !errors.Is(err, domain.ErrEmptyUserID) {
		t.Fatalf("expected ErrEmptyUserID on empty viewer ListItemLabels, got: %v", err)
	}
	if _, err := labelRepo.ListItemIDsByLabel(ctx, "", mature.ID, domain.ItemTypeSeries); !errors.Is(err, domain.ErrEmptyUserID) {
		t.Fatalf("expected ErrEmptyUserID on empty viewer ListItemIDsByLabel, got: %v", err)
	}

	// Alice sees SERVER labels + Alice's USER labels only (zero Bob labels).
	aliceVisible, err := labelRepo.ListItemLabels(ctx, alice.ID, domain.ItemTypeSeries, seriesID)
	if err != nil {
		t.Fatalf("ListItemLabels(alice): %v", err)
	}
	assertLabelIDSet(t, "alice ListItemLabels", aliceVisible,
		domain.LabelMarkedForDeletionID, mature.ID, aliceFav.ID, aliceCustom.ID)

	// Bob sees SERVER labels + Bob's USER labels only (zero Alice labels).
	bobVisible, err := labelRepo.ListItemLabels(ctx, bob.ID, domain.ItemTypeSeries, seriesID)
	if err != nil {
		t.Fatalf("ListItemLabels(bob): %v", err)
	}
	assertLabelIDSet(t, "bob ListItemLabels", bobVisible,
		domain.LabelMarkedForDeletionID, mature.ID, bobDNF.ID, bobCustom.ID)

	// SERVER-only read excludes all USER labels while preserving SERVER visibility.
	serverVisible, err := labelRepo.ListServerItemLabels(ctx, domain.ItemTypeSeries, seriesID)
	if err != nil {
		t.Fatalf("ListServerItemLabels: %v", err)
	}
	assertLabelIDSet(t, "ListServerItemLabels", serverVisible,
		domain.LabelMarkedForDeletionID, mature.ID)

	// Privileged maintenance read returns all 6 labels across SERVER and both users.
	allMaint, err := labelRepo.ListAllItemLabelsForMaintenance(ctx, domain.ItemTypeSeries, seriesID)
	if err != nil {
		t.Fatalf("ListAllItemLabelsForMaintenance: %v", err)
	}
	assertLabelIDSet(t, "ListAllItemLabelsForMaintenance", allMaint,
		domain.LabelMarkedForDeletionID, mature.ID, aliceFav.ID, aliceCustom.ID, bobDNF.ID, bobCustom.ID)

	// Reverse lookup respects viewer scope: Alice sees her favorite series, Bob sees nothing for Alice's label ID.
	aliceSeriesByFav, err := labelRepo.ListItemIDsByLabel(ctx, alice.ID, aliceFav.ID, domain.ItemTypeSeries)
	if err != nil || len(aliceSeriesByFav) != 1 || aliceSeriesByFav[0] != seriesID {
		t.Fatalf("ListItemIDsByLabel(alice, aliceFav): got=%v err=%v", aliceSeriesByFav, err)
	}
	bobSeriesByAliceFav, err := labelRepo.ListItemIDsByLabel(ctx, bob.ID, aliceFav.ID, domain.ItemTypeSeries)
	if err != nil || len(bobSeriesByAliceFav) != 0 {
		t.Fatalf("expected Bob reverse lookup on Alice private label to return empty, got=%v err=%v", bobSeriesByAliceFav, err)
	}
	// Both Alice and Bob retain reverse lookup visibility for SERVER labels.
	bobSeriesByMature, err := labelRepo.ListItemIDsByLabel(ctx, bob.ID, mature.ID, domain.ItemTypeSeries)
	if err != nil || len(bobSeriesByMature) != 1 || bobSeriesByMature[0] != seriesID {
		t.Fatalf("ListItemIDsByLabel(bob, SERVER mature): got=%v err=%v", bobSeriesByMature, err)
	}

	// Remove one assignment and verify ErrNotFound on repeat.
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
	remaining, err := labelRepo.ListAllItemLabelsForMaintenance(ctx, domain.ItemTypeSeries, seriesID)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("expected 0 remaining series labels after purge cleanup, got=%d err=%v", len(remaining), err)
	}
}

func assertLabelIDSet(t *testing.T, label string, got []domain.Label, wantIDs ...string) {
	t.Helper()
	gotSet := make(map[string]bool, len(got))
	for _, l := range got {
		gotSet[l.ID] = true
	}
	if len(got) != len(wantIDs) {
		t.Fatalf("%s: expected %d labels (%v), got %d (%+v)", label, len(wantIDs), wantIDs, len(got), got)
	}
	for _, id := range wantIDs {
		if !gotSet[id] {
			t.Fatalf("%s: missing expected label ID %s in %+v", label, id, got)
		}
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
