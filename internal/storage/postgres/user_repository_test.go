package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupTestDB(t *testing.T) (*testdb.TestDB, storage.UserRepository) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupTestDB: applying migrations: %v", err)
	}

	repo := postgres.NewUserRepository(db.Pool)
	return db, repo
}

// TestUserWithPasswordCredentialRoundTrip tests the slice acceptance criterion:
// "a round-trip test creates a user with a PASSWORD credential and reads it back through the repository."
func TestUserWithPasswordCredentialRoundTrip(t *testing.T) {
	t.Parallel()
	_, repo := setupTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	userID := domain.MustNewID()
	credID := domain.MustNewID()

	user := &domain.User{
		ID:       userID,
		Username: "alice",
	}

	// 1. Create User
	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// 2. Create PASSWORD credential
	secretHash := "$argon2id$v=19$m=65536,t=3,p=4$somehashvalue"
	hashScheme := "argon2id"
	cred := &domain.Credential{
		ID:         credID,
		UserID:     userID,
		Kind:       domain.CredentialKindPassword,
		Scopes:     nil, // PASSWORD credentials have no scopes (full role grant)
		SecretHash: &secretHash,
		HashScheme: &hashScheme,
	}

	if err := repo.CreateCredential(ctx, cred); err != nil {
		t.Fatalf("CreateCredential failed: %v", err)
	}

	// 3. Read back user through repository by ID and Username
	readUser, err := repo.GetUserByID(ctx, userID)
	if err != nil {
		t.Fatalf("GetUserByID failed: %v", err)
	}
	if readUser.ID != userID || readUser.Username != "alice" {
		t.Errorf("read user mismatch: got %+v, expected ID=%s Username=alice", readUser, userID)
	}
	if readUser.CreatedAt.IsZero() || readUser.UpdatedAt.IsZero() {
		t.Errorf("expected non-zero timestamps on read user, got %+v", readUser)
	}

	readUserByUsername, err := repo.GetUserByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserByUsername failed: %v", err)
	}
	if readUserByUsername.ID != userID {
		t.Errorf("expected user ID %s, got %s", userID, readUserByUsername.ID)
	}

	// 4. Read back credential through repository by ID and by user ID (active password)
	readCred, err := repo.GetCredentialByID(ctx, credID)
	if err != nil {
		t.Fatalf("GetCredentialByID failed: %v", err)
	}
	if readCred.ID != credID || readCred.UserID != userID || readCred.Kind != domain.CredentialKindPassword {
		t.Errorf("read credential mismatch: got %+v", readCred)
	}
	if readCred.SecretHash == nil || *readCred.SecretHash != secretHash {
		t.Errorf("secret hash mismatch: got %v, expected %s", readCred.SecretHash, secretHash)
	}
	if readCred.HashScheme == nil || *readCred.HashScheme != hashScheme {
		t.Errorf("hash scheme mismatch: got %v, expected %s", readCred.HashScheme, hashScheme)
	}

	activePasswordCred, err := repo.GetActivePasswordCredentialByUserID(ctx, userID)
	if err != nil {
		t.Fatalf("GetActivePasswordCredentialByUserID failed: %v", err)
	}
	if activePasswordCred.ID != credID {
		t.Errorf("expected active password credential ID %s, got %s", credID, activePasswordCred.ID)
	}
}

// TestSeededRolesMatchDomainBuiltInRoles tests the requirement:
// "Add a test that the seeded roles/role_permissions (incl. requires_allow_labels, I-5)
// equal domain.BuiltInRoles(), so the Go constants and the 0001 seed cannot drift."
func TestSeededRolesMatchDomainBuiltInRoles(t *testing.T) {
	t.Parallel()
	_, repo := setupTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seededRoles, err := repo.ListRoles(ctx)
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}

	expectedRoles := domain.BuiltInRoles()

	if len(seededRoles) != len(expectedRoles) {
		t.Fatalf("expected %d seeded roles, got %d", len(expectedRoles), len(seededRoles))
	}

	for i, expected := range expectedRoles {
		actual := seededRoles[i]

		if actual.ID != expected.ID {
			t.Errorf("role [%d] ID mismatch: got %s, expected %s", i, actual.ID, expected.ID)
		}
		if actual.Name != expected.Name {
			t.Errorf("role [%d] Name mismatch: got %s, expected %s", i, actual.Name, expected.Name)
		}
		if actual.BuiltIn != expected.BuiltIn {
			t.Errorf("role [%d] BuiltIn mismatch: got %v, expected %v", i, actual.BuiltIn, expected.BuiltIn)
		}
		if actual.RequiresAllowLabels != expected.RequiresAllowLabels {
			t.Errorf("role [%d] RequiresAllowLabels mismatch (decision I-5): got %v, expected %v",
				i, actual.RequiresAllowLabels, expected.RequiresAllowLabels)
		}
		if !reflect.DeepEqual(actual.Permissions, expected.Permissions) {
			t.Errorf("role [%d] (%s) permissions mismatch:\n got:      %v\n expected: %v",
				i, expected.Name, actual.Permissions, expected.Permissions)
		}
	}

	// Also verify GetRoleByID and GetRoleByName for each built-in role
	for _, expected := range expectedRoles {
		byID, err := repo.GetRoleByID(ctx, expected.ID)
		if err != nil {
			t.Errorf("GetRoleByID(%s) failed: %v", expected.ID, err)
		} else if !reflect.DeepEqual(*byID, expected) {
			t.Errorf("GetRoleByID(%s) returned %+v, expected %+v", expected.ID, *byID, expected)
		}

		byName, err := repo.GetRoleByName(ctx, expected.Name)
		if err != nil {
			t.Errorf("GetRoleByName(%s) failed: %v", expected.Name, err)
		} else if !reflect.DeepEqual(*byName, expected) {
			t.Errorf("GetRoleByName(%s) returned %+v, expected %+v", expected.Name, *byName, expected)
		}
	}
}

// TestUserCRUDOperations tests user management operations: update, delete, count, list, errors.
func TestUserCRUDOperations(t *testing.T) {
	t.Parallel()
	_, repo := setupTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initial user count should be 0
	count, err := repo.CountUsers(ctx)
	if err != nil {
		t.Fatalf("CountUsers failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 users initially, got %d", count)
	}

	u1 := &domain.User{ID: domain.MustNewID(), Username: "user1"}
	u2 := &domain.User{ID: domain.MustNewID(), Username: "user2"}

	if err := repo.CreateUser(ctx, u1); err != nil {
		t.Fatalf("creating u1 failed: %v", err)
	}
	if err := repo.CreateUser(ctx, u2); err != nil {
		t.Fatalf("creating u2 failed: %v", err)
	}

	// Verify count is now 2
	count, err = repo.CountUsers(ctx)
	if err != nil || count != 2 {
		t.Fatalf("expected count 2, got %d (err: %v)", count, err)
	}

	// Verify duplicate username returns ErrAlreadyExists
	duplicateUser := &domain.User{ID: domain.MustNewID(), Username: "user1"}
	if err := repo.CreateUser(ctx, duplicateUser); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists for duplicate username, got %v", err)
	}

	// Update user
	u1.Username = "user1_renamed"
	if err := repo.UpdateUser(ctx, u1); err != nil {
		t.Fatalf("UpdateUser failed: %v", err)
	}
	readU1, err := repo.GetUserByID(ctx, u1.ID)
	if err != nil || readU1.Username != "user1_renamed" {
		t.Errorf("expected username user1_renamed, got %v (err: %v)", readU1, err)
	}

	// Delete user
	if err := repo.DeleteUser(ctx, u1.ID); err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	// Deleting again or getting deleted user should return ErrNotFound
	if _, err := repo.GetUserByID(ctx, u1.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for deleted user, got %v", err)
	}
	if err := repo.DeleteUser(ctx, u1.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound deleting non-existent user, got %v", err)
	}
}

// TestCredentialLifecycle tests PAT creation, listing, revoking, updating last used, and deletion.
func TestCredentialLifecycle(t *testing.T) {
	t.Parallel()
	_, repo := setupTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	user := &domain.User{ID: domain.MustNewID(), Username: "pat_user"}
	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	label := "Komga on iPad"
	patCred := &domain.Credential{
		ID:     domain.MustNewID(),
		UserID: user.ID,
		Kind:   domain.CredentialKindPAT,
		Scopes: []domain.PATScope{domain.ScopeRead, domain.ScopeProgressWrite},
		Label:  &label,
	}

	if err := repo.CreateCredential(ctx, patCred); err != nil {
		t.Fatalf("CreateCredential PAT failed: %v", err)
	}

	// Update last used
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.UpdateCredentialLastUsed(ctx, patCred.ID, now); err != nil {
		t.Fatalf("UpdateCredentialLastUsed failed: %v", err)
	}

	readCred, err := repo.GetCredentialByID(ctx, patCred.ID)
	if err != nil {
		t.Fatalf("GetCredentialByID failed: %v", err)
	}
	if readCred.LastUsedAt == nil {
		t.Errorf("expected last_used_at to be set")
	}

	// Revoke credential
	if err := repo.RevokeCredential(ctx, patCred.ID, now); err != nil {
		t.Fatalf("RevokeCredential failed: %v", err)
	}

	revokedCred, err := repo.GetCredentialByID(ctx, patCred.ID)
	if err != nil {
		t.Fatalf("GetCredentialByID after revoke failed: %v", err)
	}
	if !revokedCred.IsRevoked() {
		t.Errorf("expected credential to be revoked")
	}

	// Delete credential
	if err := repo.DeleteCredential(ctx, patCred.ID); err != nil {
		t.Fatalf("DeleteCredential failed: %v", err)
	}
	if _, err := repo.GetCredentialByID(ctx, patCred.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for deleted credential, got %v", err)
	}
}

// TestUserRoleAssignment tests assigning and removing roles to/from users.
func TestUserRoleAssignment(t *testing.T) {
	t.Parallel()
	_, repo := setupTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	user := &domain.User{ID: domain.MustNewID(), Username: "role_user"}
	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Assign Reader role
	if err := repo.AssignUserRole(ctx, user.ID, domain.RoleReaderID); err != nil {
		t.Fatalf("AssignUserRole failed: %v", err)
	}

	// Duplicate assignment should be a no-op (ON CONFLICT DO NOTHING)
	if err := repo.AssignUserRole(ctx, user.ID, domain.RoleReaderID); err != nil {
		t.Fatalf("AssignUserRole duplicate failed: %v", err)
	}

	userRoles, err := repo.ListUserRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListUserRoles failed: %v", err)
	}
	if len(userRoles) != 1 || userRoles[0].ID != domain.RoleReaderID {
		t.Errorf("expected Reader role assigned, got %+v", userRoles)
	}

	// Remove role
	if err := repo.RemoveUserRole(ctx, user.ID, domain.RoleReaderID); err != nil {
		t.Fatalf("RemoveUserRole failed: %v", err)
	}

	userRoles, err = repo.ListUserRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListUserRoles after removal failed: %v", err)
	}
	if len(userRoles) != 0 {
		t.Errorf("expected 0 roles, got %d", len(userRoles))
	}
}

// TestTransactionRollback tests transactional rollback using WithTx.
func TestTransactionRollback(t *testing.T) {
	t.Parallel()
	db, _ := setupTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pgRepo := postgres.NewUserRepository(db.Pool)

	user := &domain.User{ID: domain.MustNewID(), Username: "tx_user"}

	simulatedErr := errors.New("simulated error during transaction")
	err := pgRepo.WithTx(ctx, func(txRepo storage.UserRepository) error {
		if err := txRepo.CreateUser(ctx, user); err != nil {
			return err
		}
		return simulatedErr
	})

	if !errors.Is(err, simulatedErr) {
		t.Fatalf("expected simulatedErr, got %v", err)
	}

	// Verify user was NOT committed due to rollback
	if _, err := pgRepo.GetUserByID(ctx, user.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for rolled back user, got %v", err)
	}
}
