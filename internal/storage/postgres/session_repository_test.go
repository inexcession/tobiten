package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupSessionRepoTestDB(t *testing.T) (*UserRepository, *SessionRepository) {
	t.Helper()
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := migrations.RunFS(ctx, db.Pool, migrations.CoreFS); err != nil {
		t.Fatalf("RunFS: %v", err)
	}
	return NewUserRepository(db.Pool), NewSessionRepository(db.Pool)
}

func seedUserAndPasswordCred(t *testing.T, users *UserRepository, secretHash string) (userID, credID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	userID = domain.MustNewID()
	credID = domain.MustNewID()
	scheme := "argon2id"

	if err := users.CreateUser(ctx, &domain.User{
		ID:        userID,
		Username:  "user-" + userID[:8],
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := users.CreateCredential(ctx, &domain.Credential{
		ID:         credID,
		UserID:     userID,
		Kind:       domain.CredentialKindPassword,
		SecretHash: &secretHash,
		HashScheme: &scheme,
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateCredential: %v", err)
	}
	return userID, credID
}

func TestSessionRepository_PersistentInvalidationAndNonResurrection(t *testing.T) {
	t.Parallel()
	users, sessions := setupSessionRepoTestDB(t)
	ctx := context.Background()

	const oldHash = "$argon2id$v=19$m=19456,t=2,p=1$old-salt$old-hash"
	userID, credID := seedUserAndPasswordCred(t, users, oldHash)
	now := time.Now().UTC()

	// CreateSession rejects stale/mismatched digest before inserting anything.
	badSess := &domain.Session{
		ID:                   domain.MustNewID(),
		UserID:               userID,
		CredentialID:         credID,
		Kind:                 domain.SessionKindNative,
		TokenHash:            domain.HashSHA256Hex("tok-bad"),
		CSRFTokenHash:        domain.HashSHA256Hex("csrf-bad"),
		CredentialHashDigest: domain.HashSHA256Hex("different-hash"),
		CreatedAt:            now,
		ExpiresAt:            now.Add(time.Hour),
	}
	if err := sessions.CreateSession(ctx, badSess); !errors.Is(err, storage.ErrCredentialChanged) {
		t.Fatalf("expected ErrCredentialChanged for mismatched digest, got %v", err)
	}

	// Valid creation succeeds.
	tok1Hash := domain.HashSHA256Hex("tok-1")
	s1 := &domain.Session{
		ID:                   domain.MustNewID(),
		UserID:               userID,
		CredentialID:         credID,
		Kind:                 domain.SessionKindNative,
		TokenHash:            tok1Hash,
		CSRFTokenHash:        domain.HashSHA256Hex("csrf-1"),
		CredentialHashDigest: domain.HashSHA256Hex(oldHash),
		CreatedAt:            now,
		ExpiresAt:            now.Add(time.Hour),
	}
	if err := sessions.CreateSession(ctx, s1); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}

	// Updating secret_hash atomically revokes s1 in PostgreSQL.
	const newHash = "$argon2id$v=19$m=19456,t=2,p=1$new-salt$new-hash"
	if _, err := sessions.pool.Exec(ctx, `UPDATE credentials SET secret_hash = $2 WHERE id = $1`, credID, newHash); err != nil {
		t.Fatalf("rotate secret_hash: %v", err)
	}
	got1, err := sessions.GetSessionByTokenHash(ctx, tok1Hash)
	if err != nil {
		t.Fatalf("GetSessionByTokenHash after rotation: %v", err)
	}
	if !got1.IsRevoked() {
		t.Fatalf("expected s1.revoked_at to be set persistently on hash rotation, got nil")
	}

	// Restoring oldHash MUST NOT resurrect s1.
	if _, err := sessions.pool.Exec(ctx, `UPDATE credentials SET secret_hash = $2 WHERE id = $1`, credID, oldHash); err != nil {
		t.Fatalf("restore oldHash: %v", err)
	}
	got1After, err := sessions.GetSessionByTokenHash(ctx, tok1Hash)
	if err != nil {
		t.Fatalf("GetSessionByTokenHash after restore: %v", err)
	}
	if !got1After.IsRevoked() {
		t.Fatalf("expected s1 to remain permanently revoked after old hash restoration")
	}

	// New session after restore succeeds, then RevokeCredential persistently revokes it.
	tok2Hash := domain.HashSHA256Hex("tok-2")
	s2 := &domain.Session{
		ID:                   domain.MustNewID(),
		UserID:               userID,
		CredentialID:         credID,
		Kind:                 domain.SessionKindNative,
		TokenHash:            tok2Hash,
		CSRFTokenHash:        domain.HashSHA256Hex("csrf-2"),
		CredentialHashDigest: domain.HashSHA256Hex(oldHash),
		CreatedAt:            now,
		ExpiresAt:            now.Add(time.Hour),
	}
	if err := sessions.CreateSession(ctx, s2); err != nil {
		t.Fatalf("CreateSession s2: %v", err)
	}
	if err := users.RevokeCredential(ctx, credID, time.Now().UTC()); err != nil {
		t.Fatalf("RevokeCredential: %v", err)
	}
	got2, err := sessions.GetSessionByTokenHash(ctx, tok2Hash)
	if err != nil {
		t.Fatalf("GetSessionByTokenHash after RevokeCredential: %v", err)
	}
	if !got2.IsRevoked() {
		t.Fatalf("expected s2.revoked_at to be set persistently by RevokeCredential")
	}

	// Creating session on revoked credential fails with ErrCredentialChanged.
	s3 := &domain.Session{
		ID:                   domain.MustNewID(),
		UserID:               userID,
		CredentialID:         credID,
		Kind:                 domain.SessionKindNative,
		TokenHash:            domain.HashSHA256Hex("tok-3"),
		CSRFTokenHash:        domain.HashSHA256Hex("csrf-3"),
		CredentialHashDigest: domain.HashSHA256Hex(oldHash),
		CreatedAt:            now,
		ExpiresAt:            now.Add(time.Hour),
	}
	if err := sessions.CreateSession(ctx, s3); !errors.Is(err, storage.ErrCredentialChanged) {
		t.Fatalf("expected ErrCredentialChanged on revoked credential, got %v", err)
	}
}

func TestSessionRepository_CreateBlocksOnConcurrentCredentialRotation(t *testing.T) {
	t.Parallel()
	users, sessions := setupSessionRepoTestDB(t)
	ctx := context.Background()

	const oldHash = "$argon2id$v=19$m=19456,t=2,p=1$old$hash"
	const newHash = "$argon2id$v=19$m=19456,t=2,p=1$new$hash"
	userID, credID := seedUserAndPasswordCred(t, users, oldHash)

	// Hold an uncommitted credential rotation transaction updating secret_hash.
	rotTx, err := sessions.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rotation tx: %v", err)
	}
	defer func() { _ = rotTx.Rollback(ctx) }()

	if _, err := rotTx.Exec(ctx, `UPDATE credentials SET secret_hash = $2 WHERE id = $1`, credID, newHash); err != nil {
		t.Fatalf("update secret_hash in tx: %v", err)
	}

	tokHash := domain.HashSHA256Hex("raced-token")
	var (
		wg        sync.WaitGroup
		createErr error
	)
	wg.Go(func() {
		now := time.Now().UTC()
		createErr = sessions.CreateSession(ctx, &domain.Session{
			ID:                   domain.MustNewID(),
			UserID:               userID,
			CredentialID:         credID,
			Kind:                 domain.SessionKindNative,
			TokenHash:            tokHash,
			CSRFTokenHash:        domain.HashSHA256Hex("csrf"),
			CredentialHashDigest: domain.HashSHA256Hex(oldHash),
			CreatedAt:            now,
			ExpiresAt:            now.Add(time.Hour),
		})
	})

	// Give CreateSession a brief window to enqueue on credentials row lock, then commit rotation.
	time.Sleep(100 * time.Millisecond)
	if err := rotTx.Commit(ctx); err != nil {
		t.Fatalf("commit rotation tx: %v", err)
	}
	wg.Wait()

	if !errors.Is(createErr, storage.ErrCredentialChanged) {
		t.Fatalf("expected CreateSession to fail with ErrCredentialChanged after concurrent rotation, got %v", createErr)
	}
	if _, err := sessions.GetSessionByTokenHash(ctx, tokHash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected zero persisted sessions for raced login, got %v", err)
	}
}
