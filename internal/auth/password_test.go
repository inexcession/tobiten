package auth

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

func TestArgon2id_DocumentedParameters(t *testing.T) {
	// Assert the documented production cost parameters (RFC 9106 §7.4, ADR-0006 §3, ADR-0014 §1).
	const (
		expectedMemory      uint32 = 64 * 1024 // 65536 KiB = 64 MiB
		expectedIterations  uint32 = 3         // 3 passes
		expectedParallelism uint8  = 4         // 4 threads
		expectedSaltLength  uint32 = 16        // 128-bit salt
		expectedKeyLength   uint32 = 32        // 256-bit derived key
		expectedVersion     int    = 19        // 0x13 per RFC 9106
	)

	if DefaultArgon2idParams.Memory != expectedMemory {
		t.Errorf("expected Memory=%d, got %d", expectedMemory, DefaultArgon2idParams.Memory)
	}
	if DefaultArgon2idParams.Iterations != expectedIterations {
		t.Errorf("expected Iterations=%d, got %d", expectedIterations, DefaultArgon2idParams.Iterations)
	}
	if DefaultArgon2idParams.Parallelism != expectedParallelism {
		t.Errorf("expected Parallelism=%d, got %d", expectedParallelism, DefaultArgon2idParams.Parallelism)
	}
	if DefaultArgon2idParams.SaltLength != expectedSaltLength {
		t.Errorf("expected SaltLength=%d, got %d", expectedSaltLength, DefaultArgon2idParams.SaltLength)
	}
	if DefaultArgon2idParams.KeyLength != expectedKeyLength {
		t.Errorf("expected KeyLength=%d, got %d", expectedKeyLength, DefaultArgon2idParams.KeyLength)
	}
	if DefaultArgon2idParams.Version != expectedVersion {
		t.Errorf("expected Version=%d, got %d", expectedVersion, DefaultArgon2idParams.Version)
	}
	if DefaultArgon2idParams.Version != argon2.Version {
		t.Errorf("expected Version to match argon2.Version (%d), got %d", argon2.Version, DefaultArgon2idParams.Version)
	}
	if AlgorithmArgon2id != "argon2id" {
		t.Errorf("expected AlgorithmArgon2id to be %q, got %q", "argon2id", AlgorithmArgon2id)
	}
	if HashSchemeArgon2id != "argon2id" {
		t.Errorf("expected HashSchemeArgon2id to be %q, got %q", "argon2id", HashSchemeArgon2id)
	}
}

func TestArgon2id_RoundTrip(t *testing.T) {
	password := "correct-horse-battery-staple-789!"

	encoded, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	// Verify prefix matches standard format with documented parameters.
	expectedPrefix := "$argon2id$v=19$m=65536,t=3,p=4$"
	if !strings.HasPrefix(encoded, expectedPrefix) {
		t.Fatalf("expected hash to start with %q, got %q", expectedPrefix, encoded)
	}

	// Verify with the correct password returns true.
	match, err := VerifyPassword(password, encoded)
	if err != nil {
		t.Fatalf("VerifyPassword failed: %v", err)
	}
	if !match {
		t.Errorf("expected password to match, got false")
	}

	// ComparePassword with the correct password returns nil.
	if err := ComparePassword(password, encoded); err != nil {
		t.Errorf("ComparePassword failed on matching password: %v", err)
	}

	// Verify with a wrong password returns false.
	wrongPassword := "incorrect-password"
	match, err = VerifyPassword(wrongPassword, encoded)
	if err != nil {
		t.Fatalf("VerifyPassword failed on wrong password: %v", err)
	}
	if match {
		t.Errorf("expected wrong password to not match, got true")
	}

	// ComparePassword with wrong password returns ErrPasswordMismatch.
	if err := ComparePassword(wrongPassword, encoded); !errors.Is(err, ErrPasswordMismatch) {
		t.Errorf("expected ErrPasswordMismatch on wrong password, got %v", err)
	}
}

func TestArgon2id_ParseAndDocumentedValues(t *testing.T) {
	password := "admin-bootstrap-password-42"
	encoded, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	params, salt, key, err := ParseArgon2idHash(encoded)
	if err != nil {
		t.Fatalf("ParseArgon2idHash failed: %v", err)
	}

	if params.Memory != DefaultArgon2idParams.Memory {
		t.Errorf("expected Memory=%d, got %d", DefaultArgon2idParams.Memory, params.Memory)
	}
	if params.Iterations != DefaultArgon2idParams.Iterations {
		t.Errorf("expected Iterations=%d, got %d", DefaultArgon2idParams.Iterations, params.Iterations)
	}
	if params.Parallelism != DefaultArgon2idParams.Parallelism {
		t.Errorf("expected Parallelism=%d, got %d", DefaultArgon2idParams.Parallelism, params.Parallelism)
	}
	if params.SaltLength != DefaultArgon2idParams.SaltLength {
		t.Errorf("expected SaltLength=%d, got %d", DefaultArgon2idParams.SaltLength, params.SaltLength)
	}
	if params.KeyLength != DefaultArgon2idParams.KeyLength {
		t.Errorf("expected KeyLength=%d, got %d", DefaultArgon2idParams.KeyLength, params.KeyLength)
	}
	if params.Version != DefaultArgon2idParams.Version {
		t.Errorf("expected Version=%d, got %d", DefaultArgon2idParams.Version, params.Version)
	}

	if len(salt) != int(DefaultArgon2idParams.SaltLength) {
		t.Errorf("expected salt length %d, got %d", DefaultArgon2idParams.SaltLength, len(salt))
	}
	if len(key) != int(DefaultArgon2idParams.KeyLength) {
		t.Errorf("expected key length %d, got %d", DefaultArgon2idParams.KeyLength, len(key))
	}
}

func TestArgon2id_CostValidation_BoundedTime(t *testing.T) {
	password := "sample-interactive-login-password"

	// 1. Hash the password with default production parameters.
	hashStart := time.Now()
	encoded, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	hashDuration := time.Since(hashStart)

	// 2. Measure verification time (which runs the Argon2id key derivation).
	verifyStart := time.Now()
	match, err := VerifyPassword(password, encoded)
	if err != nil {
		t.Fatalf("VerifyPassword failed: %v", err)
	}
	if !match {
		t.Fatal("expected password to match")
	}
	verifyDuration := time.Since(verifyStart)

	t.Logf("Argon2id cost validation: hash=%v, verify=%v (memory=%d KiB, iterations=%d, parallelism=%d)",
		hashDuration, verifyDuration, DefaultArgon2idParams.Memory, DefaultArgon2idParams.Iterations, DefaultArgon2idParams.Parallelism)

	// Bounded time assertions:
	// - Lower bound (5ms): proves non-trivial memory-hard work was actually executed.
	// - Upper bound (2s): proves evaluation completes within bounded interactive limits.
	if verifyDuration < 5*time.Millisecond {
		t.Errorf("verification completed suspiciously fast (%v < 5ms); expected genuine memory-hard computation", verifyDuration)
	}
	if verifyDuration > 2*time.Second {
		t.Errorf("verification exceeded upper time bound (%v > 2s)", verifyDuration)
	}
}

func TestArgon2id_Verification_MalformedAndErrors(t *testing.T) {
	validSalt := base64.RawStdEncoding.EncodeToString([]byte("0123456789abcdef"))
	validKey := base64.RawStdEncoding.EncodeToString(make([]byte, 32))

	tests := []struct {
		name    string
		hash    string
		wantErr error
	}{
		{
			name:    "empty string",
			hash:    "",
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "missing leading dollar",
			hash:    "argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "too few parts",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "too many parts",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$" + validKey + "$extra",
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "unsupported algorithm bcrypt",
			hash:    "$bcrypt$v=19$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrUnsupportedAlgorithm,
		},
		{
			name:    "unsupported algorithm pbkdf2",
			hash:    "$pbkdf2$v=19$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrUnsupportedAlgorithm,
		},
		{
			name:    "incompatible version",
			hash:    "$argon2id$v=16$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrIncompatibleVersion,
		},
		{
			name:    "invalid version string",
			hash:    "$argon2id$v=xyz$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "missing memory parameter",
			hash:    "$argon2id$v=19$t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "missing iterations parameter",
			hash:    "$argon2id$v=19$m=65536,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "missing parallelism parameter",
			hash:    "$argon2id$v=19$m=65536,t=3$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "zero memory",
			hash:    "$argon2id$v=19$m=0,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "zero iterations",
			hash:    "$argon2id$v=19$m=65536,t=0,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "zero parallelism",
			hash:    "$argon2id$v=19$m=65536,t=3,p=0$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "parallelism overflow",
			hash:    "$argon2id$v=19$m=65536,t=3,p=256$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "unknown cost parameter",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4,unknown=1$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid salt base64",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$not-valid-base64!$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "empty salt",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid key base64",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$not-valid-base64!",
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "empty key",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$",
			wantErr: ErrInvalidHashFormat,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			match, err := VerifyPassword("any-password", tc.hash)
			if match {
				t.Errorf("expected match=false on invalid hash, got true")
			}
			if err == nil {
				t.Fatalf("expected error on invalid hash, got nil")
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("expected error %v, got %v", tc.wantErr, err)
			}

			// ComparePassword should also return an error.
			cmpErr := ComparePassword("any-password", tc.hash)
			if cmpErr == nil {
				t.Fatalf("ComparePassword expected error, got nil")
			}
			if !errors.Is(cmpErr, tc.wantErr) {
				t.Errorf("ComparePassword expected %v, got %v", tc.wantErr, cmpErr)
			}
		})
	}
}

func TestArgon2id_NeedsRehash(t *testing.T) {
	// A freshly hashed password with default parameters should not need rehash.
	encoded, err := HashPassword("test-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	if NeedsRehash(encoded) {
		t.Errorf("expected NeedsRehash=false for default params, got true")
	}

	// A hash with lower cost parameters should need rehash.
	lowerParams := Argon2idParams{
		Memory:      16 * 1024,
		Iterations:  2,
		Parallelism: 2,
		SaltLength:  16,
		KeyLength:   32,
		Version:     argon2.Version,
	}
	lowerHash, err := HashPasswordWithParams("test-password", lowerParams)
	if err != nil {
		t.Fatalf("HashPasswordWithParams failed: %v", err)
	}

	if !NeedsRehash(lowerHash) {
		t.Errorf("expected NeedsRehash=true for lower params against DefaultArgon2idParams, got false")
	}
	// But against lowerParams itself, it should not need rehash.
	if NeedsRehash(lowerHash, lowerParams) {
		t.Errorf("expected NeedsRehash=false when target matches lowerParams, got true")
	}

	// Malformed hash should need rehash.
	if !NeedsRehash("invalid-hash") {
		t.Errorf("expected NeedsRehash=true for invalid hash, got false")
	}
}

func TestArgon2id_CustomParams_RoundTrip(t *testing.T) {
	fastParams := Argon2idParams{
		Memory:      1024,
		Iterations:  1,
		Parallelism: 1,
		SaltLength:  16,
		KeyLength:   32,
		Version:     argon2.Version,
	}

	encoded, err := HashPasswordWithParams("fast-password", fastParams)
	if err != nil {
		t.Fatalf("HashPasswordWithParams failed: %v", err)
	}

	match, err := VerifyPassword("fast-password", encoded)
	if err != nil {
		t.Fatalf("VerifyPassword failed: %v", err)
	}
	if !match {
		t.Errorf("expected password to match, got false")
	}

	match, err = VerifyPassword("wrong-password", encoded)
	if err != nil {
		t.Fatalf("VerifyPassword failed on wrong: %v", err)
	}
	if match {
		t.Errorf("expected wrong password to not match, got true")
	}
}

func TestArgon2id_PaddedBase64Acceptance(t *testing.T) {
	// Verify that padded base64 strings (using StdEncoding with =) are also accepted.
	salt := []byte("16-byte-salt-val")
	key := argon2.IDKey([]byte("secret"), salt, 1, 1024, 1, 32)

	b64PaddedSalt := base64.StdEncoding.EncodeToString(salt)
	b64PaddedKey := base64.StdEncoding.EncodeToString(key)

	paddedHash := "$argon2id$v=19$m=1024,t=1,p=1$" + b64PaddedSalt + "$" + b64PaddedKey

	match, err := VerifyPassword("secret", paddedHash)
	if err != nil {
		t.Fatalf("VerifyPassword failed on padded base64 hash: %v", err)
	}
	if !match {
		t.Errorf("expected match=true for padded hash, got false")
	}
}

func TestArgon2id_HashPasswordWithParams_Validation(t *testing.T) {
	// Zero salt length
	_, err := HashPasswordWithParams("pw", Argon2idParams{SaltLength: 0, KeyLength: 32, Memory: 1024, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for zero salt length, got nil")
	}

	// Zero key length
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 0, Memory: 1024, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for zero key length, got nil")
	}

	// Zero memory
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 32, Memory: 0, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for zero memory, got nil")
	}
}
