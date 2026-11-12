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
			name:    "incompatible version 16",
			hash:    "$argon2id$v=16$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrIncompatibleVersion,
		},
		{
			name:    "incompatible version 20",
			hash:    "$argon2id$v=20$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrIncompatibleVersion,
		},
		{
			name:    "trailing version junk",
			hash:    "$argon2id$v=19junk$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "version with hyphen suffix",
			hash:    "$argon2id$v=19-rc1$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "version with decimal junk",
			hash:    "$argon2id$v=19.0$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "empty version value",
			hash:    "$argon2id$v=$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "missing version prefix",
			hash:    "$argon2id$19$m=65536,t=3,p=4$" + validSalt + "$" + validKey,
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
			name:    "duplicate memory parameter",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4,m=1024$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "duplicate iterations parameter",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4,t=2$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "duplicate parallelism parameter",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4,p=2$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid tuple empty element",
			hash:    "$argon2id$v=19$m=65536,,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid tuple missing key",
			hash:    "$argon2id$v=19$=65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid tuple missing value",
			hash:    "$argon2id$v=19$m=,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid tuple multiple equals",
			hash:    "$argon2id$v=19$m=65536=1,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid tuple non-digit value",
			hash:    "$argon2id$v=19$m=65536kb,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "invalid tuple signed value",
			hash:    "$argon2id$v=19$m=+65536,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "memory below RFC 9106 minimum 8*p",
			hash:    "$argon2id$v=19$m=16,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "excessive memory",
			hash:    "$argon2id$v=19$m=1048576,t=3,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "zero iterations",
			hash:    "$argon2id$v=19$m=65536,t=0,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "excessive iterations",
			hash:    "$argon2id$v=19$m=65536,t=100,p=4$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "zero parallelism",
			hash:    "$argon2id$v=19$m=65536,t=3,p=0$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "excessive parallelism",
			hash:    "$argon2id$v=19$m=65536,t=3,p=32$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "parallelism overflow uint8",
			hash:    "$argon2id$v=19$m=65536,t=3,p=256$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "unknown cost parameter",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4,unknown=1$" + validSalt + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "salt too short (<8 bytes)",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + base64.RawStdEncoding.EncodeToString([]byte("short")) + "$" + validKey,
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "salt too long (>64 bytes)",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + base64.RawStdEncoding.EncodeToString(make([]byte, 65)) + "$" + validKey,
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
			name:    "key too short (<16 bytes)",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$" + base64.RawStdEncoding.EncodeToString([]byte("tooshortkey")),
			wantErr: ErrInvalidHashFormat,
		},
		{
			name:    "key too long (>64 bytes)",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$" + base64.RawStdEncoding.EncodeToString(make([]byte, 65)),
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
		{
			name:    "excessive total string length",
			hash:    "$argon2id$v=19$m=65536,t=3,p=4$" + validSalt + "$" + strings.Repeat("a", 600),
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

func TestArgon2id_InvalidInputsNeverReachKDF(t *testing.T) {
	var kdfCalled bool
	originalKDF := argon2idKDF
	argon2idKDF = func(password, salt []byte, time, memory uint32, threads uint8, keyLen uint32) []byte {
		kdfCalled = true
		t.Fatalf("argon2idKDF was unexpectedly invoked with time=%d, memory=%d on invalid input", time, memory)
		return nil
	}
	defer func() { argon2idKDF = originalKDF }()

	validSalt := base64.RawStdEncoding.EncodeToString([]byte("0123456789abcdef"))
	validKey := base64.RawStdEncoding.EncodeToString(make([]byte, 32))

	// Excessive costs, duplicate parameters, invalid tuples, and malformed inputs.
	invalidHashes := []struct {
		name string
		hash string
	}{
		{"excessive iterations", "$argon2id$v=19$m=65536,t=1000,p=4$" + validSalt + "$" + validKey},
		{"excessive memory", "$argon2id$v=19$m=1048576,t=3,p=4$" + validSalt + "$" + validKey},
		{"duplicate m parameter", "$argon2id$v=19$m=65536,t=3,p=4,m=1024$" + validSalt + "$" + validKey},
		{"duplicate t parameter", "$argon2id$v=19$m=65536,t=3,p=4,t=2$" + validSalt + "$" + validKey},
		{"duplicate p parameter", "$argon2id$v=19$m=65536,t=3,p=4,p=2$" + validSalt + "$" + validKey},
		{"trailing version junk", "$argon2id$v=19junk$m=65536,t=3,p=4$" + validSalt + "$" + validKey},
		{"unsupported algorithm", "$bcrypt$v=19$m=65536,t=3,p=4$" + validSalt + "$" + validKey},
		{"salt too short", "$argon2id$v=19$m=65536,t=3,p=4$" + base64.RawStdEncoding.EncodeToString([]byte("short")) + "$" + validKey},
		{"excessive parallelism", "$argon2id$v=19$m=65536,t=3,p=32$" + validSalt + "$" + validKey},
		{"invalid tuple syntax", "$argon2id$v=19$m=65536=1,t=3,p=4$" + validSalt + "$" + validKey},
	}

	for _, tc := range invalidHashes {
		t.Run(tc.name, func(t *testing.T) {
			kdfCalled = false
			match, err := VerifyPassword("password", tc.hash)
			if match {
				t.Errorf("expected match=false, got true")
			}
			if err == nil {
				t.Errorf("expected error, got nil")
			}
			if kdfCalled {
				t.Errorf("KDF spy was invoked on invalid input %s", tc.name)
			}
		})
	}
}

func TestArgon2id_InvalidGenerationParamsNeverReachKDF(t *testing.T) {
	var kdfCalled bool
	originalKDF := argon2idKDF
	argon2idKDF = func(password, salt []byte, time, memory uint32, threads uint8, keyLen uint32) []byte {
		kdfCalled = true
		t.Fatalf("argon2idKDF was unexpectedly invoked during invalid generation")
		return nil
	}
	defer func() { argon2idKDF = originalKDF }()

	invalidParams := []struct {
		name   string
		params Argon2idParams
	}{
		{"version 16", Argon2idParams{Version: 16, Memory: 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}},
		{"excessive memory", Argon2idParams{Memory: 1048576, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}},
		{"excessive iterations", Argon2idParams{Memory: 1024, Iterations: 20, Parallelism: 1, SaltLength: 16, KeyLength: 32}},
		{"excessive parallelism", Argon2idParams{Memory: 1024, Iterations: 1, Parallelism: 32, SaltLength: 16, KeyLength: 32}},
		{"memory below 8*p", Argon2idParams{Memory: 4, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}},
		{"salt too short", Argon2idParams{Memory: 1024, Iterations: 1, Parallelism: 1, SaltLength: 4, KeyLength: 32}},
		{"key too short", Argon2idParams{Memory: 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 8}},
	}

	for _, tc := range invalidParams {
		t.Run(tc.name, func(t *testing.T) {
			kdfCalled = false
			_, err := HashPasswordWithParams("pass", tc.params)
			if err == nil {
				t.Errorf("expected error, got nil")
			}
			if kdfCalled {
				t.Errorf("KDF spy was invoked on invalid generation params %s", tc.name)
			}
		})
	}
}

func TestArgon2id_VersionNormalizationAndRejection(t *testing.T) {
	// 1. Version=0 must be normalized to argon2.Version (19) and successfully generate a valid hash.
	paramsWithZeroVersion := Argon2idParams{
		Version:     0,
		Memory:      1024,
		Iterations:  1,
		Parallelism: 1,
		SaltLength:  16,
		KeyLength:   32,
	}
	hash, err := HashPasswordWithParams("test-pass", paramsWithZeroVersion)
	if err != nil {
		t.Fatalf("expected Version=0 to normalize to argon2.Version, got error: %v", err)
	}
	if !strings.HasPrefix(hash, "$argon2id$v=19$") {
		t.Errorf("expected generated hash to have v=19, got: %s", hash)
	}

	// Verify the hash with normalized version verifies cleanly.
	match, err := VerifyPassword("test-pass", hash)
	if err != nil || !match {
		t.Fatalf("VerifyPassword failed on hash generated with Version=0: match=%v, err=%v", match, err)
	}

	// 2. Unsupported generation version (e.g. 16) must be rejected before hashing.
	paramsWithV16 := Argon2idParams{
		Version:     16,
		Memory:      1024,
		Iterations:  1,
		Parallelism: 1,
		SaltLength:  16,
		KeyLength:   32,
	}
	_, err = HashPasswordWithParams("test-pass", paramsWithV16)
	if err == nil {
		t.Fatal("expected HashPasswordWithParams to reject Version=16, got nil error")
	}
	if !errors.Is(err, ErrIncompatibleVersion) {
		t.Errorf("expected ErrIncompatibleVersion, got: %v", err)
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

	// Passing targetParams with Version=0 must normalize to 19 and NOT falsely report stale.
	targetWithZeroVersion := Argon2idParams{
		Memory:      64 * 1024,
		Iterations:  3,
		Parallelism: 4,
		SaltLength:  16,
		KeyLength:   32,
		Version:     0, // Uninitialized / default zero value
	}
	if NeedsRehash(encoded, targetWithZeroVersion) {
		t.Errorf("expected NeedsRehash=false when targetParams has Version=0, got true")
	}

	// Isolate salt length regression: use identical cost/version parameters between
	// hash and target, varying ONLY salt length, with an unchanged-target negative control.
	baseTarget := Argon2idParams{
		Memory:      1024,
		Iterations:  1,
		Parallelism: 1,
		SaltLength:  16,
		KeyLength:   32,
		Version:     argon2.Version,
	}
	normalHash, err := HashPasswordWithParams("test-password", baseTarget)
	if err != nil {
		t.Fatalf("HashPasswordWithParams failed: %v", err)
	}

	// Negative control: hash generated with baseTarget does NOT need rehash against baseTarget.
	if NeedsRehash(normalHash, baseTarget) {
		t.Errorf("expected NeedsRehash=false when hash matches baseTarget, got true")
	}

	// Positive test: identical in every field EXCEPT SaltLength=8.
	shortSaltParams := baseTarget
	shortSaltParams.SaltLength = 8
	shortSaltHash, err := HashPasswordWithParams("test-password", shortSaltParams)
	if err != nil {
		t.Fatalf("HashPasswordWithParams failed: %v", err)
	}

	// Against baseTarget (where SaltLength is 16), shortSaltHash MUST need rehash purely due to salt length.
	if !NeedsRehash(shortSaltHash, baseTarget) {
		t.Errorf("expected NeedsRehash=true for shortSaltHash (8B salt) against target (16B salt), got false")
	}

	// Negative control: against shortSaltParams itself (where SaltLength is 8), it does NOT need rehash.
	if NeedsRehash(shortSaltHash, shortSaltParams) {
		t.Errorf("expected NeedsRehash=false when target SaltLength matches (8B), got true")
	}

	// Also verify against DefaultArgon2idParams: short salt (8B) with identical production parameters.
	shortSaltProdParams := DefaultArgon2idParams
	shortSaltProdParams.SaltLength = 8
	shortSaltProdHash, err := HashPasswordWithParams("test-password", shortSaltProdParams)
	if err != nil {
		t.Fatalf("HashPasswordWithParams failed: %v", err)
	}
	if !NeedsRehash(shortSaltProdHash) {
		t.Errorf("expected NeedsRehash=true when only SaltLength differs from DefaultArgon2idParams, got false")
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
	// Salt length too short
	_, err := HashPasswordWithParams("pw", Argon2idParams{SaltLength: 4, KeyLength: 32, Memory: 1024, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for salt length < MinSaltLength, got nil")
	}

	// Salt length too long
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 128, KeyLength: 32, Memory: 1024, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for salt length > MaxSaltLength, got nil")
	}

	// Key length too short
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 8, Memory: 1024, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for key length < MinKeyLength, got nil")
	}

	// Key length too long
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 128, Memory: 1024, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for key length > MaxKeyLength, got nil")
	}

	// Memory below 8*p
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 32, Memory: 4, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for memory < 8*p, got nil")
	}

	// Memory above MaxMemory
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 32, Memory: 1048576, Iterations: 1, Parallelism: 1})
	if err == nil {
		t.Error("expected error for memory > MaxMemory, got nil")
	}

	// Iterations above MaxIterations
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 32, Memory: 1024, Iterations: 20, Parallelism: 1})
	if err == nil {
		t.Error("expected error for iterations > MaxIterations, got nil")
	}

	// Parallelism above MaxParallelism
	_, err = HashPasswordWithParams("pw", Argon2idParams{SaltLength: 16, KeyLength: 32, Memory: 1024, Iterations: 1, Parallelism: 32})
	if err == nil {
		t.Error("expected error for parallelism > MaxParallelism, got nil")
	}
}
