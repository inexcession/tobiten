package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Common errors for password hashing and verification.
var (
	ErrInvalidHashFormat    = errors.New("invalid argon2id hash format")
	ErrIncompatibleVersion  = errors.New("incompatible argon2id version")
	ErrUnsupportedAlgorithm = errors.New("unsupported password hash algorithm")
	ErrPasswordMismatch     = errors.New("password does not match")
)

const (
	// AlgorithmArgon2id is the identifier for the Argon2id key derivation function in PHC format.
	AlgorithmArgon2id = "argon2id"

	// HashSchemeArgon2id is the credential hash scheme string stored in credentials.hash_scheme (ADR-0006 §3, ADR-0018 S1).
	HashSchemeArgon2id = "argon2id"
)

// Argon2idParams holds the cost parameters for Argon2id password hashing (RFC 9106).
type Argon2idParams struct {
	// Memory is the memory cost parameter in KiB (e.g. 65536 for 64 MiB).
	Memory uint32
	// Iterations is the time cost parameter (number of passes over memory).
	Iterations uint32
	// Parallelism is the degree of parallelism (number of concurrent threads/lanes).
	Parallelism uint8
	// SaltLength is the length of the cryptographically random salt in bytes.
	SaltLength uint32
	// KeyLength is the length of the generated key digest in bytes.
	KeyLength uint32
	// Version is the Argon2 version (0x13 / 19 per RFC 9106).
	Version int
}

// DefaultArgon2idParams defines the production cost parameters for Tobiten passwords,
// following RFC 9106 §7.4 and OWASP recommendations for interactive password hashing.
//
// Documented parameters (ADR-0006 §3, ADR-0014 §1):
//   - Memory: 65536 KiB (64 MiB)
//   - Iterations (Time): 3 passes
//   - Parallelism: 4 threads
//   - SaltLength: 16 bytes (128-bit cryptographically secure random salt)
//   - KeyLength: 32 bytes (256-bit derived key digest)
//   - Version: 0x13 (19)
var DefaultArgon2idParams = Argon2idParams{
	Memory:      64 * 1024, // 65536 KiB = 64 MiB
	Iterations:  3,
	Parallelism: 4,
	SaltLength:  16,
	KeyLength:   32,
	Version:     argon2.Version,
}

// HashPassword hashes a plaintext password using Argon2id with DefaultArgon2idParams.
// It returns a standard PHC-formatted string:
//
//	$argon2id$v=19$m=65536,t=3,p=4$<base64-salt>$<base64-key>
func HashPassword(password string) (string, error) {
	return HashPasswordWithParams(password, DefaultArgon2idParams)
}

// HashPasswordWithParams hashes a plaintext password using Argon2id with custom parameters.
func HashPasswordWithParams(password string, params Argon2idParams) (string, error) {
	if params.SaltLength == 0 {
		return "", errors.New("salt length must be greater than 0")
	}
	if params.KeyLength == 0 {
		return "", errors.New("key length must be greater than 0")
	}
	if params.Memory == 0 || params.Iterations == 0 || params.Parallelism == 0 {
		return "", errors.New("memory, iterations, and parallelism must be greater than 0")
	}

	salt := make([]byte, params.SaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return "", fmt.Errorf("failed to generate random salt: %w", err)
	}

	key := argon2.IDKey([]byte(password), salt, params.Iterations, params.Memory, params.Parallelism, params.KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Key := base64.RawStdEncoding.EncodeToString(key)

	version := params.Version
	if version == 0 {
		version = argon2.Version
	}

	encoded := fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s",
		AlgorithmArgon2id,
		version,
		params.Memory,
		params.Iterations,
		params.Parallelism,
		b64Salt,
		b64Key,
	)

	return encoded, nil
}

// ParseArgon2idHash decomposes a PHC-formatted Argon2id string into its parameters, salt, and key.
// The expected format is:
//
//	$argon2id$v=19$m=65536,t=3,p=4$<salt>$<key>
func ParseArgon2idHash(encodedHash string) (*Argon2idParams, []byte, []byte, error) {
	parts := strings.Split(encodedHash, "$")
	// Expected parts: ["", "argon2id", "v=19", "m=65536,t=3,p=4", "<salt>", "<key>"]
	if len(parts) != 6 || parts[0] != "" {
		return nil, nil, nil, ErrInvalidHashFormat
	}

	if parts[1] != AlgorithmArgon2id {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, parts[1])
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %v", ErrInvalidHashFormat, err)
	}
	if version != argon2.Version {
		return nil, nil, nil, fmt.Errorf("%w: got %d, want %d", ErrIncompatibleVersion, version, argon2.Version)
	}

	var (
		memory      uint32
		iterations  uint32
		parallelism uint8
		hasM, hasT  bool
		hasP        bool
	)

	for _, param := range strings.Split(parts[3], ",") {
		kv := strings.SplitN(param, "=", 2)
		if len(kv) != 2 {
			return nil, nil, nil, ErrInvalidHashFormat
		}
		val, err := strconv.ParseUint(kv[1], 10, 32)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%w: invalid parameter %s: %v", ErrInvalidHashFormat, kv[0], err)
		}
		switch kv[0] {
		case "m":
			memory = uint32(val)
			hasM = true
		case "t":
			iterations = uint32(val)
			hasT = true
		case "p":
			if val == 0 || val > 255 {
				return nil, nil, nil, fmt.Errorf("%w: parallelism out of uint8 range: %d", ErrInvalidHashFormat, val)
			}
			parallelism = uint8(val)
			hasP = true
		default:
			return nil, nil, nil, fmt.Errorf("%w: unknown cost parameter %q", ErrInvalidHashFormat, kv[0])
		}
	}

	if !hasM || !hasT || !hasP || memory == 0 || iterations == 0 || parallelism == 0 {
		return nil, nil, nil, ErrInvalidHashFormat
	}

	salt, err := decodeBase64(parts[4])
	if err != nil || len(salt) == 0 {
		return nil, nil, nil, fmt.Errorf("%w: invalid salt base64: %v", ErrInvalidHashFormat, err)
	}

	key, err := decodeBase64(parts[5])
	if err != nil || len(key) == 0 {
		return nil, nil, nil, fmt.Errorf("%w: invalid key base64: %v", ErrInvalidHashFormat, err)
	}

	params := &Argon2idParams{
		Memory:      memory,
		Iterations:  iterations,
		Parallelism: parallelism,
		SaltLength:  uint32(len(salt)),
		KeyLength:   uint32(len(key)),
		Version:     version,
	}

	return params, salt, key, nil
}

// VerifyPassword verifies whether the plaintext password matches the encoded Argon2id hash.
// It returns (true, nil) on match, (false, nil) on mismatch, or (false, err) if the hash is malformed.
// It uses constant-time comparison (subtle.ConstantTimeCompare) to prevent timing side-channel attacks.
func VerifyPassword(password, encodedHash string) (bool, error) {
	params, salt, expectedKey, err := ParseArgon2idHash(encodedHash)
	if err != nil {
		return false, err
	}

	derivedKey := argon2.IDKey(
		[]byte(password),
		salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		uint32(len(expectedKey)),
	)

	if subtle.ConstantTimeCompare(derivedKey, expectedKey) == 1 {
		return true, nil
	}
	return false, nil
}

// ComparePassword compares a plaintext password with an encoded hash.
// It returns nil on match, ErrPasswordMismatch on mismatch, or another error if the hash is invalid.
func ComparePassword(password, encodedHash string) error {
	match, err := VerifyPassword(password, encodedHash)
	if err != nil {
		return err
	}
	if !match {
		return ErrPasswordMismatch
	}
	return nil
}

// NeedsRehash reports whether the encoded hash was created with parameters different
// from targetParams (or DefaultArgon2idParams if targetParams is omitted).
func NeedsRehash(encodedHash string, targetParams ...Argon2idParams) bool {
	target := DefaultArgon2idParams
	if len(targetParams) > 0 {
		target = targetParams[0]
	}

	params, _, _, err := ParseArgon2idHash(encodedHash)
	if err != nil {
		return true
	}

	return params.Memory != target.Memory ||
		params.Iterations != target.Iterations ||
		params.Parallelism != target.Parallelism ||
		params.KeyLength != target.KeyLength ||
		params.Version != target.Version
}

// decodeBase64 decodes base64 string attempting unpadded raw standard encoding first,
// then falling back to padded standard encoding.
func decodeBase64(s string) ([]byte, error) {
	b, err := base64.RawStdEncoding.DecodeString(s)
	if err == nil {
		return b, nil
	}
	return base64.StdEncoding.DecodeString(s)
}
