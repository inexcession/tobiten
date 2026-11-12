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

	// Bounded limits for Argon2id parameters to prevent DoS attacks.
	MinMemory         uint32 = 8          // 8 KiB (RFC 9106 requires >= 8*p KiB)
	MaxMemory         uint32 = 256 * 1024 // 256 MiB (65536 KiB default)
	MinIterations     uint32 = 1
	MaxIterations     uint32 = 10 // default is 3
	MinParallelism    uint8  = 1
	MaxParallelism    uint8  = 16 // default is 4
	MinSaltLength     uint32 = 8  // bytes (default is 16)
	MaxSaltLength     uint32 = 64 // bytes
	MinKeyLength      uint32 = 16 // bytes (default is 32)
	MaxKeyLength      uint32 = 64 // bytes
	MinEncodedHashLen        = 30
	MaxEncodedHashLen        = 512
)

// argon2idKDF points to the Argon2id key derivation function, defaulting to argon2.IDKey.
// It can be replaced in unit tests to deterministically verify that invalid inputs never trigger key derivation.
var argon2idKDF = argon2.IDKey

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
// If params.Version is 0, it normalizes to argon2.Version (19).
// Unsupported versions (e.g. 16) are rejected before hashing.
func HashPasswordWithParams(password string, params Argon2idParams) (string, error) {
	if params.Version == 0 {
		params.Version = argon2.Version
	}
	if params.Version != argon2.Version {
		return "", fmt.Errorf("%w: unsupported generation version %d (only %d is supported)", ErrIncompatibleVersion, params.Version, argon2.Version)
	}

	if params.Parallelism < MinParallelism || params.Parallelism > MaxParallelism {
		return "", fmt.Errorf("%w: parallelism %d out of bounds [%d, %d]", ErrInvalidHashFormat, params.Parallelism, MinParallelism, MaxParallelism)
	}
	minMem := 8 * uint32(params.Parallelism)
	if params.Memory < minMem || params.Memory > MaxMemory {
		return "", fmt.Errorf("%w: memory %d out of bounds [%d, %d]", ErrInvalidHashFormat, params.Memory, minMem, MaxMemory)
	}
	if params.Iterations < MinIterations || params.Iterations > MaxIterations {
		return "", fmt.Errorf("%w: iterations %d out of bounds [%d, %d]", ErrInvalidHashFormat, params.Iterations, MinIterations, MaxIterations)
	}
	if params.SaltLength < MinSaltLength || params.SaltLength > MaxSaltLength {
		return "", fmt.Errorf("%w: salt length %d out of bounds [%d, %d]", ErrInvalidHashFormat, params.SaltLength, MinSaltLength, MaxSaltLength)
	}
	if params.KeyLength < MinKeyLength || params.KeyLength > MaxKeyLength {
		return "", fmt.Errorf("%w: key length %d out of bounds [%d, %d]", ErrInvalidHashFormat, params.KeyLength, MinKeyLength, MaxKeyLength)
	}

	salt := make([]byte, params.SaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return "", fmt.Errorf("failed to generate random salt: %w", err)
	}

	key := argon2idKDF([]byte(password), salt, params.Iterations, params.Memory, params.Parallelism, params.KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Key := base64.RawStdEncoding.EncodeToString(key)

	encoded := fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s",
		AlgorithmArgon2id,
		params.Version,
		params.Memory,
		params.Iterations,
		params.Parallelism,
		b64Salt,
		b64Key,
	)

	return encoded, nil
}

// ParseArgon2idHash decomposes a PHC-formatted Argon2id string into its parameters, salt, and key.
// It applies strict, bounded validation before decoding base64 or allocating KDF buffers:
//
//	$argon2id$v=19$m=65536,t=3,p=4$<salt>$<key>
func ParseArgon2idHash(encodedHash string) (*Argon2idParams, []byte, []byte, error) {
	if len(encodedHash) < MinEncodedHashLen || len(encodedHash) > MaxEncodedHashLen {
		return nil, nil, nil, ErrInvalidHashFormat
	}

	parts := strings.Split(encodedHash, "$")
	// Expected parts: ["", "argon2id", "v=19", "m=65536,t=3,p=4", "<salt>", "<key>"]
	if len(parts) != 6 || parts[0] != "" {
		return nil, nil, nil, ErrInvalidHashFormat
	}

	if parts[1] != AlgorithmArgon2id {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, parts[1])
	}

	version, err := parseVersion(parts[2])
	if err != nil {
		return nil, nil, nil, err
	}

	memory, iterations, parallelism, err := parseCostParams(parts[3])
	if err != nil {
		return nil, nil, nil, err
	}

	salt, err := parseComponent(parts[4], int(MinSaltLength), int(MaxSaltLength), "salt")
	if err != nil {
		return nil, nil, nil, err
	}

	key, err := parseComponent(parts[5], int(MinKeyLength), int(MaxKeyLength), "key")
	if err != nil {
		return nil, nil, nil, err
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

func parseVersion(s string) (int, error) {
	if !strings.HasPrefix(s, "v=") {
		return 0, ErrInvalidHashFormat
	}
	verStr := strings.TrimPrefix(s, "v=")
	if verStr == "" {
		return 0, ErrInvalidHashFormat
	}
	for _, r := range verStr {
		if r < '0' || r > '9' {
			return 0, ErrInvalidHashFormat
		}
	}
	version, err := strconv.ParseUint(verStr, 10, 32)
	if err != nil {
		return 0, ErrInvalidHashFormat
	}
	if version != uint64(argon2.Version) {
		return 0, fmt.Errorf("%w: got %d, want %d", ErrIncompatibleVersion, version, argon2.Version)
	}
	return int(version), nil
}

func parseCostParams(s string) (uint32, uint32, uint8, error) {
	tuples := strings.Split(s, ",")
	if len(tuples) != 3 {
		return 0, 0, 0, ErrInvalidHashFormat
	}

	var (
		memory      uint32
		iterations  uint32
		parallelism uint8
		hasM, hasT  bool
		hasP        bool
	)

	for _, tuple := range tuples {
		kv := strings.Split(tuple, "=")
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return 0, 0, 0, ErrInvalidHashFormat
		}
		for _, r := range kv[1] {
			if r < '0' || r > '9' {
				return 0, 0, 0, ErrInvalidHashFormat
			}
		}
		val, parseErr := strconv.ParseUint(kv[1], 10, 32)
		if parseErr != nil {
			return 0, 0, 0, fmt.Errorf("%w: invalid parameter %s: %v", ErrInvalidHashFormat, kv[0], parseErr)
		}

		switch kv[0] {
		case "m":
			if hasM {
				return 0, 0, 0, fmt.Errorf("%w: duplicate parameter m", ErrInvalidHashFormat)
			}
			if val > uint64(MaxMemory) {
				return 0, 0, 0, fmt.Errorf("%w: memory %d exceeds maximum %d", ErrInvalidHashFormat, val, MaxMemory)
			}
			memory = uint32(val)
			hasM = true
		case "t":
			if hasT {
				return 0, 0, 0, fmt.Errorf("%w: duplicate parameter t", ErrInvalidHashFormat)
			}
			if val < uint64(MinIterations) || val > uint64(MaxIterations) {
				return 0, 0, 0, fmt.Errorf("%w: iterations %d out of bounds [%d, %d]", ErrInvalidHashFormat, val, MinIterations, MaxIterations)
			}
			iterations = uint32(val)
			hasT = true
		case "p":
			if hasP {
				return 0, 0, 0, fmt.Errorf("%w: duplicate parameter p", ErrInvalidHashFormat)
			}
			if val < uint64(MinParallelism) || val > uint64(MaxParallelism) {
				return 0, 0, 0, fmt.Errorf("%w: parallelism %d out of bounds [%d, %d]", ErrInvalidHashFormat, val, MinParallelism, MaxParallelism)
			}
			parallelism = uint8(val)
			hasP = true
		default:
			return 0, 0, 0, fmt.Errorf("%w: unknown cost parameter %q", ErrInvalidHashFormat, kv[0])
		}
	}

	if !hasM || !hasT || !hasP {
		return 0, 0, 0, ErrInvalidHashFormat
	}

	if memory < 8*uint32(parallelism) {
		return 0, 0, 0, fmt.Errorf("%w: memory %d below minimum %d for parallelism %d", ErrInvalidHashFormat, memory, 8*parallelism, parallelism)
	}

	return memory, iterations, parallelism, nil
}

func parseComponent(s string, minRaw, maxRaw int, name string) ([]byte, error) {
	minB64 := minBase64Len(minRaw)
	maxB64 := maxBase64Len(maxRaw)
	if len(s) < minB64 || len(s) > maxB64 {
		return nil, fmt.Errorf("%w: %s length out of bounds", ErrInvalidHashFormat, name)
	}

	b, err := decodeBase64(s)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid %s base64: %v", ErrInvalidHashFormat, name, err)
	}
	if len(b) < minRaw || len(b) > maxRaw {
		return nil, fmt.Errorf("%w: %s bytes out of bounds", ErrInvalidHashFormat, name)
	}
	return b, nil
}

// VerifyPassword verifies whether the plaintext password matches the encoded Argon2id hash.
// It returns (true, nil) on match, (false, nil) on mismatch, or (false, err) if the hash is malformed.
// It uses constant-time comparison (subtle.ConstantTimeCompare) to prevent timing side-channel attacks.
func VerifyPassword(password, encodedHash string) (bool, error) {
	params, salt, expectedKey, err := ParseArgon2idHash(encodedHash)
	if err != nil {
		return false, err
	}

	derivedKey := argon2idKDF(
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
// Target parameters are normalized: Version=0 defaults to argon2.Version,
// and SaltLength=0 or KeyLength=0 default to DefaultArgon2idParams.
func NeedsRehash(encodedHash string, targetParams ...Argon2idParams) bool {
	target := DefaultArgon2idParams
	if len(targetParams) > 0 {
		target = targetParams[0]
		if target.Version == 0 {
			target.Version = argon2.Version
		}
		if target.SaltLength == 0 {
			target.SaltLength = DefaultArgon2idParams.SaltLength
		}
		if target.KeyLength == 0 {
			target.KeyLength = DefaultArgon2idParams.KeyLength
		}
		if target.Memory == 0 {
			target.Memory = DefaultArgon2idParams.Memory
		}
		if target.Iterations == 0 {
			target.Iterations = DefaultArgon2idParams.Iterations
		}
		if target.Parallelism == 0 {
			target.Parallelism = DefaultArgon2idParams.Parallelism
		}
	}

	params, _, _, err := ParseArgon2idHash(encodedHash)
	if err != nil {
		return true
	}

	return params.Memory != target.Memory ||
		params.Iterations != target.Iterations ||
		params.Parallelism != target.Parallelism ||
		params.SaltLength != target.SaltLength ||
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

func minBase64Len(rawBytes int) int {
	return (rawBytes*8 + 5) / 6
}

func maxBase64Len(rawBytes int) int {
	return ((rawBytes + 2) / 3) * 4
}
