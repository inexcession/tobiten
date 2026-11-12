package storage

import (
	"context"
	"errors"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

var (
	// ErrNotFound is returned when a requested record does not exist in storage.
	ErrNotFound = errors.New("record not found")

	// ErrAlreadyExists is returned when a unique constraint or primary key violation occurs.
	ErrAlreadyExists = errors.New("record already exists")
)

// UserRepository defines the persistence interface for users, credentials, and roles.
// It returns internal/domain identity types exclusively (never sqlc-generated structs).
type UserRepository interface {
	// Users
	CreateUser(ctx context.Context, user *domain.User) error
	GetUserByID(ctx context.Context, id string) (*domain.User, error)
	GetUserByUsername(ctx context.Context, username string) (*domain.User, error)
	UpdateUser(ctx context.Context, user *domain.User) error
	DeleteUser(ctx context.Context, id string) error
	CountUsers(ctx context.Context) (int64, error)
	ListUsers(ctx context.Context) ([]domain.User, error)

	// Credentials
	CreateCredential(ctx context.Context, cred *domain.Credential) error
	GetCredentialByID(ctx context.Context, id string) (*domain.Credential, error)
	GetActivePasswordCredentialByUserID(ctx context.Context, userID string) (*domain.Credential, error)
	GetActiveOIDCCredentialBySubject(ctx context.Context, externalSubject string) (*domain.Credential, error)
	ListCredentialsByUserID(ctx context.Context, userID string) ([]domain.Credential, error)
	RevokeCredential(ctx context.Context, id string, revokedAt time.Time) error
	DeleteCredential(ctx context.Context, id string) error
	UpdateCredentialLastUsed(ctx context.Context, id string, lastUsedAt time.Time) error

	// Roles
	ListRoles(ctx context.Context) ([]domain.Role, error)
	GetRoleByID(ctx context.Context, id string) (*domain.Role, error)
	GetRoleByName(ctx context.Context, name string) (*domain.Role, error)
	AssignUserRole(ctx context.Context, userID, roleID string) error
	RemoveUserRole(ctx context.Context, userID, roleID string) error
	ListUserRoles(ctx context.Context, userID string) ([]domain.Role, error)
}

// SettingsRepository defines the persistence interface for the system_settings table (ADR-0014 §2).
type SettingsRepository interface {
	GetSetting(ctx context.Context, key string) (*domain.SystemSetting, error)
	SetSetting(ctx context.Context, setting *domain.SystemSetting) error
	DeleteSetting(ctx context.Context, key string) error
	ListSettings(ctx context.Context) ([]domain.SystemSetting, error)
	GetSettingsMap(ctx context.Context) (map[string]string, error)
}
