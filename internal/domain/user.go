package domain

import (
	"errors"
	"slices"
	"time"
)

// Common domain errors for identity types.
var (
	ErrEmptyUsername         = errors.New("username cannot be empty")
	ErrEmptyUserID           = errors.New("user ID cannot be empty")
	ErrEmptyScopesForPAT     = errors.New("a PAT must name at least one scope")
	ErrInvalidCredentialKind = errors.New("invalid credential kind")
	ErrInvalidPATScope       = errors.New("invalid PAT scope")
	ErrInvalidPermission     = errors.New("invalid permission")
)

// User represents a named user account (ADR-0006 §2).
type User struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Validate checks User invariants.
func (u *User) Validate() error {
	if u.ID == "" {
		return ErrEmptyUserID
	}
	if u.Username == "" {
		return ErrEmptyUsername
	}
	return nil
}

// CredentialKind represents the kind of credential stored in credentials table (ADR-0006 §3, ADR-0018 S1).
type CredentialKind string

const (
	CredentialKindPassword CredentialKind = "PASSWORD"
	CredentialKindPAT      CredentialKind = "PAT"
	CredentialKindOIDCLink CredentialKind = "OIDC_LINK"
)

// ValidCredentialKinds returns all 1.0 valid credential kinds.
func ValidCredentialKinds() []CredentialKind {
	return []CredentialKind{
		CredentialKindPassword,
		CredentialKindPAT,
		CredentialKindOIDCLink,
	}
}

// IsValid reports whether the credential kind is recognized.
func (k CredentialKind) IsValid() bool {
	return slices.Contains(ValidCredentialKinds(), k)
}

// Credential represents an authentication credential entry (ADR-0006 §3).
type Credential struct {
	ID              string         `json:"id"`
	UserID          string         `json:"user_id"`
	Kind            CredentialKind `json:"kind"`
	Scopes          []PATScope     `json:"scopes"`
	SecretHash      *string        `json:"-"`
	HashScheme      *string        `json:"-"`
	ExternalSubject *string        `json:"external_subject,omitempty"`
	Label           *string        `json:"label,omitempty"`
	CreatedAt       time.Time      `json:"created_at"`
	LastUsedAt      *time.Time     `json:"last_used_at,omitempty"`
	ExpiresAt       *time.Time     `json:"expires_at,omitempty"`
	RevokedAt       *time.Time     `json:"revoked_at,omitempty"`
}

// Validate checks Credential invariants matching database constraints.
func (c *Credential) Validate() error {
	if c.ID == "" {
		return errors.New("credential ID cannot be empty")
	}
	if c.UserID == "" {
		return ErrEmptyUserID
	}
	if !c.Kind.IsValid() {
		return ErrInvalidCredentialKind
	}
	if c.Kind == CredentialKindPAT && len(c.Scopes) == 0 {
		return ErrEmptyScopesForPAT
	}
	for _, scope := range c.Scopes {
		if !scope.IsValid() {
			return ErrInvalidPATScope
		}
	}
	return nil
}

// IsRevoked reports whether the credential has been revoked.
func (c *Credential) IsRevoked() bool {
	return c.RevokedAt != nil && !c.RevokedAt.IsZero()
}

// IsExpired reports whether the credential has expired at the given time.
func (c *Credential) IsExpired(at time.Time) bool {
	if c.ExpiresAt == nil || c.ExpiresAt.IsZero() {
		return false
	}
	return at.After(*c.ExpiresAt)
}

// PATScope represents a Personal Access Token scope (ADR-0006 §3).
type PATScope string

const (
	ScopeRead          PATScope = "read"
	ScopeProgressWrite PATScope = "progress:write"
	ScopeMetadataWrite PATScope = "metadata:write"
	ScopeLibraryAdmin  PATScope = "library:admin"
	ScopeAdmin         PATScope = "admin"
)

// AllScopes returns all defined PAT scopes in canonical order.
func AllScopes() []PATScope {
	return []PATScope{
		ScopeRead,
		ScopeProgressWrite,
		ScopeMetadataWrite,
		ScopeLibraryAdmin,
		ScopeAdmin,
	}
}

// IsValid reports whether the scope is a recognized PATScope.
func (s PATScope) IsValid() bool {
	return slices.Contains(AllScopes(), s)
}

// Permission represents a granular permission in the Tobiten permission set (ADR-0006 §4).
type Permission string

const (
	PermissionLibraryRead       Permission = "library:read"
	PermissionProgressWrite     Permission = "progress:write"
	PermissionMetadataEdit      Permission = "metadata:edit"
	PermissionCollectionsManage Permission = "collections:manage"
	PermissionLibraryAdmin      Permission = "library:admin"
	PermissionUsersAdmin        Permission = "users:admin"
	// Administrator-only permissions (implementation decision I-3, 2026-11-10)
	PermissionJobsReadAll Permission = "jobs:read_all"
	PermissionTrashPurge  Permission = "trash:purge"
)

// AllPermissions returns all defined permissions in canonical order.
func AllPermissions() []Permission {
	return []Permission{
		PermissionLibraryRead,
		PermissionProgressWrite,
		PermissionMetadataEdit,
		PermissionCollectionsManage,
		PermissionLibraryAdmin,
		PermissionUsersAdmin,
		PermissionJobsReadAll,
		PermissionTrashPurge,
	}
}

// IsValid reports whether the permission is a recognized Permission.
func (p Permission) IsValid() bool {
	return slices.Contains(AllPermissions(), p)
}

// ScopeToPermissions returns the fixed set of role permissions mapped to a PAT scope
// (ADR-0006 §3, decision I-2).
//
// Mappings:
//   - read          -> library:read
//   - progress:write-> library:read, progress:write
//   - metadata:write-> library:read, metadata:edit
//   - library:admin -> library:read, library:admin
//   - admin         -> every permission
func ScopeToPermissions(scope PATScope) []Permission {
	switch scope {
	case ScopeRead:
		return []Permission{PermissionLibraryRead}
	case ScopeProgressWrite:
		return []Permission{PermissionLibraryRead, PermissionProgressWrite}
	case ScopeMetadataWrite:
		return []Permission{PermissionLibraryRead, PermissionMetadataEdit}
	case ScopeLibraryAdmin:
		return []Permission{PermissionLibraryRead, PermissionLibraryAdmin}
	case ScopeAdmin:
		return AllPermissions()
	default:
		return nil
	}
}

// ScopesToPermissions computes the deduplicated union of permissions granted by the given scopes.
func ScopesToPermissions(scopes []PATScope) []Permission {
	seen := make(map[Permission]struct{})
	for _, scope := range scopes {
		for _, perm := range ScopeToPermissions(scope) {
			seen[perm] = struct{}{}
		}
	}
	result := make([]Permission, 0, len(seen))
	for _, perm := range AllPermissions() {
		if _, ok := seen[perm]; ok {
			result = append(result, perm)
		}
	}
	return result
}

// Role represents a named permission set (ADR-0006 §4).
type Role struct {
	ID                  string       `json:"id"`
	Name                string       `json:"name"`
	BuiltIn             bool         `json:"built_in"`
	RequiresAllowLabels bool         `json:"requires_allow_labels"`
	Permissions         []Permission `json:"permissions"`
}

// Validate checks Role invariants.
func (r *Role) Validate() error {
	if r.ID == "" {
		return errors.New("role ID cannot be empty")
	}
	if r.Name == "" {
		return errors.New("role name cannot be empty")
	}
	for _, p := range r.Permissions {
		if !p.IsValid() {
			return ErrInvalidPermission
		}
	}
	return nil
}

// Built-in role identifiers and names (ADR-0006 §4; 0001_init.sql).
const (
	RoleAdminID            = "019318a0-0000-7000-8000-000000000001"
	RoleCuratorID          = "019318a0-0000-7000-8000-000000000002"
	RoleReaderID           = "019318a0-0000-7000-8000-000000000003"
	RoleRestrictedReaderID = "019318a0-0000-7000-8000-000000000004"

	RoleAdminName            = "Admin"
	RoleCuratorName          = "Curator"
	RoleReaderName           = "Reader"
	RoleRestrictedReaderName = "Restricted reader"
)

// BuiltInRoles returns the default seeded roles in the system.
func BuiltInRoles() []Role {
	return []Role{
		{
			ID:                  RoleAdminID,
			Name:                RoleAdminName,
			BuiltIn:             true,
			RequiresAllowLabels: false,
			Permissions: []Permission{
				PermissionLibraryRead,
				PermissionProgressWrite,
				PermissionMetadataEdit,
				PermissionCollectionsManage,
				PermissionLibraryAdmin,
				PermissionUsersAdmin,
				PermissionJobsReadAll,
				PermissionTrashPurge,
			},
		},
		{
			ID:                  RoleCuratorID,
			Name:                RoleCuratorName,
			BuiltIn:             true,
			RequiresAllowLabels: false,
			Permissions: []Permission{
				PermissionLibraryRead,
				PermissionProgressWrite,
				PermissionMetadataEdit,
				PermissionCollectionsManage,
			},
		},
		{
			ID:                  RoleReaderID,
			Name:                RoleReaderName,
			BuiltIn:             true,
			RequiresAllowLabels: false,
			Permissions: []Permission{
				PermissionLibraryRead,
				PermissionProgressWrite,
			},
		},
		{
			ID:                  RoleRestrictedReaderID,
			Name:                RoleRestrictedReaderName,
			BuiltIn:             true,
			RequiresAllowLabels: true,
			Permissions: []Permission{
				PermissionLibraryRead,
				PermissionProgressWrite,
			},
		},
	}
}

// BuiltInRoleByName returns a built-in role by its name.
func BuiltInRoleByName(name string) (Role, bool) {
	for _, r := range BuiltInRoles() {
		if r.Name == name {
			return r, true
		}
	}
	return Role{}, false
}

// BuiltInRoleByID returns a built-in role by its ID.
func BuiltInRoleByID(id string) (Role, bool) {
	for _, r := range BuiltInRoles() {
		if r.ID == id {
			return r, true
		}
	}
	return Role{}, false
}

// CalculateEffectivePermissions calculates the effective permissions resulting from
// intersecting the role permissions with mapped scopes if scoped, or full role permissions
// if not scoped (ADR-0006 §3, decision I-2).
//
// Rule:
//   - If isScoped is true (e.g. PAT credential):
//     effective = rolePermissions ∩ (union of ScopeToPermissions(s))
//   - If isScoped is false (e.g. PASSWORD, OIDC):
//     effective = rolePermissions
func CalculateEffectivePermissions(rolePermissions []Permission, scopes []PATScope, isScoped bool) []Permission {
	roleSet := make(map[Permission]struct{}, len(rolePermissions))
	for _, p := range rolePermissions {
		roleSet[p] = struct{}{}
	}

	if !isScoped {
		result := make([]Permission, 0, len(roleSet))
		for _, p := range AllPermissions() {
			if _, ok := roleSet[p]; ok {
				result = append(result, p)
			}
		}
		return result
	}

	// For scoped credentials (PAT): effective = rolePermissions ∩ union(scopes)
	scopePerms := ScopesToPermissions(scopes)
	result := make([]Permission, 0, len(scopePerms))
	for _, p := range scopePerms {
		if _, ok := roleSet[p]; ok {
			result = append(result, p)
		}
	}
	return result
}

// Principal represents the resolved identity and authorization context for an authenticated request
// (ADR-0006 §3, ADR-0018 S1). Core application services only ever see the Principal.
type Principal struct {
	User           User           `json:"user"`
	Roles          []Role         `json:"roles"`
	CredentialID   string         `json:"credential_id"`
	CredentialKind CredentialKind `json:"credential_kind"`
	Scopes         []PATScope     `json:"scopes"`
	Permissions    []Permission   `json:"permissions"`
}

// NewPrincipal constructs a Principal and computes effective permissions via scope-cap arithmetic.
// Only PASSWORD and OIDC_LINK are unscoped; every other kind (PAT and any future kind) goes through
// scope-cap arithmetic, so an unknown kind with no scopes gets no permissions.
func NewPrincipal(user User, roles []Role, credKind CredentialKind, credID string, scopes []PATScope) *Principal {
	var allRolePerms []Permission
	for _, r := range roles {
		allRolePerms = append(allRolePerms, r.Permissions...)
	}

	isScoped := credKind != CredentialKindPassword && credKind != CredentialKindOIDCLink
	effectivePerms := CalculateEffectivePermissions(allRolePerms, scopes, isScoped)

	return &Principal{
		User:           user,
		Roles:          roles,
		CredentialID:   credID,
		CredentialKind: credKind,
		Scopes:         scopes,
		Permissions:    effectivePerms,
	}
}

// RequiresAllowLabels reports whether any of the principal's roles requires mandatory
// allow labels (fail closed when no ALLOW rows exist; ADR-0006 §4–5, decision I-5).
func (p *Principal) RequiresAllowLabels() bool {
	for _, r := range p.Roles {
		if r.RequiresAllowLabels {
			return true
		}
	}
	return false
}

// HasPermission reports whether the principal holds the specified permission.
func (p *Principal) HasPermission(perm Permission) bool {
	return slices.Contains(p.Permissions, perm)
}

// HasAllPermissions reports whether the principal holds all of the specified permissions.
func (p *Principal) HasAllPermissions(perms ...Permission) bool {
	for _, perm := range perms {
		if !p.HasPermission(perm) {
			return false
		}
	}
	return true
}

// HasAnyPermission reports whether the principal holds at least one of the specified permissions.
func (p *Principal) HasAnyPermission(perms ...Permission) bool {
	for _, perm := range perms {
		if p.HasPermission(perm) {
			return true
		}
	}
	return false
}
