package domain_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestScopeToPermissions(t *testing.T) {
	tests := []struct {
		name     string
		scope    domain.PATScope
		expected []domain.Permission
	}{
		{
			name:     "read scope",
			scope:    domain.ScopeRead,
			expected: []domain.Permission{domain.PermissionLibraryRead},
		},
		{
			name:  "progress:write scope",
			scope: domain.ScopeProgressWrite,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionProgressWrite,
			},
		},
		{
			name:  "metadata:write scope",
			scope: domain.ScopeMetadataWrite,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionMetadataEdit,
			},
		},
		{
			name:  "library:admin scope",
			scope: domain.ScopeLibraryAdmin,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionLibraryAdmin,
			},
		},
		{
			name:  "admin scope grants all permissions",
			scope: domain.ScopeAdmin,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionProgressWrite,
				domain.PermissionMetadataEdit,
				domain.PermissionCollectionsManage,
				domain.PermissionLibraryAdmin,
				domain.PermissionUsersAdmin,
				domain.PermissionJobsReadAll,
				domain.PermissionTrashPurge,
			},
		},
		{
			name:     "invalid scope returns nil",
			scope:    domain.PATScope("unknown"),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.ScopeToPermissions(tt.scope)
			if !slices.Equal(got, tt.expected) {
				t.Errorf("ScopeToPermissions(%q) = %v, want %v", tt.scope, got, tt.expected)
			}
		})
	}
}

func TestScopesToPermissions(t *testing.T) {
	t.Run("empty scopes", func(t *testing.T) {
		got := domain.ScopesToPermissions(nil)
		if len(got) != 0 {
			t.Errorf("expected empty permissions, got %v", got)
		}
	})

	t.Run("deduplicated union of multiple scopes", func(t *testing.T) {
		scopes := []domain.PATScope{
			domain.ScopeRead,
			domain.ScopeProgressWrite,
			domain.ScopeMetadataWrite,
		}
		got := domain.ScopesToPermissions(scopes)
		expected := []domain.Permission{
			domain.PermissionLibraryRead,
			domain.PermissionProgressWrite,
			domain.PermissionMetadataEdit,
		}
		if !slices.Equal(got, expected) {
			t.Errorf("ScopesToPermissions(%v) = %v, want %v", scopes, got, expected)
		}
	})
}

func TestScopeCapArithmetic(t *testing.T) {
	adminRole, ok := domain.BuiltInRoleByName(domain.RoleAdminName)
	if !ok {
		t.Fatalf("Admin role not found")
	}
	curatorRole, ok := domain.BuiltInRoleByName(domain.RoleCuratorName)
	if !ok {
		t.Fatalf("Curator role not found")
	}
	readerRole, ok := domain.BuiltInRoleByName(domain.RoleReaderName)
	if !ok {
		t.Fatalf("Reader role not found")
	}
	restrictedReaderRole, ok := domain.BuiltInRoleByName(domain.RoleRestrictedReaderName)
	if !ok {
		t.Fatalf("Restricted reader role not found")
	}

	tests := []struct {
		name       string
		rolePerms  []domain.Permission
		scopes     []domain.PATScope
		isScoped   bool
		expected   []domain.Permission
		wantDenied []domain.Permission
	}{
		{
			name:      "PASSWORD credential gets full role permissions without cap",
			rolePerms: adminRole.Permissions,
			scopes:    nil,
			isScoped:  false,
			expected:  adminRole.Permissions,
		},
		{
			name:      "Admin user with read PAT is capped to library:read only",
			rolePerms: adminRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeRead},
			isScoped:  true,
			expected:  []domain.Permission{domain.PermissionLibraryRead},
			wantDenied: []domain.Permission{
				domain.PermissionProgressWrite,
				domain.PermissionMetadataEdit,
				domain.PermissionLibraryAdmin,
				domain.PermissionUsersAdmin,
				domain.PermissionJobsReadAll,
				domain.PermissionTrashPurge,
			},
		},
		{
			name:      "Admin user with progress:write PAT gets library:read and progress:write",
			rolePerms: adminRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeProgressWrite},
			isScoped:  true,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionProgressWrite,
			},
			wantDenied: []domain.Permission{
				domain.PermissionMetadataEdit,
				domain.PermissionLibraryAdmin,
			},
		},
		{
			name:      "Admin user with metadata:write PAT gets library:read and metadata:edit",
			rolePerms: adminRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeMetadataWrite},
			isScoped:  true,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionMetadataEdit,
			},
			wantDenied: []domain.Permission{
				domain.PermissionProgressWrite,
				domain.PermissionLibraryAdmin,
			},
		},
		{
			name:      "Admin user with library:admin PAT gets library:read and library:admin",
			rolePerms: adminRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeLibraryAdmin},
			isScoped:  true,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionLibraryAdmin,
			},
			wantDenied: []domain.Permission{
				domain.PermissionMetadataEdit,
				domain.PermissionUsersAdmin,
				domain.PermissionTrashPurge,
			},
		},
		{
			name:      "Admin user with admin PAT gets all permissions",
			rolePerms: adminRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeAdmin},
			isScoped:  true,
			expected:  adminRole.Permissions,
		},
		{
			name:      "Curator user with admin PAT is capped by Curator role",
			rolePerms: curatorRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeAdmin},
			isScoped:  true,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionProgressWrite,
				domain.PermissionMetadataEdit,
				domain.PermissionCollectionsManage,
			},
			wantDenied: []domain.Permission{
				domain.PermissionLibraryAdmin,
				domain.PermissionUsersAdmin,
				domain.PermissionJobsReadAll,
				domain.PermissionTrashPurge,
			},
		},
		{
			name:      "Curator user with library:admin PAT is capped to library:read",
			rolePerms: curatorRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeLibraryAdmin},
			isScoped:  true,
			expected:  []domain.Permission{domain.PermissionLibraryRead},
			wantDenied: []domain.Permission{
				domain.PermissionLibraryAdmin,
				domain.PermissionMetadataEdit,
			},
		},
		{
			name:      "Reader user with metadata:write PAT is capped to library:read (M1-AC22)",
			rolePerms: readerRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeMetadataWrite},
			isScoped:  true,
			expected:  []domain.Permission{domain.PermissionLibraryRead},
			wantDenied: []domain.Permission{
				domain.PermissionMetadataEdit,
				domain.PermissionProgressWrite,
			},
		},
		{
			name:      "Reader user with admin PAT is capped to Reader role",
			rolePerms: readerRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeAdmin},
			isScoped:  true,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionProgressWrite,
			},
			wantDenied: []domain.Permission{
				domain.PermissionMetadataEdit,
				domain.PermissionCollectionsManage,
				domain.PermissionLibraryAdmin,
				domain.PermissionUsersAdmin,
			},
		},
		{
			name:      "Restricted reader user with admin PAT is capped to Reader permissions",
			rolePerms: restrictedReaderRole.Permissions,
			scopes:    []domain.PATScope{domain.ScopeAdmin},
			isScoped:  true,
			expected: []domain.Permission{
				domain.PermissionLibraryRead,
				domain.PermissionProgressWrite,
			},
			wantDenied: []domain.Permission{
				domain.PermissionMetadataEdit,
				domain.PermissionLibraryAdmin,
			},
		},
		{
			name:      "User with no roles gets empty effective permissions even with admin PAT",
			rolePerms: nil,
			scopes:    []domain.PATScope{domain.ScopeAdmin},
			isScoped:  true,
			expected:  []domain.Permission{},
		},
		{
			name:      "PAT with empty scopes gets empty effective permissions",
			rolePerms: adminRole.Permissions,
			scopes:    []domain.PATScope{},
			isScoped:  true,
			expected:  []domain.Permission{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.CalculateEffectivePermissions(tt.rolePerms, tt.scopes, tt.isScoped)
			if !slices.Equal(got, tt.expected) {
				t.Fatalf("CalculateEffectivePermissions() = %v, want %v", got, tt.expected)
			}
			for _, denied := range tt.wantDenied {
				if slices.Contains(got, denied) {
					t.Errorf("expected permission %q to be denied, but it was present in %v", denied, got)
				}
			}
		})
	}
}

func TestPrincipalConstructionAndPermissionChecks(t *testing.T) {
	adminRole, _ := domain.BuiltInRoleByName(domain.RoleAdminName)
	readerRole, _ := domain.BuiltInRoleByName(domain.RoleReaderName)
	restrictedRole, _ := domain.BuiltInRoleByName(domain.RoleRestrictedReaderName)

	user := domain.User{
		ID:        "019318a0-0000-7000-8000-000000000010",
		Username:  "alice",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	t.Run("password principal holds full role permissions", func(t *testing.T) {
		p := domain.NewPrincipal(user, []domain.Role{adminRole}, domain.CredentialKindPassword, "cred_1", nil)

		if !p.HasPermission(domain.PermissionLibraryRead) {
			t.Errorf("expected principal to have library:read")
		}
		if !p.HasPermission(domain.PermissionJobsReadAll) {
			t.Errorf("expected admin principal to have jobs:read_all")
		}
		if !p.HasPermission(domain.PermissionTrashPurge) {
			t.Errorf("expected admin principal to have trash:purge")
		}
		if !p.HasAllPermissions(domain.PermissionLibraryRead, domain.PermissionTrashPurge) {
			t.Errorf("expected principal to have all admin permissions")
		}
		if p.RequiresAllowLabels() {
			t.Errorf("admin principal should not require allow labels")
		}
	})

	t.Run("OIDC_LINK principal is unscoped like password", func(t *testing.T) {
		p := domain.NewPrincipal(user, []domain.Role{adminRole}, domain.CredentialKindOIDCLink, "cred_oidc", nil)

		if !p.HasPermission(domain.PermissionLibraryRead) {
			t.Errorf("expected OIDC principal to have library:read")
		}
		if !p.HasPermission(domain.PermissionJobsReadAll) {
			t.Errorf("expected OIDC principal to have jobs:read_all")
		}
	})

	t.Run("unknown credential kind goes through scope-cap arithmetic and gets no permissions with no scopes", func(t *testing.T) {
		unknownKind := domain.CredentialKind("UNKNOWN_FUTURE_KIND")
		p := domain.NewPrincipal(user, []domain.Role{adminRole}, unknownKind, "cred_unk", nil)

		if len(p.Permissions) != 0 {
			t.Errorf("unknown credential kind with no scopes must receive 0 permissions, got %v", p.Permissions)
		}
	})

	t.Run("PAT principal is capped by scopes", func(t *testing.T) {
		p := domain.NewPrincipal(
			user,
			[]domain.Role{adminRole},
			domain.CredentialKindPAT,
			"pat_1",
			[]domain.PATScope{domain.ScopeRead},
		)

		if !p.HasPermission(domain.PermissionLibraryRead) {
			t.Errorf("expected read PAT to have library:read")
		}
		if p.HasPermission(domain.PermissionProgressWrite) {
			t.Errorf("read PAT must NOT have progress:write")
		}
		if p.HasPermission(domain.PermissionTrashPurge) {
			t.Errorf("read PAT must NOT have trash:purge")
		}
		if p.HasAllPermissions(domain.PermissionLibraryRead, domain.PermissionProgressWrite) {
			t.Errorf("HasAllPermissions should be false when missing progress:write")
		}
		if !p.HasAnyPermission(domain.PermissionLibraryRead, domain.PermissionTrashPurge) {
			t.Errorf("HasAnyPermission should be true when library:read is present")
		}
	})

	t.Run("reader principal cannot exceed reader permissions even with admin PAT", func(t *testing.T) {
		p := domain.NewPrincipal(
			user,
			[]domain.Role{readerRole},
			domain.CredentialKindPAT,
			"pat_2",
			[]domain.PATScope{domain.ScopeAdmin},
		)

		if !p.HasPermission(domain.PermissionLibraryRead) {
			t.Errorf("expected reader to have library:read")
		}
		if !p.HasPermission(domain.PermissionProgressWrite) {
			t.Errorf("expected reader to have progress:write")
		}
		if p.HasPermission(domain.PermissionMetadataEdit) {
			t.Errorf("reader must NOT have metadata:edit")
		}
		if p.HasPermission(domain.PermissionJobsReadAll) {
			t.Errorf("reader must NOT have jobs:read_all")
		}
		if p.RequiresAllowLabels() {
			t.Errorf("reader principal should not require allow labels")
		}
	})

	t.Run("restricted reader principal requires allow labels", func(t *testing.T) {
		p := domain.NewPrincipal(
			user,
			[]domain.Role{restrictedRole},
			domain.CredentialKindPassword,
			"cred_restr",
			nil,
		)

		if !p.RequiresAllowLabels() {
			t.Errorf("restricted reader principal must require allow labels (decision I-5)")
		}
	})
}

func TestCredentialRevocationAndExpiry(t *testing.T) {
	now := time.Now()
	past := now.Add(-1 * time.Hour)
	future := now.Add(1 * time.Hour)

	t.Run("active credential", func(t *testing.T) {
		cred := domain.Credential{
			ID:        "cred_1",
			UserID:    "user_1",
			Kind:      domain.CredentialKindPassword,
			CreatedAt: past,
		}
		if cred.IsRevoked() {
			t.Errorf("expected active credential not to be revoked")
		}
		if cred.IsExpired(now) {
			t.Errorf("expected active credential not to be expired")
		}
	})

	t.Run("revoked credential", func(t *testing.T) {
		cred := domain.Credential{
			ID:        "cred_2",
			UserID:    "user_1",
			Kind:      domain.CredentialKindPAT,
			RevokedAt: &past,
		}
		if !cred.IsRevoked() {
			t.Errorf("expected revoked credential to report IsRevoked=true")
		}
	})

	t.Run("expired credential", func(t *testing.T) {
		cred := domain.Credential{
			ID:        "cred_3",
			UserID:    "user_1",
			Kind:      domain.CredentialKindPAT,
			ExpiresAt: &past,
		}
		if !cred.IsExpired(now) {
			t.Errorf("expected past expires_at credential to report IsExpired=true")
		}
	})

	t.Run("unexpired credential with future expiry", func(t *testing.T) {
		cred := domain.Credential{
			ID:        "cred_4",
			UserID:    "user_1",
			Kind:      domain.CredentialKindPAT,
			ExpiresAt: &future,
		}
		if cred.IsExpired(now) {
			t.Errorf("expected future expires_at credential to report IsExpired=false")
		}
	})
}

func TestCredentialJSONExcludesSensitiveFields(t *testing.T) {
	hash := "$argon2id$v=19$m=65536,t=3,p=4$secret"
	scheme := "argon2id"
	cred := domain.Credential{
		ID:         "cred_sec",
		UserID:     "user_sec",
		Kind:       domain.CredentialKindPassword,
		SecretHash: &hash,
		HashScheme: &scheme,
		CreatedAt:  time.Now(),
	}

	bytes, err := json.Marshal(cred)
	if err != nil {
		t.Fatalf("failed to marshal credential: %v", err)
	}

	jsonStr := string(bytes)
	if strings.Contains(jsonStr, "secret_hash") || strings.Contains(jsonStr, "SecretHash") || strings.Contains(jsonStr, hash) {
		t.Errorf("secret_hash must be excluded from JSON, got: %s", jsonStr)
	}
	if strings.Contains(jsonStr, "hash_scheme") || strings.Contains(jsonStr, "HashScheme") || strings.Contains(jsonStr, scheme) {
		t.Errorf("hash_scheme must be excluded from JSON, got: %s", jsonStr)
	}
}

func TestBuiltInRolesExactMatching(t *testing.T) {
	roles := domain.BuiltInRoles()
	if len(roles) != 4 {
		t.Fatalf("expected 4 built-in roles, got %d", len(roles))
	}

	admin, ok := domain.BuiltInRoleByID(domain.RoleAdminID)
	if !ok || admin.Name != domain.RoleAdminName {
		t.Fatalf("failed to find built-in Admin role by ID %s", domain.RoleAdminID)
	}
	if admin.RequiresAllowLabels {
		t.Errorf("Admin role must have RequiresAllowLabels=false")
	}
	if !slices.Contains(admin.Permissions, domain.PermissionJobsReadAll) ||
		!slices.Contains(admin.Permissions, domain.PermissionTrashPurge) {
		t.Errorf("Admin role missing administrator-only permissions (decision I-3)")
	}

	curator, ok := domain.BuiltInRoleByID(domain.RoleCuratorID)
	if !ok || curator.Name != domain.RoleCuratorName {
		t.Fatalf("failed to find built-in Curator role by ID %s", domain.RoleCuratorID)
	}
	if curator.RequiresAllowLabels {
		t.Errorf("Curator role must have RequiresAllowLabels=false")
	}

	reader, ok := domain.BuiltInRoleByID(domain.RoleReaderID)
	if !ok || reader.Name != domain.RoleReaderName {
		t.Fatalf("failed to find built-in Reader role by ID %s", domain.RoleReaderID)
	}
	if reader.RequiresAllowLabels {
		t.Errorf("Reader role must have RequiresAllowLabels=false")
	}

	restricted, ok := domain.BuiltInRoleByID(domain.RoleRestrictedReaderID)
	if !ok || restricted.Name != domain.RoleRestrictedReaderName {
		t.Fatalf("failed to find built-in Restricted reader role by ID %s", domain.RoleRestrictedReaderID)
	}
	if !restricted.RequiresAllowLabels {
		t.Errorf("Restricted reader role must have RequiresAllowLabels=true (decision I-5)")
	}
}

func TestValidationFunctions(t *testing.T) {
	t.Run("User.Validate", func(t *testing.T) {
		u := domain.User{}
		if !errors.Is(u.Validate(), domain.ErrEmptyUserID) {
			t.Errorf("expected ErrEmptyUserID, got %v", u.Validate())
		}
		u.ID = "u1"
		if !errors.Is(u.Validate(), domain.ErrEmptyUsername) {
			t.Errorf("expected ErrEmptyUsername, got %v", u.Validate())
		}
		u.Username = "alice"
		if err := u.Validate(); err != nil {
			t.Errorf("expected valid user, got %v", err)
		}
	})

	t.Run("Credential.Validate", func(t *testing.T) {
		c := domain.Credential{}
		if err := c.Validate(); err == nil {
			t.Errorf("expected error on empty credential")
		}
		c.ID = "c1"
		if !errors.Is(c.Validate(), domain.ErrEmptyUserID) {
			t.Errorf("expected ErrEmptyUserID, got %v", c.Validate())
		}
		c.UserID = "u1"
		c.Kind = "INVALID"
		if !errors.Is(c.Validate(), domain.ErrInvalidCredentialKind) {
			t.Errorf("expected ErrInvalidCredentialKind, got %v", c.Validate())
		}
		c.Kind = domain.CredentialKindPAT
		if !errors.Is(c.Validate(), domain.ErrEmptyScopesForPAT) {
			t.Errorf("expected ErrEmptyScopesForPAT, got %v", c.Validate())
		}
		c.Scopes = []domain.PATScope{"unknown_scope"}
		if !errors.Is(c.Validate(), domain.ErrInvalidPATScope) {
			t.Errorf("expected ErrInvalidPATScope, got %v", c.Validate())
		}
		c.Scopes = []domain.PATScope{domain.ScopeRead}
		if err := c.Validate(); err != nil {
			t.Errorf("expected valid credential, got %v", err)
		}
	})

	t.Run("Role.Validate", func(t *testing.T) {
		r := domain.Role{}
		if err := r.Validate(); err == nil {
			t.Errorf("expected error on empty role")
		}
		r.ID = "r1"
		if err := r.Validate(); err == nil {
			t.Errorf("expected error on empty role name")
		}
		r.Name = "Custom"
		r.Permissions = []domain.Permission{"invalid:perm"}
		if !errors.Is(r.Validate(), domain.ErrInvalidPermission) {
			t.Errorf("expected ErrInvalidPermission, got %v", r.Validate())
		}
		r.Permissions = []domain.Permission{domain.PermissionLibraryRead}
		if err := r.Validate(); err != nil {
			t.Errorf("expected valid role, got %v", err)
		}
	})
}

func TestPrincipal_Context(t *testing.T) {
	t.Run("nil context returns false", func(t *testing.T) {
		var nilCtx context.Context
		p, ok := domain.PrincipalFromContext(nilCtx)
		if ok || p != nil {
			t.Errorf("expected ok=false and p=nil on nil context, got ok=%v, p=%v", ok, p)
		}
	})

	t.Run("context without principal returns false", func(t *testing.T) {
		p, ok := domain.PrincipalFromContext(context.Background())
		if ok || p != nil {
			t.Errorf("expected ok=false and p=nil on empty context, got ok=%v, p=%v", ok, p)
		}
	})

	t.Run("MustPrincipalFromContext panics on missing principal", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on missing principal in context")
			}
		}()
		domain.MustPrincipalFromContext(context.Background())
	})

	t.Run("round-trip attaches and extracts principal", func(t *testing.T) {
		expected := domain.NewPrincipal(
			domain.User{ID: "usr_1", Username: "alice"},
			domain.BuiltInRoles()[:1],
			domain.CredentialKindPassword,
			"cred_1",
			nil,
		)
		ctx := domain.ContextWithPrincipal(context.Background(), expected)

		p, ok := domain.PrincipalFromContext(ctx)
		if !ok || p == nil {
			t.Fatalf("expected ok=true, got ok=%v", ok)
		}
		if p.User.ID != expected.User.ID || p.User.Username != expected.User.Username {
			t.Errorf("got user %+v, want %+v", p.User, expected.User)
		}

		mustP := domain.MustPrincipalFromContext(ctx)
		if mustP.User.ID != expected.User.ID {
			t.Errorf("MustPrincipalFromContext got ID %q, want %q", mustP.User.ID, expected.User.ID)
		}
	})
}
