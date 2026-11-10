package domain_test

import (
	"slices"
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

func TestBuiltInRolesExactMatching(t *testing.T) {
	roles := domain.BuiltInRoles()
	if len(roles) != 4 {
		t.Fatalf("expected 4 built-in roles, got %d", len(roles))
	}

	admin, ok := domain.BuiltInRoleByID(domain.RoleAdminID)
	if !ok || admin.Name != domain.RoleAdminName {
		t.Fatalf("failed to find built-in Admin role by ID %s", domain.RoleAdminID)
	}
	if !slices.Contains(admin.Permissions, domain.PermissionJobsReadAll) ||
		!slices.Contains(admin.Permissions, domain.PermissionTrashPurge) {
		t.Errorf("Admin role missing administrator-only permissions (decision I-3)")
	}

	curator, ok := domain.BuiltInRoleByID(domain.RoleCuratorID)
	if !ok || curator.Name != domain.RoleCuratorName {
		t.Fatalf("failed to find built-in Curator role by ID %s", domain.RoleCuratorID)
	}

	reader, ok := domain.BuiltInRoleByID(domain.RoleReaderID)
	if !ok || reader.Name != domain.RoleReaderName {
		t.Fatalf("failed to find built-in Reader role by ID %s", domain.RoleReaderID)
	}

	restricted, ok := domain.BuiltInRoleByID(domain.RoleRestrictedReaderID)
	if !ok || restricted.Name != domain.RoleRestrictedReaderName {
		t.Fatalf("failed to find built-in Restricted reader role by ID %s", domain.RoleRestrictedReaderID)
	}
}
