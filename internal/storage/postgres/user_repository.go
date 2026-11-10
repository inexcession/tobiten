package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TxBeginner abstracts the capability to execute queries and begin transactions/savepoints.
// It is implemented by both *pgxpool.Pool and pgx.Tx (which creates pseudo-nested transactions via savepoints).
type TxBeginner interface {
	DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

// UserRepository implements storage.UserRepository using PostgreSQL and sqlc.
type UserRepository struct {
	db TxBeginner
	q  *Queries
}

// NewUserRepository creates a new UserRepository backed by a pgxpool.Pool.
func NewUserRepository(pool *pgxpool.Pool) *UserRepository {
	return &UserRepository{
		db: pool,
		q:  New(pool),
	}
}

// Ensure UserRepository implements storage.UserRepository.
var _ storage.UserRepository = (*UserRepository)(nil)

// WithTx executes fn inside a transaction. If called from a repository that is
// already running within a transaction (from an outer WithTx), it begins a nested
// pseudo-transaction using a SAVEPOINT, ensuring operations do not escape the
// enclosing transaction.
func (r *UserRepository) WithTx(ctx context.Context, fn func(repo storage.UserRepository) error) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	txRepo := &UserRepository{
		db: tx,
		q:  r.q.WithTx(tx),
	}
	if err := fn(txRepo); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func mapPgError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// PostgreSQL unique_violation code is 23505
		if pgErr.Code == "23505" {
			return storage.ErrAlreadyExists
		}
	}
	return err
}

func toDomainCredential(c Credential) domain.Credential {
	scopes := make([]domain.PATScope, len(c.Scopes))
	for i, s := range c.Scopes {
		scopes[i] = domain.PATScope(s)
	}
	return domain.Credential{
		ID:              c.ID,
		UserID:          c.UserID,
		Kind:            domain.CredentialKind(c.Kind),
		Scopes:          scopes,
		SecretHash:      c.SecretHash,
		HashScheme:      c.HashScheme,
		ExternalSubject: c.ExternalSubject,
		Label:           c.Label,
		CreatedAt:       c.CreatedAt,
		LastUsedAt:      c.LastUsedAt,
		ExpiresAt:       c.ExpiresAt,
		RevokedAt:       c.RevokedAt,
	}
}

func sortPermissionsCanonical(perms []domain.Permission) {
	rank := func(p domain.Permission) int {
		for i, cand := range domain.AllPermissions() {
			if cand == p {
				return i
			}
		}
		return len(domain.AllPermissions())
	}
	slices.SortFunc(perms, func(a, b domain.Permission) int {
		return rank(a) - rank(b)
	})
}

// CreateUser persists a new user account.
func (r *UserRepository) CreateUser(ctx context.Context, user *domain.User) error {
	if err := user.Validate(); err != nil {
		return fmt.Errorf("validating user: %w", err)
	}
	now := time.Now().UTC()
	if user.CreatedAt.IsZero() {
		user.CreatedAt = now
	}
	if user.UpdatedAt.IsZero() {
		user.UpdatedAt = user.CreatedAt
	}
	err := r.q.CreateUser(ctx, CreateUserParams{
		ID:        user.ID,
		Username:  user.Username,
		CreatedAt: user.CreatedAt,
		UpdatedAt: user.UpdatedAt,
	})
	return mapPgError(err)
}

// GetUserByID retrieves a user by ID.
func (r *UserRepository) GetUserByID(ctx context.Context, id string) (*domain.User, error) {
	u, err := r.q.GetUserByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	return &domain.User{
		ID:        u.ID,
		Username:  u.Username,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}, nil
}

// GetUserByUsername retrieves a user by username.
func (r *UserRepository) GetUserByUsername(ctx context.Context, username string) (*domain.User, error) {
	u, err := r.q.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, mapPgError(err)
	}
	return &domain.User{
		ID:        u.ID,
		Username:  u.Username,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}, nil
}

// UpdateUser updates a user's details.
func (r *UserRepository) UpdateUser(ctx context.Context, user *domain.User) error {
	if err := user.Validate(); err != nil {
		return fmt.Errorf("validating user: %w", err)
	}
	if user.UpdatedAt.IsZero() {
		user.UpdatedAt = time.Now().UTC()
	}
	rows, err := r.q.UpdateUser(ctx, UpdateUserParams{
		ID:        user.ID,
		Username:  user.Username,
		UpdatedAt: user.UpdatedAt,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// DeleteUser deletes a user by ID.
func (r *UserRepository) DeleteUser(ctx context.Context, id string) error {
	rows, err := r.q.DeleteUser(ctx, id)
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// CountUsers returns the total count of user accounts.
func (r *UserRepository) CountUsers(ctx context.Context) (int64, error) {
	return r.q.CountUsers(ctx)
}

// ListUsers returns all users ordered by creation time.
func (r *UserRepository) ListUsers(ctx context.Context) ([]domain.User, error) {
	list, err := r.q.ListUsers(ctx)
	if err != nil {
		return nil, mapPgError(err)
	}
	res := make([]domain.User, len(list))
	for i, u := range list {
		res[i] = domain.User{
			ID:        u.ID,
			Username:  u.Username,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		}
	}
	return res, nil
}

// CreateCredential persists a new credential entry.
func (r *UserRepository) CreateCredential(ctx context.Context, cred *domain.Credential) error {
	if err := cred.Validate(); err != nil {
		return fmt.Errorf("validating credential: %w", err)
	}
	if cred.CreatedAt.IsZero() {
		cred.CreatedAt = time.Now().UTC()
	}
	scopes := make([]string, len(cred.Scopes))
	for i, s := range cred.Scopes {
		scopes[i] = string(s)
	}
	err := r.q.CreateCredential(ctx, CreateCredentialParams{
		ID:              cred.ID,
		UserID:          cred.UserID,
		Kind:            string(cred.Kind),
		Scopes:          scopes,
		SecretHash:      cred.SecretHash,
		HashScheme:      cred.HashScheme,
		ExternalSubject: cred.ExternalSubject,
		Label:           cred.Label,
		CreatedAt:       cred.CreatedAt,
		LastUsedAt:      cred.LastUsedAt,
		ExpiresAt:       cred.ExpiresAt,
		RevokedAt:       cred.RevokedAt,
	})
	return mapPgError(err)
}

// GetCredentialByID retrieves a credential by ID.
func (r *UserRepository) GetCredentialByID(ctx context.Context, id string) (*domain.Credential, error) {
	c, err := r.q.GetCredentialByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	dc := toDomainCredential(c)
	return &dc, nil
}

// GetActivePasswordCredentialByUserID retrieves the active (non-revoked, non-expired) PASSWORD credential for a user.
func (r *UserRepository) GetActivePasswordCredentialByUserID(ctx context.Context, userID string) (*domain.Credential, error) {
	c, err := r.q.GetActivePasswordCredentialByUserID(ctx, userID)
	if err != nil {
		return nil, mapPgError(err)
	}
	dc := toDomainCredential(c)
	return &dc, nil
}

// GetActiveOIDCCredentialBySubject retrieves the active (non-revoked, non-expired) OIDC_LINK credential for an external subject.
func (r *UserRepository) GetActiveOIDCCredentialBySubject(ctx context.Context, externalSubject string) (*domain.Credential, error) {
	c, err := r.q.GetActiveOIDCCredentialBySubject(ctx, &externalSubject)
	if err != nil {
		return nil, mapPgError(err)
	}
	dc := toDomainCredential(c)
	return &dc, nil
}

// ListCredentialsByUserID returns all credentials belonging to a user.
func (r *UserRepository) ListCredentialsByUserID(ctx context.Context, userID string) ([]domain.Credential, error) {
	list, err := r.q.ListCredentialsByUserID(ctx, userID)
	if err != nil {
		return nil, mapPgError(err)
	}
	res := make([]domain.Credential, len(list))
	for i, c := range list {
		res[i] = toDomainCredential(c)
	}
	return res, nil
}

// RevokeCredential marks a credential as revoked. If already revoked, the earlier revoked_at timestamp is preserved.
func (r *UserRepository) RevokeCredential(ctx context.Context, id string, revokedAt time.Time) error {
	if revokedAt.IsZero() {
		revokedAt = time.Now().UTC()
	}
	rows, err := r.q.RevokeCredential(ctx, RevokeCredentialParams{
		ID:        id,
		RevokedAt: &revokedAt,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// DeleteCredential removes a credential permanently.
func (r *UserRepository) DeleteCredential(ctx context.Context, id string) error {
	rows, err := r.q.DeleteCredential(ctx, id)
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// UpdateCredentialLastUsed updates the last_used_at timestamp of a credential.
func (r *UserRepository) UpdateCredentialLastUsed(ctx context.Context, id string, lastUsedAt time.Time) error {
	if lastUsedAt.IsZero() {
		lastUsedAt = time.Now().UTC()
	}
	rows, err := r.q.UpdateCredentialLastUsed(ctx, UpdateCredentialLastUsedParams{
		ID:         id,
		LastUsedAt: &lastUsedAt,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ListRoles returns all roles in the system with their permissions populated in canonical order.
func (r *UserRepository) ListRoles(ctx context.Context) ([]domain.Role, error) {
	roles, err := r.q.ListRoles(ctx)
	if err != nil {
		return nil, mapPgError(err)
	}
	allPerms, err := r.q.ListAllRolePermissions(ctx)
	if err != nil {
		return nil, mapPgError(err)
	}
	permsByRole := make(map[string][]domain.Permission)
	for _, p := range allPerms {
		permsByRole[p.RoleID] = append(permsByRole[p.RoleID], domain.Permission(p.Permission))
	}
	res := make([]domain.Role, len(roles))
	for i, rl := range roles {
		perms := permsByRole[rl.ID]
		sortPermissionsCanonical(perms)
		res[i] = domain.Role{
			ID:                  rl.ID,
			Name:                rl.Name,
			BuiltIn:             rl.BuiltIn,
			RequiresAllowLabels: rl.RequiresAllowLabels,
			Permissions:         perms,
		}
	}
	return res, nil
}

// GetRoleByID returns a single role with its permissions by role ID.
func (r *UserRepository) GetRoleByID(ctx context.Context, id string) (*domain.Role, error) {
	rl, err := r.q.GetRoleByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	permsRaw, err := r.q.ListPermissionsForRole(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	perms := make([]domain.Permission, len(permsRaw))
	for i, p := range permsRaw {
		perms[i] = domain.Permission(p)
	}
	sortPermissionsCanonical(perms)
	return &domain.Role{
		ID:                  rl.ID,
		Name:                rl.Name,
		BuiltIn:             rl.BuiltIn,
		RequiresAllowLabels: rl.RequiresAllowLabels,
		Permissions:         perms,
	}, nil
}

// GetRoleByName returns a single role with its permissions by role name.
func (r *UserRepository) GetRoleByName(ctx context.Context, name string) (*domain.Role, error) {
	rl, err := r.q.GetRoleByName(ctx, name)
	if err != nil {
		return nil, mapPgError(err)
	}
	permsRaw, err := r.q.ListPermissionsForRole(ctx, rl.ID)
	if err != nil {
		return nil, mapPgError(err)
	}
	perms := make([]domain.Permission, len(permsRaw))
	for i, p := range permsRaw {
		perms[i] = domain.Permission(p)
	}
	sortPermissionsCanonical(perms)
	return &domain.Role{
		ID:                  rl.ID,
		Name:                rl.Name,
		BuiltIn:             rl.BuiltIn,
		RequiresAllowLabels: rl.RequiresAllowLabels,
		Permissions:         perms,
	}, nil
}

// AssignUserRole assigns a role to a user.
func (r *UserRepository) AssignUserRole(ctx context.Context, userID, roleID string) error {
	return mapPgError(r.q.AssignUserRole(ctx, AssignUserRoleParams{
		UserID: userID,
		RoleID: roleID,
	}))
}

// RemoveUserRole removes an assigned role from a user.
func (r *UserRepository) RemoveUserRole(ctx context.Context, userID, roleID string) error {
	rows, err := r.q.RemoveUserRole(ctx, RemoveUserRoleParams{
		UserID: userID,
		RoleID: roleID,
	})
	if err != nil {
		return mapPgError(err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ListUserRoles returns all roles assigned to a user with their permissions in a single query.
func (r *UserRepository) ListUserRoles(ctx context.Context, userID string) ([]domain.Role, error) {
	rows, err := r.q.ListUserRoles(ctx, userID)
	if err != nil {
		return nil, mapPgError(err)
	}
	res := make([]domain.Role, len(rows))
	for i, row := range rows {
		perms := make([]domain.Permission, len(row.Permissions))
		for j, p := range row.Permissions {
			perms[j] = domain.Permission(p)
		}
		sortPermissionsCanonical(perms)
		res[i] = domain.Role{
			ID:                  row.ID,
			Name:                row.Name,
			BuiltIn:             row.BuiltIn,
			RequiresAllowLabels: row.RequiresAllowLabels,
			Permissions:         perms,
		}
	}
	return res, nil
}
