// Package bootstrap implements first-run account provisioning.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

// Repository serializes the empty-user check and account creation in one transaction.
type Repository interface {
	WithBootstrapTx(context.Context, func(storage.UserRepository) error) error
}

// ProvisionAdmin creates the initial named administrator, if configured and unsealed.
// Existing accounts are never modified, even if bootstrap credentials change.
func ProvisionAdmin(ctx context.Context, repo Repository, username, password string) error {
	return repo.WithBootstrapTx(ctx, func(tx storage.UserRepository) error {
		count, err := tx.CountUsers(ctx)
		if err != nil {
			return fmt.Errorf("counting users: %w", err)
		}
		if count != 0 {
			return nil
		}
		if strings.TrimSpace(username) == "" && strings.TrimSpace(password) == "" {
			return nil
		}
		if strings.TrimSpace(username) == "" || strings.TrimSpace(password) == "" {
			return errors.New("first-run provisioning requires both TOBITEN_ADMIN_USER and TOBITEN_ADMIN_PASS")
		}
		userID, err := domain.NewID()
		if err != nil {
			return fmt.Errorf("generating user ID: %w", err)
		}
		credentialID, err := domain.NewID()
		if err != nil {
			return fmt.Errorf("generating credential ID: %w", err)
		}
		scheme := auth.HashSchemeArgon2id
		hash, err := auth.HashPassword(password)
		if err != nil {
			return fmt.Errorf("hashing bootstrap password: %w", err)
		}
		if err := tx.CreateUser(ctx, &domain.User{ID: userID, Username: username}); err != nil {
			return fmt.Errorf("creating initial user: %w", err)
		}
		if err := tx.CreateCredential(ctx, &domain.Credential{
			ID: credentialID, UserID: userID, Kind: domain.CredentialKindPassword,
			SecretHash: &hash, HashScheme: &scheme,
		}); err != nil {
			return fmt.Errorf("creating initial password credential: %w", err)
		}
		if err := tx.AssignUserRole(ctx, userID, domain.RoleAdminID); err != nil {
			return fmt.Errorf("assigning initial Admin role: %w", err)
		}
		return nil
	})
}
