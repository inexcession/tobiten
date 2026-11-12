package bootstrap

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"log/slog"
	"strings"
	"sync"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

// Setup implements Path B for the lifetime of one daemon. The database check and
// creation use the same serialized transaction as environment provisioning.
type Setup struct {
	mu     sync.Mutex
	repo   Repository
	token  []byte
	sealed bool
}

var _ app.SetupService = (*Setup)(nil)

// NewSetup checks the persisted seal before generating or logging a token.
// setupAPI must be built by the composition root from the resolved base URL.
func NewSetup(ctx context.Context, repo Repository, logger *slog.Logger, setupAPI string) (*Setup, error) {
	s := &Setup{repo: repo}
	sealed, err := s.IsSetupSealed(ctx)
	if err != nil {
		return nil, err
	}
	if sealed {
		return s, nil
	}
	var entropy [32]byte
	rand.Read(entropy[:])
	s.token = []byte("tb_setup_" + base64.RawURLEncoding.EncodeToString(entropy[:]))
	clear(entropy[:])
	logger.Warn("First-run setup token generated", slog.String("setup_token", string(s.token)),
		slog.String("setup_api", setupAPI), slog.String("setup_method", "POST"))
	return s, nil
}

// Close erases the token and seals this instance, including during shutdown.
func (s *Setup) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seal()
}

func (s *Setup) seal() {
	clear(s.token)
	s.token = nil
	s.sealed = true
}

func (s *Setup) checkSeal(ctx context.Context, tx storage.UserRepository) error {
	count, err := tx.CountUsers(ctx)
	if err == nil && count > 0 {
		s.seal()
	}
	return err
}

// IsSetupSealed permanently latches and erases the token once users exist.
func (s *Setup) IsSetupSealed(ctx context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return true, nil
	}
	err := s.repo.WithBootstrapTx(ctx, func(tx storage.UserRepository) error {
		return s.checkSeal(ctx, tx)
	})
	return s.sealed, err
}

// SetupInitialAdmin consumes a valid token only after account, credential and
// Admin grant commit successfully. A failed transaction leaves setup retryable.
func (s *Setup) SetupInitialAdmin(ctx context.Context, req app.SetupAdminRequest) (*domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return nil, app.ErrSetupSealed
	}
	var user *domain.User
	err := s.repo.WithBootstrapTx(ctx, func(tx storage.UserRepository) error {
		if err := s.checkSeal(ctx, tx); err != nil {
			return err
		}
		if s.sealed {
			return app.ErrSetupSealed
		}
		if len(s.token) == 0 || subtle.ConstantTimeCompare(s.token, []byte(req.SetupToken)) != 1 {
			return app.ErrUnauthorized
		}
		username := strings.TrimSpace(req.Username)
		if username == "" || strings.TrimSpace(req.Password) == "" {
			return app.ErrInvalidInput
		}
		var err error
		user, err = createAdmin(ctx, tx, username, req.Password)
		return err
	})
	if err != nil {
		return nil, err
	}
	s.seal()
	return user, nil
}
