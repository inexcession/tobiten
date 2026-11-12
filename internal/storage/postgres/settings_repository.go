package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SettingsRepository implements storage.SettingsRepository using PostgreSQL and sqlc.
type SettingsRepository struct {
	db TxBeginner
	q  *Queries
}

// NewSettingsRepository creates a new SettingsRepository backed by a pgxpool.Pool.
func NewSettingsRepository(pool *pgxpool.Pool) *SettingsRepository {
	return &SettingsRepository{
		db: pool,
		q:  New(pool),
	}
}

// Ensure SettingsRepository implements storage.SettingsRepository.
var _ storage.SettingsRepository = (*SettingsRepository)(nil)

// WithTx executes fn inside a transaction. If called from a repository that is
// already running within a transaction (from an outer WithTx), it begins a nested
// pseudo-transaction using a SAVEPOINT.
func (r *SettingsRepository) WithTx(ctx context.Context, fn func(repo storage.SettingsRepository) error) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	txRepo := &SettingsRepository{
		db: tx,
		q:  r.q.WithTx(tx),
	}
	if err := fn(txRepo); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// GetSetting retrieves a setting by key.
func (r *SettingsRepository) GetSetting(ctx context.Context, key string) (*domain.SystemSetting, error) {
	s, err := r.q.GetSystemSetting(ctx, key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("getting system setting %q: %w", key, err)
	}
	return &domain.SystemSetting{
		Key:       s.Key,
		Value:     s.Value,
		IsSecret:  s.IsSecret,
		UpdatedAt: s.UpdatedAt,
		UpdatedBy: s.UpdatedBy,
	}, nil
}

// SetSetting inserts or updates a setting in system_settings.
func (r *SettingsRepository) SetSetting(ctx context.Context, setting *domain.SystemSetting) error {
	if setting == nil {
		return errors.New("setting cannot be nil")
	}
	if err := setting.Validate(); err != nil {
		return err
	}
	updatedAt := setting.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}

	res, err := r.q.UpsertSystemSetting(ctx, UpsertSystemSettingParams{
		Key:       setting.Key,
		Value:     setting.Value,
		IsSecret:  setting.IsSecret,
		UpdatedAt: updatedAt,
		UpdatedBy: setting.UpdatedBy,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign key violation
			return fmt.Errorf("user not found for updated_by: %w", storage.ErrNotFound)
		}
		return fmt.Errorf("upserting system setting %q: %w", setting.Key, err)
	}
	setting.UpdatedAt = res.UpdatedAt
	return nil
}

// DeleteSetting removes a setting from system_settings.
func (r *SettingsRepository) DeleteSetting(ctx context.Context, key string) error {
	if err := r.q.DeleteSystemSetting(ctx, key); err != nil {
		return fmt.Errorf("deleting system setting %q: %w", key, err)
	}
	return nil
}

// ListSettings returns all settings ordered by key.
func (r *SettingsRepository) ListSettings(ctx context.Context) ([]domain.SystemSetting, error) {
	rows, err := r.q.ListSystemSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing system settings: %w", err)
	}
	settings := make([]domain.SystemSetting, len(rows))
	for i, row := range rows {
		settings[i] = domain.SystemSetting{
			Key:       row.Key,
			Value:     row.Value,
			IsSecret:  row.IsSecret,
			UpdatedAt: row.UpdatedAt,
			UpdatedBy: row.UpdatedBy,
		}
	}
	return settings, nil
}

// GetSettingsMap returns non-secret settings as a string key-value map.
func (r *SettingsRepository) GetSettingsMap(ctx context.Context) (map[string]string, error) {
	settings, err := r.ListSettings(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(settings))
	for _, s := range settings {
		if !s.IsSecret {
			m[s.Key] = s.ValueString()
		}
	}
	return m, nil
}
