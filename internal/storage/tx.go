package storage

import "context"

// Tx represents an active cross-repository database transaction.
// Every repository returned by Tx is bound to the same underlying transaction.
type Tx interface {
	Events() EventRepository
	Jobs() JobRepository
	Users() UserRepository
	Labels() LabelRepository
	Settings() SettingsRepository
}

// Transactor executes multi-repository work atomically in one database transaction.
type Transactor interface {
	WithTx(ctx context.Context, fn func(tx Tx) error) error
}
