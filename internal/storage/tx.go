package storage

import "context"

// Tx represents an active cross-repository database transaction.
// Every repository returned by Tx is bound to the same underlying transaction.
type Tx interface {
	Transactor
	Events() EventRepository
	Jobs() JobRepository
	Users() UserRepository
	Labels() LabelRepository
	Settings() SettingsRepository
	Libraries() LibraryRepository
	CatalogueWrites() CatalogueWriteRepository
	SeriesMemberships() SeriesMembershipRepository
	People() PersonRepository
	Credits() CreditRepository
	Entities() EntityRepository
}

// ReadTx represents a snapshot-consistent read transaction across repositories.
type ReadTx interface {
	Libraries() LibraryRepository
	Catalogue() CatalogueRepository
}

// Transactor executes multi-repository work atomically in one database transaction.
type Transactor interface {
	WithTx(ctx context.Context, fn func(tx Tx) error) error
	// WithReadCommittedTx executes fn in a READ COMMITTED transaction when opening
	// a root transaction from a pool, or a savepoint inside an existing caller
	// transaction (where repositories requiring fresh post-lock reads still verify
	// READ COMMITTED isolation and fail closed on stricter levels).
	WithReadCommittedTx(ctx context.Context, fn func(tx Tx) error) error
}

// ReadTransactor executes multi-query reads inside a single REPEATABLE READ READ ONLY
// transaction snapshot so paged results, total counts, and associations stay consistent.
type ReadTransactor interface {
	WithRepeatableReadTx(ctx context.Context, fn func(tx ReadTx) error) error
}
