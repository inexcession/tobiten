package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const txRollbackTimeout = 5 * time.Second

var (
	_ storage.Transactor     = (*Transactor)(nil)
	_ storage.ReadTransactor = (*Transactor)(nil)
)

type txIsolationMode uint8

const (
	txModeDefault txIsolationMode = iota
	txModeReadCommitted
	txModeRepeatableReadReadOnly
)

// Transactor implements storage.Transactor over a PostgreSQL pool or transaction beginner.
type Transactor struct {
	db TxBeginner
}

// NewTransactor constructs a Transactor backed by db.
func NewTransactor(db TxBeginner) *Transactor {
	return &Transactor{db: db}
}

// WithTx executes fn inside a single PostgreSQL transaction (or nested savepoint when
// backed by an existing pgx.Tx) and commits only when fn returns nil.
// Unconfirmed savepoint cleanup may close the caller's connection to abort the transaction.
func (t *Transactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return t.withTx(ctx, txModeDefault, fn)
}

// WithReadCommittedTx executes fn inside a READ COMMITTED PostgreSQL transaction when
// backed by a pool, or a nested savepoint when backed by an existing pgx.Tx.
func (t *Transactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return t.withTx(ctx, txModeReadCommitted, fn)
}

// WithRepeatableReadTx executes fn inside a REPEATABLE READ READ ONLY PostgreSQL transaction
// when backed by a pool, or a nested savepoint when backed by a caller-owned pgx.Tx that
// is already running at REPEATABLE READ (or SERIALIZABLE) and READ ONLY; incompatible caller
// transactions fail closed with storage.ErrRepeatableReadRequired before invoking fn.
func (t *Transactor) WithRepeatableReadTx(ctx context.Context, fn func(tx storage.ReadTx) error) error {
	if t == nil {
		return errors.New("transactor database handle is required")
	}
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	var parentScope *txScope
	if extTx, ok := t.db.(pgx.Tx); ok {
		if err := verifyRepeatableReadReadOnlyParent(ctx, extTx); err != nil {
			return err
		}
		parentScope = newCallerTxScope(extTx)
	}
	return runTxWithOptions(ctx, t.db, parentScope, "read", txModeRepeatableReadReadOnly, func(child *txScope) error {
		return fn(&pgReadTx{scope: child, q: New(child.tx)})
	})
}

func verifyRepeatableReadReadOnlyParent(ctx context.Context, tx pgx.Tx) error {
	var iso, readOnly string
	err := tx.QueryRow(ctx, `SELECT current_setting('transaction_isolation')::text, current_setting('transaction_read_only')::text`).Scan(&iso, &readOnly)
	if err != nil {
		return fmt.Errorf("inspecting caller transaction isolation: %w", err)
	}
	isoNorm := strings.ToLower(strings.TrimSpace(iso))
	roNorm := strings.ToLower(strings.TrimSpace(readOnly))
	if (isoNorm != "repeatable read" && isoNorm != "serializable") || roNorm != "on" {
		return fmt.Errorf("%w: got isolation=%q read_only=%q", storage.ErrRepeatableReadRequired, iso, readOnly)
	}
	return nil
}

func (t *Transactor) withTx(ctx context.Context, mode txIsolationMode, fn func(tx storage.Tx) error) error {
	if t == nil {
		return errors.New("transactor database handle is required")
	}
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	var parentScope *txScope
	if extTx, ok := t.db.(pgx.Tx); ok {
		parentScope = newCallerTxScope(extTx)
	}
	return runTxWithOptions(ctx, t.db, parentScope, "storage", mode, func(child *txScope) error {
		return fn(&pgTx{scope: child, q: New(child.tx)})
	})
}

type txScope struct {
	tx                     pgx.Tx
	parent                 *txScope
	closeOnRollbackFailure bool
	mu                     sync.Mutex
	unconfirmedErr         error
}

func newCallerTxScope(tx pgx.Tx) *txScope {
	// A pgxpool.Tx is a root transaction. Its Rollback handles aborting the
	// transaction and returns the connection to the pool. Other pgx.Tx handles
	// can be savepoints whose ancestors are outside the tracked chain.
	_, pooledRoot := tx.(*pgxpool.Tx)
	return &txScope{tx: tx, closeOnRollbackFailure: !pooledRoot}
}

func (s *txScope) markUnconfirmed(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unconfirmedErr == nil {
		s.unconfirmedErr = err
	}
}

func (s *txScope) unconfirmed() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unconfirmedErr
}

// abortAncestors walks upward from s.parent until an ancestor rollback succeeds
// (discarding s and its savepoint on PostgreSQL) or the tracked chain ends.
// Every traversed ancestor is marked unconfirmed so it cannot return nil and commit.
func (s *txScope) abortAncestors(rollbackCtx context.Context, cause error) error {
	var errs []error
	for cur := s.parent; cur != nil; cur = cur.parent {
		cur.markUnconfirmed(cause)
		ancErr := cur.tx.Rollback(rollbackCtx)
		if ancErr == nil {
			return nil
		}
		if !errors.Is(ancErr, pgx.ErrTxClosed) {
			errs = append(errs, ancErr)
		}
		if cur.closeOnRollbackFailure {
			// pgx has no parent accessor for savepoints. Closing the connection
			// aborts any untracked root, even if rollbackCtx has expired.
			if conn := cur.tx.Conn(); conn != nil {
				if err := conn.Close(rollbackCtx); err != nil {
					errs = append(errs, fmt.Errorf("closing connection after unconfirmed savepoint rollback: %w", err))
				}
			}
		}
	}
	return errors.Join(errs...)
}

func beginScopedTx(ctx context.Context, db TxBeginner, parentScope *txScope, label string, mode txIsolationMode) (pgx.Tx, error) {
	if parentScope == nil {
		switch mode {
		case txModeReadCommitted:
			return beginPoolTxWithSQLFallback(ctx, db, label, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
		case txModeRepeatableReadReadOnly:
			return beginPoolTxWithSQLFallback(ctx, db, label, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
		}
	}
	return db.Begin(ctx)
}

func beginPoolTxWithSQLFallback(ctx context.Context, db TxBeginner, label string, opts pgx.TxOptions, fallbackSQL string) (pgx.Tx, error) {
	if bt, ok := db.(readCommittedTxBeginner); ok {
		return bt.BeginTx(ctx, opts)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, isoErr := tx.Exec(ctx, fallbackSQL); isoErr != nil {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txRollbackTimeout)
		_ = tx.Rollback(rollbackCtx)
		cancel()
		return nil, fmt.Errorf("setting isolation mode for %s transaction: %w", label, isoErr)
	}
	return tx, nil
}

func runTx(ctx context.Context, db TxBeginner, parentScope *txScope, label string, fn func(scope *txScope) error) error {
	return runTxWithOptions(ctx, db, parentScope, label, txModeDefault, fn)
}

func runTxWithOptions(ctx context.Context, db TxBeginner, parentScope *txScope, label string, mode txIsolationMode, fn func(scope *txScope) error) (retErr error) {
	if db == nil {
		return errors.New("db connection does not support transactions")
	}
	if err := parentScope.unconfirmed(); err != nil {
		return err
	}
	tx, err := beginScopedTx(ctx, db, parentScope, label, mode)
	if err != nil {
		return fmt.Errorf("beginning %s transaction: %w", label, err)
	}
	scope := &txScope{
		tx:     tx,
		parent: parentScope,
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if unconfirmedErr := scope.unconfirmed(); unconfirmedErr != nil {
			if retErr == nil {
				retErr = unconfirmedErr
			}
			return
		}
		// Detach cancellation so ROLLBACK / ROLLBACK TO SAVEPOINT still runs even when
		// the caller context was cancelled before cleanup.
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txRollbackTimeout)
		defer cancel()
		rbErr := tx.Rollback(rollbackCtx)
		if rbErr == nil {
			return
		}
		var rbWrapped error
		if !errors.Is(rbErr, pgx.ErrTxClosed) {
			rbWrapped = fmt.Errorf("rolling back %s transaction: %w", label, rbErr)
		} else {
			rbWrapped = fmt.Errorf("unconfirmed %s savepoint cleanup: %w", label, rbErr)
		}
		if ancErr := scope.abortAncestors(rollbackCtx, rbWrapped); ancErr != nil {
			rbWrapped = errors.Join(rbWrapped, ancErr)
		}
		if retErr == nil {
			retErr = rbWrapped
		} else {
			retErr = errors.Join(retErr, rbWrapped)
		}
	}()

	if err := fn(scope); err != nil {
		return err
	}
	if unconfirmedErr := scope.unconfirmed(); unconfirmedErr != nil {
		return unconfirmedErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing %s transaction: %w", label, err)
	}
	committed = true
	return nil
}

type readCommittedTxBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

func runReadCommittedTx(ctx context.Context, db TxBeginner, label string, fn func(tx pgx.Tx, q *Queries) error) error {
	if db == nil {
		return errors.New("db connection does not support transactions")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var (
		tx  pgx.Tx
		err error
	)
	if bt, ok := db.(readCommittedTxBeginner); ok {
		tx, err = bt.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	} else {
		tx, err = db.Begin(ctx)
		if err == nil {
			if _, isoErr := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); isoErr != nil {
				rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txRollbackTimeout)
				_ = tx.Rollback(rollbackCtx)
				cancel()
				return fmt.Errorf("setting READ COMMITTED isolation for %s transaction: %w", label, isoErr)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("beginning %s transaction: %w", label, err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txRollbackTimeout)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	if err := fn(tx, New(tx)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing %s transaction: %w", label, err)
	}
	committed = true
	return nil
}

type pgTx struct {
	scope *txScope
	q     *Queries
}

func (s *pgTx) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	return runTx(ctx, s.scope.tx, s.scope, "storage", func(child *txScope) error {
		return fn(&pgTx{scope: child, q: New(child.tx)})
	})
}

func (s *pgTx) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return s.WithTx(ctx, fn)
}

func (s *pgTx) Events() storage.EventRepository {
	return &EventRepository{q: s.q, tx: s.scope.tx, scope: s.scope, inTx: true}
}

func (s *pgTx) Jobs() storage.JobRepository {
	return &JobRepository{q: s.q, tx: s.scope.tx, scope: s.scope, inTx: true}
}

func (s *pgTx) Users() storage.UserRepository {
	return &UserRepository{db: s.scope.tx, q: s.q}
}

func (s *pgTx) Labels() storage.LabelRepository {
	return &LabelRepository{q: s.q, tx: s.scope.tx}
}

func (s *pgTx) Settings() storage.SettingsRepository {
	return &SettingsRepository{db: s.scope.tx, q: s.q}
}

func (s *pgTx) Libraries() storage.LibraryRepository {
	return &LibraryRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

func (s *pgTx) CatalogueWrites() storage.CatalogueWriteRepository {
	return &CatalogueWriteRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

func (s *pgTx) SeriesMemberships() storage.SeriesMembershipRepository {
	return &SeriesMembershipRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

func (s *pgTx) People() storage.PersonRepository {
	return &PersonRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

func (s *pgTx) Credits() storage.CreditRepository {
	return &CreditRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

func (s *pgTx) Entities() storage.EntityRepository {
	return &EntityRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

type pgReadTx struct {
	scope *txScope
	q     *Queries
}

func (s *pgReadTx) Libraries() storage.LibraryRepository {
	return &LibraryRepository{q: s.q, tx: s.scope.tx, scope: s.scope}
}

func (s *pgReadTx) Catalogue() storage.CatalogueRepository {
	return NewCatalogueRepository(s.scope.tx)
}
