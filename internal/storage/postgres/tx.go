package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
)

const txRollbackTimeout = 5 * time.Second

var _ storage.Transactor = (*Transactor)(nil)

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
func (t *Transactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	if t == nil {
		return errors.New("transactor database handle is required")
	}
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	var parentScope *txScope
	if extTx, ok := t.db.(pgx.Tx); ok {
		parentScope = &txScope{tx: extTx}
	}
	return runTx(ctx, t.db, parentScope, "storage", func(child *txScope) error {
		return fn(&pgTx{scope: child, q: New(child.tx)})
	})
}

type txScope struct {
	tx             pgx.Tx
	parent         *txScope
	mu             sync.Mutex
	unconfirmedErr error
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
// (discarding s and its savepoint on PostgreSQL) or the root transaction is reached.
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
	}
	return errors.Join(errs...)
}

func runTx(ctx context.Context, db TxBeginner, parentScope *txScope, label string, fn func(scope *txScope) error) (retErr error) {
	if db == nil {
		return errors.New("db connection does not support transactions")
	}
	if err := parentScope.unconfirmed(); err != nil {
		return err
	}
	tx, err := db.Begin(ctx)
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

func (s *pgTx) Events() storage.EventRepository {
	return &EventRepository{q: s.q, tx: s.scope.tx, scope: s.scope, inTx: true}
}

func (s *pgTx) Jobs() storage.JobRepository {
	return &JobRepository{q: s.q, tx: s.scope.tx}
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
