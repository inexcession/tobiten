package postgres

import (
	"context"
	"errors"
	"fmt"
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
	return runTx(ctx, t.db, "storage", func(tx pgx.Tx, q *Queries) error {
		return fn(&pgTx{tx: tx, q: q})
	})
}

func runTx(ctx context.Context, db TxBeginner, label string, fn func(tx pgx.Tx, q *Queries) error) (retErr error) {
	if db == nil {
		return errors.New("db connection does not support transactions")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning %s transaction: %w", label, err)
	}
	parentTx, isNested := db.(pgx.Tx)
	committed := false
	defer func() {
		if committed {
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
		if isNested {
			// Savepoint cleanup could not be confirmed (including when pgx closes the
			// savepoint wrapper after a failed Commit); abort the parent transaction so
			// unconfirmed inner writes can never survive an outer commit.
			_ = parentTx.Rollback(rollbackCtx)
		}
		if errors.Is(rbErr, pgx.ErrTxClosed) {
			return
		}
		rbWrapped := fmt.Errorf("rolling back %s transaction: %w", label, rbErr)
		if retErr == nil {
			retErr = rbWrapped
		} else {
			retErr = errors.Join(retErr, rbWrapped)
		}
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
	tx pgx.Tx
	q  *Queries
}

func (s *pgTx) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	return runTx(ctx, s.tx, "storage", func(tx pgx.Tx, q *Queries) error {
		return fn(&pgTx{tx: tx, q: q})
	})
}

func (s *pgTx) Events() storage.EventRepository {
	return &EventRepository{q: s.q, tx: s.tx, inTx: true}
}

func (s *pgTx) Jobs() storage.JobRepository {
	return &JobRepository{q: s.q, tx: s.tx}
}

func (s *pgTx) Users() storage.UserRepository {
	return &UserRepository{db: s.tx, q: s.q}
}

func (s *pgTx) Labels() storage.LabelRepository {
	return &LabelRepository{q: s.q, tx: s.tx}
}

func (s *pgTx) Settings() storage.SettingsRepository {
	return &SettingsRepository{db: s.tx, q: s.q}
}
