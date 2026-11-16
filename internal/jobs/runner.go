package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

// Sentinel errors returned by the durable job runner and kind registry.
var (
	ErrRecoveryNotCompleted = errors.New("startup recovery must complete before claiming jobs")
	ErrUnknownJobKind       = errors.New("unregistered job kind")
	ErrDuplicateJobKind     = errors.New("duplicate or conflicting job kind registration")
	ErrInvalidJobKind       = errors.New("invalid job kind registration")
	ErrInvalidPoolSize      = errors.New("invalid worker pool size")
	ErrPoolsAlreadyRunning  = errors.New("worker pools already running")
	ErrMissingJobHandler    = errors.New("job kind has no execution handler")
	ErrMissingJobReconciler = errors.New("job kind requires a reconciliation hook")
	ErrJobCancelled         = errors.New("job cancelled")
	ErrPermanentFailure     = errors.New("permanent job failure")
)

// ReconcileAction describes the state transition chosen during startup recovery
// for an interrupted RUNNING job (ADR-0017 §7).
type ReconcileAction = domain.JobReconcileAction

const (
	// ReconcileRequeue returns an interrupted job to QUEUED (unless cancelled or exhausted).
	ReconcileRequeue = domain.JobReconcileRequeue
	// ReconcileSucceeded records that the interrupted job completed its final step before shutdown.
	ReconcileSucceeded = domain.JobReconcileSucceeded
	// ReconcileFailed marks the interrupted job as permanently FAILED.
	ReconcileFailed = domain.JobReconcileFailed
	// ReconcileCancelled marks the interrupted job as CANCELLED.
	ReconcileCancelled = domain.JobReconcileCancelled
)

// ReconcileOutcome describes the reconciliation decision for an interrupted RUNNING job.
type ReconcileOutcome = domain.JobReconcileOutcome

// Reconciler inspects an interrupted RUNNING job before startup recovery updates its state.
type Reconciler = domain.JobReconciler

// ReconcileFunc adapts a function to the Reconciler interface.
type ReconcileFunc = domain.JobReconcileFunc

// Handler executes one claimed attempt of a durable background job.
type Handler interface {
	Run(ctx context.Context, exec *Execution) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, exec *Execution) error

// Run implements Handler.
func (f HandlerFunc) Run(ctx context.Context, exec *Execution) error {
	return f(ctx, exec)
}

// KindConfig registers a job kind's startup recovery policy, execution hooks, and optional pool size.
type KindConfig struct {
	Kind               domain.JobKind
	ResumePolicy       domain.JobResumePolicy
	Handler            Handler
	Reconciler         Reconciler
	RequiresReconciler bool
	PoolSize           int
}

// KindRegistrar is an optional interface implemented by modules that register
// background job handlers directly on a Runner.
type KindRegistrar interface {
	RegisterJobs(r *Runner) error
}

// RunnerOptions configures a durable job Runner and its bounded per-kind worker pools.
type RunnerOptions struct {
	Logger          *slog.Logger
	Now             func() time.Time
	DefaultPoolSize int
	PoolSizes       map[domain.JobKind]int
	PollInterval    time.Duration
}

// RecoveryReport summarizes the interrupted RUNNING jobs reconciled at startup.
type RecoveryReport struct {
	TotalRunning       int
	RequeuedCheckpoint int
	RequeuedRestart    int
	Succeeded          int
	Failed             int
	Cancelled          int
}

// Execution provides a running job handler with checkpoint persistence and cancellation checks
// bound to the claimed attempt.
type Execution struct {
	mu   sync.RWMutex
	job  domain.Job
	repo storage.JobRepository
	now  func() time.Time
}

// Job returns a snapshot of the claimed job, including its latest saved checkpoint.
func (e *Execution) Job() domain.Job {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make([]byte, len(e.job.Checkpoint))
	copy(cp, e.job.Checkpoint)
	out := e.job
	out.Checkpoint = cp
	return out
}

// Checkpoint returns a copy of the job's current checkpoint JSON.
func (e *Execution) Checkpoint() json.RawMessage {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make([]byte, len(e.job.Checkpoint))
	copy(cp, e.job.Checkpoint)
	return cp
}

// DecodeCheckpoint unmarshals the current checkpoint into dst when non-empty.
func (e *Execution) DecodeCheckpoint(dst any) error {
	cp := e.Checkpoint()
	if len(cp) == 0 {
		return nil
	}
	return json.Unmarshal(cp, dst)
}

// SaveCheckpoint persists checkpoint for the current RUNNING attempt.
// If ctx is already cancelled (for example due to ownership loss), it refuses to write.
func (e *Execution) SaveCheckpoint(ctx context.Context, checkpoint json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.RLock()
	jobID := e.job.ID
	attempt := e.job.Attempts
	e.mu.RUnlock()

	updated, err := e.repo.SaveCheckpoint(ctx, jobID, attempt, checkpoint, e.now())
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.job = *updated
	e.mu.Unlock()
	return nil
}

// SaveCheckpointValue marshals v as JSON and persists it via SaveCheckpoint.
func (e *Execution) SaveCheckpointValue(ctx context.Context, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling job checkpoint: %w", err)
	}
	return e.SaveCheckpoint(ctx, raw)
}

// CheckCancelled reports whether cancellation has been requested for this job or ctx is cancelled.
func (e *Execution) CheckCancelled(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	e.mu.RLock()
	jobID := e.job.ID
	e.mu.RUnlock()
	return e.repo.IsCancelRequested(ctx, jobID)
}

// Runner coordinates job-kind registration, startup recovery under daemon ownership,
// and attempt-safe job execution over storage.JobRepository (ADR-0017).
type Runner struct {
	repo            storage.JobRepository
	logger          *slog.Logger
	now             func() time.Time
	defaultPoolSize int
	poolSizes       map[domain.JobKind]int
	pollInterval    time.Duration
	mu              sync.RWMutex
	kinds           map[domain.JobKind]KindConfig
	pools           map[domain.JobKind]*kindPool
	poolsRunning    bool
	poolCancel      context.CancelFunc
	poolWG          *sync.WaitGroup
	recovered       atomic.Bool
}

// NewRunner constructs a Runner over repo.
func NewRunner(repo storage.JobRepository, opts RunnerOptions) *Runner {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	var poolSizes map[domain.JobKind]int
	if len(opts.PoolSizes) > 0 {
		poolSizes = make(map[domain.JobKind]int, len(opts.PoolSizes))
		for k, v := range opts.PoolSizes {
			poolSizes[k] = v
		}
	}
	return &Runner{
		repo:            repo,
		logger:          logger,
		now:             nowFn,
		defaultPoolSize: opts.DefaultPoolSize,
		poolSizes:       poolSizes,
		pollInterval:    opts.PollInterval,
		kinds:           make(map[domain.JobKind]KindConfig),
	}
}

// RegisterKind registers a job kind's recovery policy and optional handler/reconciler/pool size.
func (r *Runner) RegisterKind(cfg KindConfig) error {
	if strings.TrimSpace(string(cfg.Kind)) == "" {
		return fmt.Errorf("%w: empty job kind", ErrInvalidJobKind)
	}
	if !cfg.ResumePolicy.IsValid() {
		return fmt.Errorf("%w: kind %q has invalid resume policy %q", ErrInvalidJobKind, cfg.Kind, cfg.ResumePolicy)
	}
	if cfg.PoolSize < 0 {
		return fmt.Errorf("%w: kind %q pool size %d must be non-negative", ErrInvalidPoolSize, cfg.Kind, cfg.PoolSize)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.poolsRunning {
		return ErrPoolsAlreadyRunning
	}

	existing, exists := r.kinds[cfg.Kind]
	if !exists {
		r.kinds[cfg.Kind] = cfg
		return nil
	}

	if existing.ResumePolicy != cfg.ResumePolicy {
		return fmt.Errorf("%w: kind %q policy %s conflicts with %s",
			ErrDuplicateJobKind, cfg.Kind, cfg.ResumePolicy, existing.ResumePolicy)
	}
	if existing.Handler != nil && cfg.Handler != nil {
		return fmt.Errorf("%w: kind %q already has a handler", ErrDuplicateJobKind, cfg.Kind)
	}
	if existing.Reconciler != nil && cfg.Reconciler != nil {
		return fmt.Errorf("%w: kind %q already has a reconciler", ErrDuplicateJobKind, cfg.Kind)
	}
	if existing.PoolSize > 0 && cfg.PoolSize > 0 && existing.PoolSize != cfg.PoolSize {
		return fmt.Errorf("%w: kind %q pool size %d conflicts with %d",
			ErrDuplicateJobKind, cfg.Kind, cfg.PoolSize, existing.PoolSize)
	}

	if existing.Handler == nil {
		existing.Handler = cfg.Handler
	}
	if existing.Reconciler == nil {
		existing.Reconciler = cfg.Reconciler
	}
	if existing.PoolSize == 0 {
		existing.PoolSize = cfg.PoolSize
	}
	existing.RequiresReconciler = existing.RequiresReconciler || cfg.RequiresReconciler
	r.kinds[cfg.Kind] = existing
	return nil
}

// RegisterModuleKinds imports all enabled job kinds, their declared resume policies,
// optional pool sizes, and any declared reconciliation hooks from reg into the runner's job-kind registry.
func (r *Runner) RegisterModuleKinds(reg module.Registry) error {
	if reg == nil {
		return nil
	}
	seenInRegistry := make(map[domain.JobKind]struct{})
	for _, jk := range reg.JobKinds() {
		if jk == nil {
			return fmt.Errorf("%w: nil module job kind", ErrInvalidJobKind)
		}
		name := strings.TrimSpace(jk.Name())
		if name == "" {
			return fmt.Errorf("%w: empty module job kind name", ErrInvalidJobKind)
		}
		kind := domain.JobKind(name)
		if _, dup := seenInRegistry[kind]; dup {
			return fmt.Errorf("%w: module registry declares duplicate job kind %q", ErrDuplicateJobKind, kind)
		}
		seenInRegistry[kind] = struct{}{}

		var reconciler Reconciler
		if recKind, ok := jk.(module.ReconcilableJobKind); ok {
			reconciler = recKind.Reconciler()
		}
		var requiresReconciler bool
		if reqKind, ok := jk.(module.ReconcilerRequirement); ok {
			requiresReconciler = reqKind.RequiresReconciler()
		}
		var poolSize int
		if pooledKind, ok := jk.(module.PooledJobKind); ok {
			poolSize = pooledKind.PoolSize()
		}

		if err := r.RegisterKind(KindConfig{
			Kind:               kind,
			ResumePolicy:       jk.ResumePolicy(),
			Reconciler:         reconciler,
			RequiresReconciler: requiresReconciler,
			PoolSize:           poolSize,
		}); err != nil {
			return err
		}
	}
	return nil
}

// RegisterModuleHandlers invokes RegisterJobs on every enabled module in reg
// that implements KindRegistrar.
func (r *Runner) RegisterModuleHandlers(reg module.Registry) error {
	if reg == nil {
		return nil
	}
	for _, m := range reg.EnabledModules() {
		if registrar, ok := m.(KindRegistrar); ok {
			if err := registrar.RegisterJobs(r); err != nil {
				return fmt.Errorf("registering jobs for module %q: %w", m.Name(), err)
			}
		}
	}
	return nil
}

// KindPolicy returns the registered resume policy for kind.
func (r *Runner) KindPolicy(kind domain.JobKind) (domain.JobResumePolicy, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.kinds[kind]
	if !ok {
		return "", false
	}
	return cfg.ResumePolicy, true
}

func (r *Runner) lookupKind(kind domain.JobKind) (KindConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.kinds[kind]
	return cfg, ok
}

// HasRecovered reports whether startup recovery has completed.
func (r *Runner) HasRecovered() bool {
	return r.recovered.Load()
}

// Recover reconciles every existing RUNNING job before normal claims begin (ADR-0017 §7).
// Callers must hold exclusive daemon ownership on the database before invoking Recover.
func (r *Runner) Recover(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport
	if r.repo == nil {
		return report, errors.New("job repository is required")
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	runningJobs, err := r.repo.ListRunningJobs(ctx)
	if err != nil {
		return report, fmt.Errorf("listing interrupted running jobs: %w", err)
	}
	report.TotalRunning = len(runningJobs)

	for _, job := range runningJobs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := r.recoverOne(ctx, job, &report); err != nil {
			return report, err
		}
	}

	r.recovered.Store(true)
	if report.TotalRunning > 0 {
		r.logger.Info("startup job recovery completed",
			slog.Int("total_running", report.TotalRunning),
			slog.Int("requeued_checkpoint", report.RequeuedCheckpoint),
			slog.Int("requeued_restart", report.RequeuedRestart),
			slog.Int("succeeded", report.Succeeded),
			slog.Int("failed", report.Failed),
			slog.Int("cancelled", report.Cancelled),
		)
	}
	return report, nil
}

func (r *Runner) recoverOne(ctx context.Context, job domain.Job, report *RecoveryReport) error {
	now := r.now().UTC()
	cfg, ok := r.lookupKind(job.Kind)
	if !ok {
		return fmt.Errorf("%w: interrupted job %q has unavailable kind %q", ErrUnknownJobKind, job.ID, job.Kind)
	}
	if cfg.RequiresReconciler && cfg.Reconciler == nil {
		return fmt.Errorf("%w: interrupted job %q (%s) cannot recover without reconciliation hook", ErrMissingJobReconciler, job.ID, job.Kind)
	}

	outcome := ReconcileOutcome{Action: ReconcileRequeue}
	if cfg.Reconciler != nil {
		var err error
		outcome, err = cfg.Reconciler.Reconcile(ctx, job)
		if err != nil {
			return fmt.Errorf("reconciling interrupted job %q (%s): %w", job.ID, job.Kind, err)
		}
	}

	return r.applyReconcileOutcome(ctx, job, cfg.ResumePolicy, outcome, now, report)
}

func (r *Runner) applyReconcileOutcome(
	ctx context.Context,
	job domain.Job,
	policy domain.JobResumePolicy,
	outcome ReconcileOutcome,
	now time.Time,
	report *RecoveryReport,
) error {
	switch outcome.Action {
	case ReconcileSucceeded:
		if _, err := r.repo.RecoverSucceeded(ctx, job.ID, job.Attempts, outcome.Checkpoint, now); err != nil {
			return fmt.Errorf("recovering reconciled job %q as succeeded: %w", job.ID, err)
		}
		report.Succeeded++
		return nil

	case ReconcileCancelled:
		var errPtr *string
		if outcome.Error != "" {
			errPtr = &outcome.Error
		}
		if _, err := r.repo.RecoverCancelled(ctx, job.ID, job.Attempts, outcome.Checkpoint, errPtr, now); err != nil {
			return fmt.Errorf("recovering reconciled job %q as cancelled: %w", job.ID, err)
		}
		report.Cancelled++
		return nil

	case ReconcileFailed:
		errMsg := outcome.Error
		if errMsg == "" {
			errMsg = "interrupted job marked failed during reconciliation"
		}
		updated, err := r.repo.RecoverFailed(ctx, job.ID, job.Attempts, outcome.Checkpoint, errMsg, now)
		if err != nil {
			return fmt.Errorf("recovering reconciled job %q as failed: %w", job.ID, err)
		}
		if updated.State == domain.JobStateCancelled {
			report.Cancelled++
		} else {
			report.Failed++
		}
		return nil

	case ReconcileRequeue:
		if job.CancelRequested {
			var errPtr *string
			if outcome.Error != "" {
				errPtr = &outcome.Error
			}
			if _, err := r.repo.RecoverCancelled(ctx, job.ID, job.Attempts, outcome.Checkpoint, errPtr, now); err != nil {
				return fmt.Errorf("cancelling interrupted job %q (%s): %w", job.ID, job.Kind, err)
			}
			report.Cancelled++
			return nil
		}
		if job.Attempts >= job.MaxAttempts {
			failMsg := outcome.Error
			if failMsg == "" {
				failMsg = "interrupted by daemon restart after exhausting max_attempts"
			}
			updated, err := r.repo.RecoverFailed(ctx, job.ID, job.Attempts, outcome.Checkpoint, failMsg, now)
			if err != nil {
				return fmt.Errorf("failing exhausted interrupted job %q (%s): %w", job.ID, job.Kind, err)
			}
			if updated.State == domain.JobStateCancelled {
				report.Cancelled++
			} else {
				report.Failed++
			}
			return nil
		}

		targetCheckpoint := resolveRequeueCheckpoint(job, policy, outcome.Checkpoint)
		reason := outcome.Error
		if reason == "" {
			reason = "interrupted by daemon restart"
		}
		updated, err := r.repo.RequeueInterrupted(ctx, job.ID, job.Attempts, targetCheckpoint, &reason, now, now)
		if err != nil {
			return fmt.Errorf("requeueing interrupted job %q (%s): %w", job.ID, job.Kind, err)
		}
		switch updated.State {
		case domain.JobStateCancelled:
			report.Cancelled++
		case domain.JobStateFailed:
			report.Failed++
		case domain.JobStateQueued:
			if policy == domain.JobResumeCheckpoint {
				report.RequeuedCheckpoint++
			} else {
				report.RequeuedRestart++
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported reconcile action %d for job %q", outcome.Action, job.ID)
	}
}

func resolveRequeueCheckpoint(job domain.Job, policy domain.JobResumePolicy, override json.RawMessage) json.RawMessage {
	if policy == domain.JobResumeRestart {
		return json.RawMessage("{}")
	}
	if len(override) > 0 {
		return override
	}
	if len(job.Checkpoint) > 0 {
		return job.Checkpoint
	}
	return json.RawMessage("{}")
}

// ClaimAndRunNext claims the next runnable job of kind and executes its registered handler.
// It refuses to claim until startup recovery has completed.
// Returns claimed=false, err=nil when no runnable job of kind is available.
func (r *Runner) ClaimAndRunNext(ctx context.Context, kind domain.JobKind) (bool, error) {
	if !r.recovered.Load() {
		return false, ErrRecoveryNotCompleted
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	cfg, ok := r.lookupKind(kind)
	if !ok {
		return false, fmt.Errorf("%w: %q", ErrUnknownJobKind, kind)
	}
	if cfg.Handler == nil {
		return false, fmt.Errorf("%w: %q", ErrMissingJobHandler, kind)
	}

	job, err := r.repo.ClaimNext(ctx, kind, r.now())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, err
	}

	return true, r.executeClaimedJob(ctx, cfg, job)
}

func (r *Runner) executeClaimedJob(ctx context.Context, cfg KindConfig, job *domain.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	exec := &Execution{
		job:  *job,
		repo: r.repo,
		now:  r.now,
	}

	runErr := cfg.Handler.Run(ctx, exec)

	// If the runner context was cancelled (shutdown or ownership loss), stop writing to DB
	// so the interrupted RUNNING job is left cleanly for startup recovery on next boot.
	if err := ctx.Err(); err != nil {
		return err
	}

	now := r.now().UTC()
	if runErr == nil {
		if _, err := r.repo.MarkSucceeded(ctx, job.ID, job.Attempts, now); err != nil {
			return fmt.Errorf("marking job %q succeeded: %w", job.ID, err)
		}
		return nil
	}

	if errors.Is(runErr, ErrJobCancelled) {
		msg := runErr.Error()
		if _, err := r.repo.MarkCancelled(ctx, job.ID, job.Attempts, &msg, now); err != nil {
			return fmt.Errorf("marking job %q cancelled: %w", job.ID, err)
		}
		return nil
	}

	if errors.Is(runErr, ErrPermanentFailure) {
		if _, err := r.repo.MarkFailed(ctx, job.ID, job.Attempts, runErr.Error(), now); err != nil {
			return fmt.Errorf("marking job %q permanently failed: %w", job.ID, err)
		}
		return nil
	}

	if _, err := r.repo.FailOrRetry(ctx, job.ID, job.Attempts, runErr.Error(), time.Time{}, now); err != nil {
		return fmt.Errorf("recording failure for job %q: %w", job.ID, err)
	}
	return nil
}
