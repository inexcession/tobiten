package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
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
	ErrRecoveryNotCompleted     = errors.New("startup recovery must complete before claiming jobs")
	ErrUnknownJobKind           = errors.New("unregistered job kind")
	ErrDuplicateJobKind         = errors.New("duplicate or conflicting job kind registration")
	ErrInvalidJobKind           = errors.New("invalid job kind registration")
	ErrInvalidPoolSize          = errors.New("invalid worker pool size")
	ErrPoolsAlreadyRunning      = errors.New("worker pools already running")
	ErrDirectClaimInProgress    = errors.New("direct job claim is currently in progress")
	ErrClaimFailed              = errors.New("failed to claim job")
	ErrOutcomePersistenceFailed = errors.New("failed to persist job outcome")
	ErrMissingJobHandler        = errors.New("job kind has no execution handler")
	ErrMissingJobReconciler     = errors.New("job kind requires a reconciliation hook")
	ErrJobCancelled             = errors.New("job cancelled")
	ErrPermanentFailure         = errors.New("permanent job failure")
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

// CheckpointReconciler inspects an already-locked job row (principally its
// durable checkpoint) without performing external I/O or repository queries to
// determine whether the attempt already committed its terminal outcome.
type CheckpointReconciler func(job domain.Job) (ReconcileOutcome, error)

// KindConfig registers a job kind's startup recovery policy, execution hooks, and optional pool size.
type KindConfig struct {
	Kind                 domain.JobKind
	ResumePolicy         domain.JobResumePolicy
	Handler              Handler
	Reconciler           Reconciler
	CheckpointReconciler CheckpointReconciler
	RequiresReconciler   bool
	PoolSize             int
	OnSucceeded          func(ctx context.Context, job domain.Job)
}

// KindRegistrar is an optional interface implemented by modules that register
// background job handlers directly on a Runner.
type KindRegistrar interface {
	RegisterJobs(r *Runner) error
}

// ProgressPublisher receives rate-limited ephemeral worker progress ticks (ADR-0017 §2, ADR-0018 S3).
type ProgressPublisher interface {
	Publish(tick domain.JobProgressTick) bool
}

type progressJobCleaner interface {
	ClearJob(jobID string)
}

type progressPreflighter interface {
	CanPublish(jobID string) bool
}

// RunnerOptions configures a durable job Runner and its bounded per-kind worker pools.
type RunnerOptions struct {
	Logger          *slog.Logger
	Now             func() time.Time
	DefaultPoolSize int
	PoolSizes       map[domain.JobKind]int
	PollInterval    time.Duration
	OnFatal         func(error)
	ProgressHub     ProgressPublisher
	OnTransition    func(job domain.Job)
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

// Execution provides a running job handler with checkpoint persistence, cancellation checks,
// and ephemeral progress reporting bound to the claimed attempt.
type Execution struct {
	mu         sync.RWMutex
	job        domain.Job
	repo       storage.JobRepository
	now        func() time.Time
	startedAt  time.Time
	execCtx    context.Context
	progressMu sync.RWMutex
	progress   ProgressPublisher
	closed     bool
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

func mergeExecutionContext(execCtx, callerCtx context.Context) (context.Context, context.CancelFunc) {
	if execCtx == nil {
		return callerCtx, func() {}
	}
	mergedCtx, cancel := context.WithCancel(callerCtx)
	stop := context.AfterFunc(execCtx, cancel)
	return mergedCtx, func() {
		stop()
		cancel()
	}
}

// ReportProgress publishes an ephemeral, rate-limited progress tick for this job
// without persisting any outbox row or SSE ID (ADR-0017 §2, ADR-0018 S3).
// It resolves current job ownership from persistence (failing closed on error or
// non-running state) and stops accepting reports once the handler execution ends.
// Internal EVENT_DELIVERY jobs suppress progress ticks.
func (e *Execution) ReportProgress(ctx context.Context, scannedCount int64, currentPath string) bool {
	if e == nil || ctx == nil || ctx.Err() != nil {
		return false
	}

	e.progressMu.RLock()
	closed := e.closed
	progress := e.progress
	repo := e.repo
	execCtx := e.execCtx
	e.progressMu.RUnlock()

	if closed || progress == nil || repo == nil {
		return false
	}
	if execCtx != nil && execCtx.Err() != nil {
		return false
	}

	e.mu.RLock()
	jobID := e.job.ID
	kind := e.job.Kind
	attempt := e.job.Attempts
	startedAt := e.startedAt
	e.mu.RUnlock()

	if kind.IsInternal() {
		return false
	}
	if preflight, ok := progress.(progressPreflighter); ok && !preflight.CanPublish(jobID) {
		return false
	}

	lookupCtx, cancelLookup := mergeExecutionContext(execCtx, ctx)
	defer cancelLookup()

	current, err := repo.GetJobByID(lookupCtx, jobID)
	if err != nil || current == nil {
		return false
	}
	if current.State != domain.JobStateRunning || current.Attempts != attempt {
		return false
	}

	nowFn := time.Now
	if e.now != nil {
		nowFn = e.now
	}
	now := nowFn().UTC()
	durationMS := max(int64(0), now.Sub(startedAt).Milliseconds())

	var createdBy *string
	if current.CreatedBy != nil {
		v := *current.CreatedBy
		createdBy = &v
	}

	e.progressMu.RLock()
	defer e.progressMu.RUnlock()
	if e.closed || e.progress == nil || lookupCtx.Err() != nil {
		return false
	}

	return e.progress.Publish(domain.JobProgressTick{
		JobID:        current.ID,
		Kind:         current.Kind,
		CreatedBy:    createdBy,
		ScannedCount: scannedCount,
		CurrentPath:  currentPath,
		DurationMS:   durationMS,
	})
}

func (e *Execution) finish() {
	if e == nil {
		return
	}
	e.progressMu.Lock()
	e.closed = true
	e.progress = nil
	e.progressMu.Unlock()
}

// Runner coordinates job-kind registration, startup recovery under daemon ownership,
// and attempt-safe job execution over storage.JobRepository (ADR-0017).
type Runner struct {
	repo               storage.JobRepository
	logger             *slog.Logger
	now                func() time.Time
	defaultPoolSize    int
	poolSizes          map[domain.JobKind]int
	pollInterval       time.Duration
	onFatal            func(error)
	progress           ProgressPublisher
	onTransition       func(domain.Job)
	mu                 sync.RWMutex
	kinds              map[domain.JobKind]KindConfig
	activePools        *poolGeneration
	activeDirectClaims int
	poolErr            error
	recovered          atomic.Bool
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
		onFatal:         opts.OnFatal,
		progress:        opts.ProgressHub,
		onTransition:    opts.OnTransition,
		kinds:           make(map[domain.JobKind]KindConfig),
	}
}

func (r *Runner) notifyTransition(job domain.Job) {
	if r == nil || r.onTransition == nil || job.Kind.IsInternal() {
		return
	}
	if _, ok := domain.JobLifecycleEventKind(job.State); ok {
		r.onTransition(job)
	}
}

func (r *Runner) notifyTransitionPtr(job *domain.Job) {
	if job != nil {
		r.notifyTransition(*job)
	}
}

func (r *Runner) clearProgressJob(jobID string) {
	if r == nil || r.progress == nil || jobID == "" {
		return
	}
	if cleaner, ok := r.progress.(progressJobCleaner); ok {
		cleaner.ClearJob(jobID)
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

	if r.activePools != nil {
		return ErrPoolsAlreadyRunning
	}
	if r.activeDirectClaims > 0 {
		return ErrDirectClaimInProgress
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
	if existing.CheckpointReconciler != nil && cfg.CheckpointReconciler != nil {
		return fmt.Errorf("%w: kind %q already has a checkpoint reconciler", ErrDuplicateJobKind, cfg.Kind)
	}
	if existing.PoolSize > 0 && cfg.PoolSize > 0 && existing.PoolSize != cfg.PoolSize {
		return fmt.Errorf("%w: kind %q pool size %d conflicts with %d",
			ErrDuplicateJobKind, cfg.Kind, cfg.PoolSize, existing.PoolSize)
	}
	if existing.OnSucceeded != nil && cfg.OnSucceeded != nil {
		return fmt.Errorf("%w: kind %q already has an OnSucceeded hook", ErrDuplicateJobKind, cfg.Kind)
	}

	if existing.Handler == nil {
		existing.Handler = cfg.Handler
	}
	if existing.Reconciler == nil {
		existing.Reconciler = cfg.Reconciler
	}
	if existing.CheckpointReconciler == nil {
		existing.CheckpointReconciler = cfg.CheckpointReconciler
	}
	if existing.PoolSize == 0 {
		existing.PoolSize = cfg.PoolSize
	}
	if existing.OnSucceeded == nil {
		existing.OnSucceeded = cfg.OnSucceeded
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
		updated, err := r.repo.RecoverSucceeded(ctx, job.ID, job.Attempts, outcome.Checkpoint, now)
		if err != nil {
			return fmt.Errorf("recovering reconciled job %q as succeeded: %w", job.ID, err)
		}
		report.Succeeded++
		r.notifyTransitionPtr(updated)
		return nil

	case ReconcileCancelled:
		updated, err := r.repo.RecoverCancelled(ctx, job.ID, job.Attempts, outcome.Checkpoint, optionalErrorPtr(outcome.Error), now)
		if err != nil {
			return fmt.Errorf("recovering reconciled job %q as cancelled: %w", job.ID, err)
		}
		report.Cancelled++
		r.notifyTransitionPtr(updated)
		return nil

	case ReconcileFailed:
		errMsg := fallbackMsg(outcome.Error, "interrupted job marked failed during reconciliation")
		updated, err := r.repo.RecoverFailed(ctx, job.ID, job.Attempts, outcome.Checkpoint, errMsg, now)
		if err != nil {
			return fmt.Errorf("recovering reconciled job %q as failed: %w", job.ID, err)
		}
		recordTerminalOutcome(updated, report)
		r.notifyTransitionPtr(updated)
		return nil

	case ReconcileRequeue:
		return r.applyReconcileRequeue(ctx, job, policy, outcome, now, report)

	default:
		return fmt.Errorf("unsupported reconcile action %d for job %q", outcome.Action, job.ID)
	}
}

func (r *Runner) applyReconcileRequeue(
	ctx context.Context,
	job domain.Job,
	policy domain.JobResumePolicy,
	outcome ReconcileOutcome,
	now time.Time,
	report *RecoveryReport,
) error {
	if job.CancelRequested {
		updated, err := r.repo.RecoverCancelled(ctx, job.ID, job.Attempts, outcome.Checkpoint, optionalErrorPtr(outcome.Error), now)
		if err != nil {
			return fmt.Errorf("cancelling interrupted job %q (%s): %w", job.ID, job.Kind, err)
		}
		report.Cancelled++
		r.notifyTransitionPtr(updated)
		return nil
	}
	if job.Attempts >= job.MaxAttempts {
		failMsg := fallbackMsg(outcome.Error, "interrupted by daemon restart after exhausting max_attempts")
		updated, err := r.repo.RecoverFailed(ctx, job.ID, job.Attempts, outcome.Checkpoint, failMsg, now)
		if err != nil {
			return fmt.Errorf("failing exhausted interrupted job %q (%s): %w", job.ID, job.Kind, err)
		}
		recordTerminalOutcome(updated, report)
		r.notifyTransitionPtr(updated)
		return nil
	}

	targetCheckpoint := resolveRequeueCheckpoint(job, policy, outcome.Checkpoint)
	reason := fallbackMsg(outcome.Error, "interrupted by daemon restart")
	updated, err := r.repo.RequeueInterrupted(ctx, job.ID, job.Attempts, targetCheckpoint, &reason, now, now)
	if err != nil {
		return fmt.Errorf("requeueing interrupted job %q (%s): %w", job.ID, job.Kind, err)
	}
	switch updated.State {
	case domain.JobStateCancelled:
		report.Cancelled++
		r.notifyTransition(*updated)
	case domain.JobStateFailed:
		report.Failed++
		r.notifyTransition(*updated)
	case domain.JobStateQueued:
		if policy == domain.JobResumeCheckpoint {
			report.RequeuedCheckpoint++
		} else {
			report.RequeuedRestart++
		}
	}
	return nil
}

func optionalErrorPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func fallbackMsg(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}

func recordTerminalOutcome(updated *domain.Job, report *RecoveryReport) {
	if updated.State == domain.JobStateCancelled {
		report.Cancelled++
	} else {
		report.Failed++
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
// It refuses to claim until startup recovery has completed or while managed worker pools are running.
// Returns claimed=false, err=nil when no runnable job of kind is available.
func (r *Runner) ClaimAndRunNext(ctx context.Context, kind domain.JobKind) (bool, error) {
	if !r.recovered.Load() {
		return false, ErrRecoveryNotCompleted
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	r.mu.Lock()
	if r.activePools != nil {
		r.mu.Unlock()
		return false, ErrPoolsAlreadyRunning
	}
	cfg, ok := r.kinds[kind]
	if !ok {
		r.mu.Unlock()
		return false, fmt.Errorf("%w: %q", ErrUnknownJobKind, kind)
	}
	if cfg.Handler == nil {
		r.mu.Unlock()
		return false, fmt.Errorf("%w: %q", ErrMissingJobHandler, kind)
	}
	r.activeDirectClaims++
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.activeDirectClaims--
		r.mu.Unlock()
	}()

	job, err := r.repo.ClaimNext(ctx, kind, r.now())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	r.notifyTransition(*job)

	return true, r.executeClaimedJob(ctx, cfg, job)
}

var defaultOutcomeRetryDelays = []time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
}

func (r *Runner) executeClaimedJob(ctx context.Context, cfg KindConfig, job *domain.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	startedAt := r.now().UTC()
	if job.StartedAt != nil && !job.StartedAt.IsZero() && !job.StartedAt.After(startedAt) {
		startedAt = job.StartedAt.UTC()
	}
	exec := &Execution{
		job:       *job,
		repo:      r.repo,
		now:       r.now,
		startedAt: startedAt,
		execCtx:   execCtx,
		progress:  r.progress,
	}
	var cleanupOnce sync.Once
	cleanupProgress := func() {
		cleanupOnce.Do(func() {
			cancelExec()
			exec.finish()
			r.clearProgressJob(job.ID)
		})
	}
	defer cleanupProgress()

	runErr := r.runHandlerSafely(execCtx, cfg, job, exec)

	// End progress reporting as soon as the handler returns so a retained Execution
	// cannot emit late ticks or recreate rate-limit state after ClearJob.
	cleanupProgress()

	// If the runner context was cancelled (shutdown or ownership loss), stop writing to DB
	// so the interrupted RUNNING job is left cleanly for startup recovery on next boot.
	if err := ctx.Err(); err != nil {
		return err
	}

	return r.persistExecutionOutcome(ctx, cfg, job, runErr)
}

var errJobHandlerPanicked = errors.New("job handler panicked")

func (r *Runner) runHandlerSafely(ctx context.Context, cfg KindConfig, job *domain.Job, exec *Execution) (runErr error) {
	defer func() {
		if rec := recover(); rec != nil {
			if r.logger != nil {
				r.logger.Error("job handler panicked",
					slog.String("job_id", job.ID),
					slog.String("job_kind", string(job.Kind)),
					slog.Int("attempt", job.Attempts),
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
				)
			}
			runErr = errJobHandlerPanicked
		}
	}()
	return cfg.Handler.Run(ctx, exec)
}

func (r *Runner) persistExecutionOutcome(ctx context.Context, cfg KindConfig, job *domain.Job, runErr error) error {
	now := r.now().UTC()
	settled := *job
	settledAsSucceeded := runErr == nil

	var (
		label      string
		writeFn    func(context.Context) error
		isResolved func(*domain.Job) bool
	)

	if runErr != nil && cfg.CheckpointReconciler != nil {
		label, writeFn, isResolved = reconciledOutcomePersistence(
			r.repo, cfg.CheckpointReconciler, job, runErr, now, &settled, &settledAsSucceeded,
		)
	} else {
		label, writeFn, isResolved = directOutcomePersistence(r.repo, job, runErr, now, &settled)
	}

	err := r.persistOutcomeWithRetry(ctx, job, label, writeFn, isResolved)
	if err != nil {
		return err
	}

	r.notifyTransition(settled)
	if settledAsSucceeded && cfg.OnSucceeded != nil {
		cfg.OnSucceeded(ctx, settled)
	}
	return nil
}

func reconciledOutcomePersistence(
	repo storage.JobRepository,
	reconciler CheckpointReconciler,
	job *domain.Job,
	runErr error,
	now time.Time,
	settled *domain.Job,
	settledAsSucceeded *bool,
) (string, func(context.Context) error, func(*domain.Job) bool) {
	writeFn := func(writeCtx context.Context) error {
		return repo.WithTx(writeCtx, func(txRepo storage.JobRepository) error {
			updated, succeeded, err := settleLockedOutcomeTx(writeCtx, txRepo, reconciler, job, runErr, now)
			if err != nil {
				return err
			}
			if updated != nil {
				*settled = *updated
			}
			*settledAsSucceeded = succeeded
			return nil
		})
	}
	isResolved := func(row *domain.Job) bool {
		if row.State == domain.JobStateSucceeded && row.Attempts == job.Attempts {
			*settled = *row
			*settledAsSucceeded = true
			return true
		}
		if failureOutcomeResolved(row, job.Attempts, runErr) {
			*settled = *row
			*settledAsSucceeded = false
			return true
		}
		return false
	}
	return "reconciling and recording outcome", writeFn, isResolved
}

func settleLockedOutcomeTx(
	ctx context.Context,
	txRepo storage.JobRepository,
	reconciler CheckpointReconciler,
	job *domain.Job,
	runErr error,
	now time.Time,
) (*domain.Job, bool, error) {
	row, err := txRepo.GetJobByIDForUpdate(ctx, job.ID)
	if err != nil {
		return nil, false, err
	}
	if row.State != domain.JobStateRunning {
		return nil, false, fmt.Errorf("%w: job %q is in state %s (expected RUNNING)", domain.ErrInvalidJobTransition, job.ID, row.State)
	}
	if row.Attempts != job.Attempts {
		return nil, false, fmt.Errorf("%w: job %q expected attempt %d, got %d", domain.ErrStaleJobAttempt, job.ID, job.Attempts, row.Attempts)
	}

	outcome, err := reconciler(*row)
	if err != nil {
		return nil, false, err
	}
	if outcome.Action == ReconcileSucceeded {
		updated, markErr := persistReconciledSuccess(ctx, txRepo, job.ID, job.Attempts, outcome.Checkpoint, now)
		return updated, true, markErr
	}
	updated, failErr := persistFailureOutcome(ctx, txRepo, job.ID, job.Attempts, runErr, now)
	return updated, false, failErr
}

func directOutcomePersistence(
	repo storage.JobRepository,
	job *domain.Job,
	runErr error,
	now time.Time,
	settled *domain.Job,
) (string, func(context.Context) error, func(*domain.Job) bool) {
	var label string
	var call func(context.Context) (*domain.Job, error)
	switch {
	case runErr == nil:
		label = "marking succeeded"
		call = func(writeCtx context.Context) (*domain.Job, error) {
			return repo.MarkSucceeded(writeCtx, job.ID, job.Attempts, now)
		}
	case errors.Is(runErr, ErrJobCancelled):
		label = "marking cancelled"
		call = func(writeCtx context.Context) (*domain.Job, error) {
			msg := runErr.Error()
			return repo.MarkCancelled(writeCtx, job.ID, job.Attempts, &msg, now)
		}
	case errors.Is(runErr, ErrPermanentFailure):
		label = "marking permanently failed"
		call = func(writeCtx context.Context) (*domain.Job, error) {
			return repo.MarkFailed(writeCtx, job.ID, job.Attempts, runErr.Error(), now)
		}
	default:
		label = "recording failure"
		call = func(writeCtx context.Context) (*domain.Job, error) {
			return repo.FailOrRetry(writeCtx, job.ID, job.Attempts, runErr.Error(), time.Time{}, now)
		}
	}

	writeFn := func(writeCtx context.Context) error {
		updated, err := call(writeCtx)
		if err == nil && updated != nil {
			*settled = *updated
		}
		return err
	}
	isResolved := func(row *domain.Job) bool {
		var ok bool
		if runErr == nil {
			ok = (row.State == domain.JobStateSucceeded && row.Attempts == job.Attempts) || row.Attempts > job.Attempts
		} else {
			ok = failureOutcomeResolved(row, job.Attempts, runErr)
		}
		if ok {
			*settled = *row
		}
		return ok
	}
	return label, writeFn, isResolved
}

func persistReconciledSuccess(
	ctx context.Context,
	repo storage.JobRepository,
	jobID string,
	attempt int,
	checkpoint json.RawMessage,
	now time.Time,
) (*domain.Job, error) {
	if len(checkpoint) > 0 {
		return repo.RecoverSucceeded(ctx, jobID, attempt, checkpoint, now)
	}
	return repo.MarkSucceeded(ctx, jobID, attempt, now)
}

func persistFailureOutcome(
	ctx context.Context,
	repo storage.JobRepository,
	jobID string,
	attempt int,
	runErr error,
	now time.Time,
) (*domain.Job, error) {
	switch {
	case errors.Is(runErr, ErrJobCancelled):
		msg := runErr.Error()
		return repo.MarkCancelled(ctx, jobID, attempt, &msg, now)
	case errors.Is(runErr, ErrPermanentFailure):
		return repo.MarkFailed(ctx, jobID, attempt, runErr.Error(), now)
	default:
		return repo.FailOrRetry(ctx, jobID, attempt, runErr.Error(), time.Time{}, now)
	}
}

func failureOutcomeResolved(row *domain.Job, attempt int, runErr error) bool {
	if row.Attempts > attempt {
		return true
	}
	switch {
	case errors.Is(runErr, ErrJobCancelled):
		return row.State == domain.JobStateCancelled && row.Attempts >= attempt
	case errors.Is(runErr, ErrPermanentFailure):
		return (row.State == domain.JobStateFailed || row.State == domain.JobStateCancelled) && row.Attempts >= attempt
	default:
		return (row.State == domain.JobStateQueued && row.Attempts == attempt) ||
			((row.State == domain.JobStateFailed || row.State == domain.JobStateCancelled) && row.Attempts >= attempt)
	}
}

func (r *Runner) persistOutcomeWithRetry(
	ctx context.Context,
	job *domain.Job,
	label string,
	writeFn func(context.Context) error,
	isResolved func(*domain.Job) bool,
) error {
	var lastErr error
	maxAttempts := 1 + len(defaultOutcomeRetryDelays)
	for attemptIdx := range maxAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := writeFn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if errors.Is(err, domain.ErrInvalidJobTransition) || errors.Is(err, domain.ErrStaleJobAttempt) {
			row, getErr := r.repo.GetJobByID(ctx, job.ID)
			if getErr == nil {
				if isResolved(row) {
					return nil
				}
				if row.State != domain.JobStateRunning || row.Attempts != job.Attempts {
					return fmt.Errorf("%w: %s job %q (attempt %d) ended in unexpected state %s (attempt %d): %w",
						ErrOutcomePersistenceFailed, label, job.ID, job.Attempts, row.State, row.Attempts, err)
				}
			}
		}

		if attemptIdx >= len(defaultOutcomeRetryDelays) {
			break
		}
		timer := time.NewTimer(defaultOutcomeRetryDelays[attemptIdx])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return fmt.Errorf("%w: %s job %q (attempt %d): %w", ErrOutcomePersistenceFailed, label, job.ID, job.Attempts, lastErr)
}
