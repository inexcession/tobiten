package roots

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

// ModuleName is the canonical compiled-in module name for root management jobs (ADR-0018 §1).
const ModuleName = "roots"

// DefaultRelocateRootMaxAttempts is the default attempt budget for durable RELOCATE_ROOT jobs.
const DefaultRelocateRootMaxAttempts = domain.DefaultJobMaxAttempts

const (
	maxRelocateVerifyPasses       = 3
	maxConfigLockContentionPasses = 8
)

var (
	errVerificationDrift   = errors.New("roots: target file set drifted after verification")
	errConfigLockContended = errors.New("roots: global configuration lock contended before activation")
)

var (
	// ErrMissingAssetFile indicates a required non-missing asset file was absent at the relocation target.
	ErrMissingAssetFile = errors.New("roots: required media asset file missing at relocation target")
	// ErrUnreadableAssetFile indicates an asset path at the relocation target could not be opened or read.
	ErrUnreadableAssetFile = errors.New("roots: media asset file unreadable at relocation target")
	// ErrDigestMismatch indicates a file at the relocation target failed partial fingerprint verification.
	ErrDigestMismatch = errors.New("roots: media asset fingerprint mismatch at relocation target")
	// ErrMissingVerificationDigest indicates an asset present at the target lacks a current XXH3_64_PARTIAL digest.
	ErrMissingVerificationDigest = errors.New("roots: asset has no verifiable XXH3_64_PARTIAL digest")
	// ErrTargetNotDirectory indicates the relocation target path does not exist or is not a directory.
	ErrTargetNotDirectory = errors.New("roots: relocation target is not a directory")
	// ErrTargetUnreadable indicates the relocation target directory could not be opened or read.
	ErrTargetUnreadable = errors.New("roots: relocation target directory is unreadable")
	// ErrRootLibraryMismatch indicates the persisted root belongs to a different library than the job payload.
	ErrRootLibraryMismatch = errors.New("roots: root library mismatch")
	// ErrRootIdentityChanged indicates the root path changed concurrently before relocation activation.
	ErrRootIdentityChanged = errors.New("roots: root path changed before activation")
	// ErrTargetIdentityChanged indicates the relocation target directory identity changed before activation.
	ErrTargetIdentityChanged = errors.New("roots: relocation target identity changed before activation")
)

// JobTrigger enqueues durable background jobs (implemented by *jobs.Service).
type JobTrigger interface {
	Trigger(ctx context.Context, req jobs.TriggerRequest) (string, error)
}

// RelocateRootPayload is the JSON payload persisted on durable RELOCATE_ROOT jobs (ADR-0017, ADR-0019 §7).
type RelocateRootPayload struct {
	LibraryID       string `json:"library_id"`
	RootID          string `json:"root_id"`
	TargetPathBytes []byte `json:"target_path_bytes"`
}

// RelocateRootCheckpoint records atomic activation of the relocated root path for the current
// job attempt so startup recovery reconciles a post-activation crash as SUCCEEDED (ADR-0017 §7).
type RelocateRootCheckpoint struct {
	Activated          bool   `json:"activated,omitempty"`
	Attempt            int    `json:"attempt,omitempty"`
	RootID             string `json:"root_id,omitempty"`
	ActivatedPathBytes []byte `json:"activated_path_bytes,omitempty"`
}

// TriggerRelocateOptions configures a durable RELOCATE_ROOT job trigger.
type TriggerRelocateOptions struct {
	CreatedBy   *string
	MaxAttempts int
	RunAfter    time.Time
}

// TriggerRelocateRoot enqueues a durable RELOCATE_ROOT job for libraryID/rootID via trigger,
// returning app.ErrConflict if an active SCAN or RELOCATE_ROOT job already reserves libraryID.
func TriggerRelocateRoot(
	ctx context.Context,
	trigger JobTrigger,
	libraryID string,
	rootID string,
	targetPathBytes []byte,
	opts TriggerRelocateOptions,
) (string, error) {
	if trigger == nil {
		return "", fmt.Errorf("%w: job trigger is unavailable", app.ErrServiceBusy)
	}
	req, err := buildRelocateTriggerRequest(libraryID, rootID, targetPathBytes, opts)
	if err != nil {
		return "", err
	}
	return trigger.Trigger(ctx, req)
}

func buildRelocateTriggerRequest(
	libraryID string,
	rootID string,
	targetPathBytes []byte,
	opts TriggerRelocateOptions,
) (jobs.TriggerRequest, error) {
	if strings.TrimSpace(rootID) == "" {
		return jobs.TriggerRequest{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyLibraryRootID)
	}
	if err := domain.ValidateRootPathBytes(targetPathBytes); err != nil {
		return jobs.TriggerRequest{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	relocateKey, err := domain.RelocateRootJobDedupeKey(libraryID)
	if err != nil {
		return jobs.TriggerRequest{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	scanKey, err := domain.ScanJobDedupeKey(libraryID)
	if err != nil {
		return jobs.TriggerRequest{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	lockKey, err := domain.LibraryJobLockKey(libraryID)
	if err != nil {
		return jobs.TriggerRequest{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}

	payload, err := json.Marshal(RelocateRootPayload{
		LibraryID:       libraryID,
		RootID:          rootID,
		TargetPathBytes: slices.Clone(targetPathBytes),
	})
	if err != nil {
		return jobs.TriggerRequest{}, fmt.Errorf("marshaling relocate root payload: %w", err)
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultRelocateRootMaxAttempts
	}

	return jobs.TriggerRequest{
		Kind:               domain.JobKindRelocateRoot,
		Payload:            payload,
		DedupeKey:          &relocateKey,
		LockKey:            lockKey,
		ConflictDedupeKeys: []string{scanKey, relocateKey},
		MaxAttempts:        maxAttempts,
		CreatedBy:          opts.CreatedBy,
		RunAfter:           opts.RunAfter,
	}, nil
}

// RelocateRootReconciler returns the startup recovery reconciler for interrupted
// RELOCATE_ROOT jobs (ADR-0017 §7). Committed activation for the interrupted attempt
// reconciles to SUCCEEDED; pre-activation interruptions requeue/restart within budget.
func RelocateRootReconciler() domain.JobReconciler {
	return domain.JobReconcileFunc(func(_ context.Context, job domain.Job) (domain.JobReconcileOutcome, error) {
		return reconcileRelocateRootCheckpoint(job)
	})
}

func reconcileRelocateRootCheckpoint(job domain.Job) (domain.JobReconcileOutcome, error) {
	if len(bytes.TrimSpace(job.Checkpoint)) == 0 {
		return domain.JobReconcileOutcome{Action: domain.JobReconcileRequeue}, nil
	}
	var cp RelocateRootCheckpoint
	if err := json.Unmarshal(job.Checkpoint, &cp); err != nil {
		return domain.JobReconcileOutcome{}, fmt.Errorf("decoding relocate root checkpoint: %w", err)
	}
	if cp.Activated && cp.Attempt == job.Attempts {
		return domain.JobReconcileOutcome{Action: domain.JobReconcileSucceeded}, nil
	}
	return domain.JobReconcileOutcome{Action: domain.JobReconcileRequeue}, nil
}

// RelocatorOptions configures read-only target verification and atomic activation for RELOCATE_ROOT.
type RelocatorOptions struct {
	Now             func() time.Time
	Open            func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error)
	Stat            func(rawPath []byte) (os.FileInfo, error)
	Lstat           func(rawPath []byte) (os.FileInfo, error)
	OpenDir         func(rawDir []byte) error
	OnBeforeVerify  func(ctx context.Context, job domain.Job) error
	OnAfterVerify   func(ctx context.Context, job domain.Job) error
	OnAfterActivate func(ctx context.Context, job domain.Job) error
}

type statReadAtCloser interface {
	Stat() (os.FileInfo, error)
}

// Relocator executes durable RELOCATE_ROOT jobs by verifying asset fingerprints at the
// target path outside any transaction and atomically committing the new root path with
// an activation checkpoint (ADR-0017 §7, ADR-0019 §7).
type Relocator struct {
	tx              storage.Transactor
	now             func() time.Time
	open            func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error)
	stat            func(rawPath []byte) (os.FileInfo, error)
	lstat           func(rawPath []byte) (os.FileInfo, error)
	openDir         func(rawDir []byte) error
	customStat      bool
	onBeforeVerify  func(ctx context.Context, job domain.Job) error
	onAfterVerify   func(ctx context.Context, job domain.Job) error
	onAfterActivate func(ctx context.Context, job domain.Job) error
}

// NewRelocator constructs a Relocator backed by tx.
func NewRelocator(tx storage.Transactor, opts RelocatorOptions) (*Relocator, error) {
	if tx == nil {
		return nil, errors.New("roots: transactor is required")
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	openFn := opts.Open
	if openFn == nil {
		openFn = func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return openRawReadOnlyNonBlock(rawPath)
		}
	}
	customStat := opts.Stat != nil
	statFn := opts.Stat
	if statFn == nil {
		statFn = func(rawPath []byte) (os.FileInfo, error) {
			return os.Stat(string(rawPath))
		}
	}
	lstatFn := opts.Lstat
	if lstatFn == nil {
		lstatFn = func(rawPath []byte) (os.FileInfo, error) {
			return os.Lstat(string(rawPath))
		}
	}
	openDirFn := opts.OpenDir
	if openDirFn == nil && !customStat {
		openDirFn = checkRawDirReadable
	}

	return &Relocator{
		tx:              tx,
		now:             nowFn,
		open:            openFn,
		stat:            statFn,
		lstat:           lstatFn,
		openDir:         openDirFn,
		customStat:      customStat,
		onBeforeVerify:  opts.OnBeforeVerify,
		onAfterVerify:   opts.OnAfterVerify,
		onAfterActivate: opts.OnAfterActivate,
	}, nil
}

// Module wires RELOCATE_ROOT job registration, recovery reconciliation, and execution
// into the module registry and job runner (ADR-0017, ADR-0018).
type Module struct {
	relocator *Relocator
}

var (
	_ module.Module          = (*Module)(nil)
	_ module.JobKindProvider = (*Module)(nil)
	_ jobs.KindRegistrar     = (*Module)(nil)
)

// NewModule constructs a roots Module backed by relocator.
func NewModule(relocator *Relocator) (*Module, error) {
	if relocator == nil {
		return nil, errors.New("roots: relocator is required")
	}
	return &Module{relocator: relocator}, nil
}

// Name returns the canonical module name ("roots").
func (m *Module) Name() string {
	return ModuleName
}

// JobKinds declares the durable RELOCATE_ROOT job kind, its RESTART recovery policy,
// and its required startup reconciliation hook (ADR-0017 §7).
func (m *Module) JobKinds() []module.JobKind {
	return []module.JobKind{
		module.JobKindSpec{
			Kind:             domain.JobKindRelocateRoot,
			Policy:           domain.JobResumeRestart,
			RequireReconcile: true,
			ReconcileHook:    RelocateRootReconciler(),
		},
	}
}

// RegisterJobs attaches the RELOCATE_ROOT execution handler to r.
func (m *Module) RegisterJobs(r *jobs.Runner) error {
	if r == nil {
		return errors.New("roots: job runner is required")
	}
	if m == nil || m.relocator == nil {
		return errors.New("roots: relocator is required")
	}
	cfg := jobs.KindConfig{
		Kind:                 domain.JobKindRelocateRoot,
		ResumePolicy:         domain.JobResumeRestart,
		RequiresReconciler:   true,
		Handler:              jobs.HandlerFunc(m.handleRelocateRootJob),
		CheckpointReconciler: reconcileRelocateRootCheckpoint,
	}
	if _, already := r.KindPolicy(domain.JobKindRelocateRoot); !already {
		cfg.Reconciler = RelocateRootReconciler()
	}
	return r.RegisterKind(cfg)
}

func decodeRelocateRootPayload(raw json.RawMessage) (RelocateRootPayload, error) {
	var payload RelocateRootPayload
	if len(bytes.TrimSpace(raw)) == 0 {
		return payload, fmt.Errorf("%w: relocate root payload is required", jobs.ErrPermanentFailure)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf("%w: invalid relocate root payload: %v", jobs.ErrPermanentFailure, err)
	}
	if strings.TrimSpace(payload.LibraryID) == "" {
		return payload, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, domain.ErrEmptyLibraryID)
	}
	if strings.TrimSpace(payload.RootID) == "" {
		return payload, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, domain.ErrEmptyLibraryRootID)
	}
	if err := domain.ValidateRootPathBytes(payload.TargetPathBytes); err != nil {
		return payload, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
	}
	return payload, nil
}

func (m *Module) handleRelocateRootJob(ctx context.Context, exec *jobs.Execution) error {
	return m.relocator.Execute(ctx, exec.Job(), func(checkCtx context.Context) error {
		cancelled, err := exec.CheckCancelled(checkCtx)
		if err != nil {
			return err
		}
		if cancelled {
			return jobs.ErrJobCancelled
		}
		return nil
	})
}

type relocateAssetObservation struct {
	absPath []byte
	present bool
	info    os.FileInfo
	size    int64
	modTime time.Time
}

type relocateSnapshot struct {
	root          domain.LibraryRoot
	assets        []domain.MediaAsset
	canonTarget   []byte
	targetDirInfo os.FileInfo
	observations  []relocateAssetObservation
}

// Execute verifies target asset fingerprints read-only outside the activation
// transaction, rechecks canonical overlap and cancellation, binds activation to
// the verified target files, and atomically commits the new root path together
// with the attempt's activation checkpoint.
func (r *Relocator) Execute(ctx context.Context, job domain.Job, checkCancel func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.tx == nil {
		return errors.New("roots: relocator is not initialized")
	}
	payload, err := decodeRelocateRootPayload(job.Payload)
	if err != nil {
		return err
	}

	driftPasses := 0
	lockPasses := 0
	firstPass := true
	for {
		err := r.executeVerifyAndActivatePass(ctx, job, payload, firstPass, checkCancel)
		firstPass = false
		if err == nil {
			if r.onAfterActivate != nil {
				if hookErr := r.onAfterActivate(ctx, job); hookErr != nil {
					return hookErr
				}
			}
			return nil
		}
		switch {
		case errors.Is(err, errVerificationDrift):
			driftPasses++
			if driftPasses >= maxRelocateVerifyPasses {
				return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, ErrTargetIdentityChanged)
			}
		case errors.Is(err, errConfigLockContended):
			lockPasses++
			if lockPasses >= maxConfigLockContentionPasses {
				return fmt.Errorf("%w: %w", app.ErrServiceBusy, err)
			}
			if waitErr := r.waitForConfigLockRelease(ctx); waitErr != nil {
				return waitErr
			}
		default:
			return err
		}
	}
}

func (r *Relocator) executeVerifyAndActivatePass(
	ctx context.Context,
	job domain.Job,
	payload RelocateRootPayload,
	runHooks bool,
	checkCancel func(context.Context) error,
) error {
	if err := observeRelocateCancel(ctx, checkCancel); err != nil {
		return err
	}

	snap, err := r.prepareVerificationSnapshot(ctx, payload)
	if err != nil {
		return err
	}
	if runHooks && r.onBeforeVerify != nil {
		if err := r.onBeforeVerify(ctx, job); err != nil {
			return err
		}
	}
	observations, err := r.verifyAllAssets(ctx, snap, checkCancel)
	if err != nil {
		return err
	}
	snap.observations = observations

	if runHooks && r.onAfterVerify != nil {
		if err := r.onAfterVerify(ctx, job); err != nil {
			return err
		}
	}
	if err := r.verifyObservationsBeforeActivation(ctx, snap.observations, checkCancel); err != nil {
		return err
	}
	if err := observeRelocateCancel(ctx, checkCancel); err != nil {
		return err
	}
	return r.activateRelocation(ctx, job, payload, snap)
}

func (r *Relocator) prepareVerificationSnapshot(ctx context.Context, payload RelocateRootPayload) (relocateSnapshot, error) {
	var snap relocateSnapshot
	err := r.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		libRepo := tx.Libraries()
		cw := tx.CatalogueWrites()
		if libRepo == nil || cw == nil {
			return errors.New("roots: library and catalogue write repositories are required")
		}
		root, err := libRepo.GetLibraryRootByID(ctx, payload.RootID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
			}
			return err
		}
		if root.LibraryID != payload.LibraryID {
			return fmt.Errorf("%w: %w (expected %q, got %q)",
				jobs.ErrPermanentFailure, ErrRootLibraryMismatch, payload.LibraryID, root.LibraryID)
		}
		existing, err := CollectExistingPaths(ctx, libRepo, CollectOptions{
			ExcludeRootID: root.ID,
		})
		if err != nil {
			return err
		}
		canonTarget, err := CheckPathOverlap(payload.TargetPathBytes, existing)
		if err != nil {
			if errors.Is(err, app.ErrInvalidInput) || errors.Is(err, ErrPathOverlap) {
				return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
			}
			return err
		}
		assets, err := cw.ListAssetsByRoot(ctx, root.ID)
		if err != nil {
			return err
		}
		snap = relocateSnapshot{
			root:        *root,
			assets:      assets,
			canonTarget: canonTarget,
		}
		return nil
	})
	if err != nil {
		return relocateSnapshot{}, err
	}

	dirInfo, err := r.checkTargetDirectory(snap.canonTarget)
	if err != nil {
		return relocateSnapshot{}, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
	}
	snap.targetDirInfo = dirInfo
	return snap, nil
}

func (r *Relocator) verifyAllAssets(
	ctx context.Context,
	snap relocateSnapshot,
	checkCancel func(context.Context) error,
) ([]relocateAssetObservation, error) {
	observations := make([]relocateAssetObservation, 0, len(snap.assets))
	for _, asset := range snap.assets {
		if err := observeRelocateCancel(ctx, checkCancel); err != nil {
			return nil, err
		}
		absPath := joinAbsoluteBytes(snap.canonTarget, asset.RelPathBytes)
		obs, err := r.verifyOneAsset(ctx, asset, absPath)
		if err != nil {
			return nil, err
		}
		observations = append(observations, obs)
	}
	return observations, nil
}

func (r *Relocator) verifyOneAsset(ctx context.Context, asset domain.MediaAsset, absPath []byte) (relocateAssetObservation, error) {
	expected, hasFingerprint := asset.CurrentDigest(domain.DigestKindXXH364Partial)
	required := asset.IsAvailable()

	f, err := r.open(ctx, absPath)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return relocateAssetObservation{}, ctxErr
		}
		if classErr := r.classifyOpenFailure(absPath, required, err); classErr != nil {
			return relocateAssetObservation{}, classErr
		}
		return relocateAssetObservation{absPath: absPath, present: false}, nil
	}
	defer func() {
		_ = f.Close()
	}()

	info, err := r.resolveOpenedRegularFileInfo(absPath, f)
	if err != nil {
		return relocateAssetObservation{}, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
	}
	if !hasFingerprint {
		return relocateAssetObservation{}, fmt.Errorf("%w: asset %s (%s)", jobs.ErrPermanentFailure, ErrMissingVerificationDigest, asset.RelPathDisplay)
	}

	actual, err := scanner.PartialFingerprint(f, info.Size())
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return relocateAssetObservation{}, ctxErr
		}
		return relocateAssetObservation{}, fmt.Errorf("%w: %w: %w", jobs.ErrPermanentFailure, ErrUnreadableAssetFile, err)
	}
	if actual != expected.Value {
		return relocateAssetObservation{}, fmt.Errorf("%w: %w for asset %s (want %s, got %s)",
			jobs.ErrPermanentFailure, ErrDigestMismatch, asset.RelPathDisplay, expected.Value, actual)
	}
	return relocateAssetObservation{
		absPath: absPath,
		present: true,
		info:    info,
		size:    info.Size(),
		modTime: info.ModTime(),
	}, nil
}

func (r *Relocator) classifyOpenFailure(absPath []byte, required bool, openErr error) error {
	if isAbsentPathError(openErr) {
		_, lerr := r.lstat(absPath)
		if lerr == nil {
			// A symlink or entry exists at absPath even though Open reported not-exist (broken symlink).
			return fmt.Errorf("%w: %w: %w", jobs.ErrPermanentFailure, ErrUnreadableAssetFile, openErr)
		}
		if isAbsentPathError(lerr) {
			if required {
				return fmt.Errorf("%w: %w: %w", jobs.ErrPermanentFailure, ErrMissingAssetFile, openErr)
			}
			return nil
		}
		return fmt.Errorf("%w: %w: %w", jobs.ErrPermanentFailure, ErrUnreadableAssetFile, lerr)
	}
	return fmt.Errorf("%w: %w: %w", jobs.ErrPermanentFailure, ErrUnreadableAssetFile, openErr)
}

func (r *Relocator) resolveOpenedRegularFileInfo(absPath []byte, f scanner.ReadAtCloser) (os.FileInfo, error) {
	if statable, ok := f.(statReadAtCloser); ok {
		info, err := statable.Stat()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnreadableAssetFile, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: %s is not a regular file", ErrUnreadableAssetFile, absPath)
		}
		return info, nil
	}
	info, err := r.stat(absPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnreadableAssetFile, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrUnreadableAssetFile, absPath)
	}
	return info, nil
}

func (r *Relocator) activateRelocation(
	ctx context.Context,
	job domain.Job,
	payload RelocateRootPayload,
	snap relocateSnapshot,
) error {
	return r.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		libRepo := tx.Libraries()
		jobRepo := tx.Jobs()
		if libRepo == nil || jobRepo == nil {
			return errors.New("roots: library and job repositories are required")
		}
		if err := lockAndVerifyRelocateLibrary(ctx, libRepo, payload, snap.root.Path.Bytes); err != nil {
			return err
		}
		reCanonTarget, err := r.recheckActivationTarget(ctx, libRepo, payload.RootID, payload.TargetPathBytes, snap)
		if err != nil {
			return err
		}
		return r.commitRelocateActivation(ctx, libRepo, jobRepo, job, payload.RootID, reCanonTarget)
	})
}

func (r *Relocator) waitForConfigLockRelease(ctx context.Context) error {
	return r.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		libRepo := tx.Libraries()
		if libRepo == nil {
			return errors.New("roots: library repository is required")
		}
		return libRepo.LockConfiguration(ctx)
	})
}

func lockAndVerifyRelocateLibrary(
	ctx context.Context,
	libRepo storage.LibraryRepository,
	payload RelocateRootPayload,
	expectedOldPath []byte,
) error {
	acquired, err := libRepo.TryLockConfiguration(ctx)
	if err != nil {
		return err
	}
	if !acquired {
		return errConfigLockContended
	}
	if err := libRepo.LockLibrary(ctx, payload.LibraryID); err != nil {
		return wrapPermanentNotFound(err)
	}
	currentRoot, err := libRepo.GetLibraryRootByID(ctx, payload.RootID)
	if err != nil {
		return wrapPermanentNotFound(err)
	}
	if currentRoot.LibraryID != payload.LibraryID {
		return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, ErrRootLibraryMismatch)
	}
	if !bytes.Equal(currentRoot.Path.Bytes, expectedOldPath) {
		return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, ErrRootIdentityChanged)
	}
	return nil
}

func wrapPermanentNotFound(err error) error {
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
	}
	return err
}

func (r *Relocator) commitRelocateActivation(
	ctx context.Context,
	libRepo storage.LibraryRepository,
	jobRepo storage.JobRepository,
	job domain.Job,
	rootID string,
	reCanonTarget []byte,
) error {
	jobRow, err := jobRepo.GetJobByIDForUpdate(ctx, job.ID)
	if err != nil {
		return err
	}
	if jobRow.CancelRequested {
		return jobs.ErrJobCancelled
	}
	if jobRow.State != domain.JobStateRunning || jobRow.Attempts != job.Attempts {
		return fmt.Errorf("%w: relocate job %q state=%s attempt=%d",
			domain.ErrStaleJobAttempt, job.ID, jobRow.State, jobRow.Attempts)
	}

	newPathID, err := domain.NewRootPathIdentity(reCanonTarget)
	if err != nil {
		return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
	}
	if _, err := libRepo.UpdateLibraryRootPath(ctx, rootID, newPathID); err != nil {
		return err
	}

	cpBytes, err := json.Marshal(RelocateRootCheckpoint{
		Activated:          true,
		Attempt:            job.Attempts,
		RootID:             rootID,
		ActivatedPathBytes: reCanonTarget,
	})
	if err != nil {
		return fmt.Errorf("marshaling relocate activation checkpoint: %w", err)
	}
	_, err = jobRepo.SaveCheckpoint(ctx, job.ID, job.Attempts, cpBytes, r.now().UTC())
	return err
}

func (r *Relocator) recheckActivationTarget(
	ctx context.Context,
	libRepo storage.LibraryRepository,
	rootID string,
	rawTarget []byte,
	snap relocateSnapshot,
) ([]byte, error) {
	existing, err := CollectExistingPaths(ctx, libRepo, CollectOptions{
		ExcludeRootID: rootID,
	})
	if err != nil {
		return nil, err
	}
	reCanonTarget, err := CheckPathOverlap(rawTarget, existing)
	if err != nil {
		if errors.Is(err, app.ErrInvalidInput) || errors.Is(err, ErrPathOverlap) {
			return nil, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
		}
		return nil, err
	}
	if !bytes.Equal(reCanonTarget, snap.canonTarget) {
		return nil, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, ErrTargetIdentityChanged)
	}
	curInfo, err := r.checkTargetDirectory(reCanonTarget)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
	}
	if !r.customStat && !os.SameFile(snap.targetDirInfo, curInfo) {
		return nil, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, ErrTargetIdentityChanged)
	}
	return reCanonTarget, nil
}

func (r *Relocator) verifyObservationsBeforeActivation(
	ctx context.Context,
	observations []relocateAssetObservation,
	checkCancel func(context.Context) error,
) error {
	for _, obs := range observations {
		if err := observeRelocateCancel(ctx, checkCancel); err != nil {
			return err
		}
		if err := r.checkObservationStillValid(obs); err != nil {
			return err
		}
	}
	return nil
}

func (r *Relocator) checkObservationStillValid(obs relocateAssetObservation) error {
	if !obs.present {
		_, lerr := r.lstat(obs.absPath)
		if isAbsentPathError(lerr) {
			return nil
		}
		return errVerificationDrift
	}

	curInfo, err := r.stat(obs.absPath)
	if err != nil || !curInfo.Mode().IsRegular() {
		return errVerificationDrift
	}
	if curInfo.Size() != obs.size || !curInfo.ModTime().Equal(obs.modTime) {
		return errVerificationDrift
	}
	if !r.customStat && !sameFileIdentityAndChangeTime(obs.info, curInfo) {
		return errVerificationDrift
	}
	return nil
}

func (r *Relocator) checkTargetDirectory(target []byte) (os.FileInfo, error) {
	info, err := r.stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %w", ErrTargetNotDirectory, err)
		}
		return nil, fmt.Errorf("%w: %w", ErrTargetUnreadable, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrTargetNotDirectory, target)
	}
	if r.openDir != nil {
		if err := r.openDir(target); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTargetUnreadable, err)
		}
	}
	return info, nil
}

func observeRelocateCancel(ctx context.Context, checkCancel func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if checkCancel != nil {
		return checkCancel(ctx)
	}
	return nil
}

func openRawReadOnlyNonBlock(rawPath []byte) (*os.File, error) {
	if len(rawPath) == 0 || bytes.IndexByte(rawPath, 0) >= 0 {
		return nil, paths.ErrInvalidRawPath
	}
	return os.OpenFile(string(rawPath), os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

func checkRawDirReadable(rawDir []byte) error {
	f, err := openRawReadOnlyNonBlock(rawDir)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return ErrTargetNotDirectory
	}
	_, readErr := f.ReadDir(1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	return nil
}

func isAbsentPathError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

func joinAbsoluteBytes(dir, rel []byte) []byte {
	trimmed := bytes.TrimRight(dir, "/")
	if len(trimmed) == 0 {
		out := make([]byte, 1+len(rel))
		out[0] = '/'
		copy(out[1:], rel)
		return out
	}
	out := make([]byte, len(trimmed)+1+len(rel))
	copy(out, trimmed)
	out[len(trimmed)] = '/'
	copy(out[len(trimmed)+1:], rel)
	return out
}
