package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
)

// ModuleName is the canonical compiled-in module name for the library scanner (ADR-0018 §1).
const ModuleName = "scanner"

// ScanJobPayload is the JSON payload persisted on durable SCAN jobs (ADR-0003 §7, ADR-0017 §1).
type ScanJobPayload struct {
	LibraryID string `json:"library_id"`
}

// ScanDedupeKey returns the active-job deduplication key for scanning libraryID (ADR-0017 §1).
func ScanDedupeKey(libraryID string) (string, error) {
	return domain.ScanJobDedupeKey(libraryID)
}

// JobTrigger enqueues durable background jobs (implemented by *jobs.Service).
type JobTrigger interface {
	Trigger(ctx context.Context, req jobs.TriggerRequest) (string, error)
}

// DefaultScanMaxAttempts is the default attempt budget for durable SCAN jobs (ADR-0017 §1, §7).
// Interrupted attempts count toward max_attempts on startup recovery, so 3 attempts allows
// two restarts or crashes to recover cleanly before failing a third interrupted attempt.
const DefaultScanMaxAttempts = domain.DefaultJobMaxAttempts

// TriggerScanOptions configures an individual library scan job trigger.
type TriggerScanOptions struct {
	CreatedBy   *string
	MaxAttempts int
	RunAfter    time.Time
}

// TriggerScan enqueues a deduplicated durable SCAN job for libraryID via trigger (ADR-0017 §§1–2),
// rejecting with app.ErrConflict if a RELOCATE_ROOT job is active for libraryID (M1B-D7).
func TriggerScan(ctx context.Context, trigger JobTrigger, libraryID string, opts TriggerScanOptions) (string, error) {
	if trigger == nil {
		return "", fmt.Errorf("%w: job trigger is unavailable", app.ErrServiceBusy)
	}
	dedupeKey, err := ScanDedupeKey(libraryID)
	if err != nil {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	relocateKey, err := domain.RelocateRootJobDedupeKey(libraryID)
	if err != nil {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	lockKey, err := domain.LibraryJobLockKey(libraryID)
	if err != nil {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	payload, err := json.Marshal(ScanJobPayload{LibraryID: libraryID})
	if err != nil {
		return "", fmt.Errorf("marshaling scan job payload: %w", err)
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultScanMaxAttempts
	}
	return trigger.Trigger(ctx, jobs.TriggerRequest{
		Kind:               domain.JobKindScan,
		Payload:            payload,
		DedupeKey:          &dedupeKey,
		LockKey:            lockKey,
		ConflictDedupeKeys: []string{relocateKey},
		MaxAttempts:        maxAttempts,
		CreatedBy:          opts.CreatedBy,
		RunAfter:           opts.RunAfter,
	})
}

// ModuleOptions configures the compiled-in scanner job module.
type ModuleOptions struct {
	// Workers caps concurrent file inspections within a single scan (M1B-D2).
	// Zero selects DefaultInspectionWorkers.
	Workers int

	// Formats selects the container format registry used during scans.
	// When nil, a default registry with cbz.Format() is created.
	Formats *container.FormatRegistry
}

// Module wires the library Scanner as a compiled-in module declaring the durable SCAN
// job kind, its CBZ container format, and its worker execution handler (ADR-0017, ADR-0018).
type Module struct {
	scanner *Scanner
	workers int
	formats *container.FormatRegistry
}

var (
	_ module.Module                  = (*Module)(nil)
	_ module.JobKindProvider         = (*Module)(nil)
	_ module.ContainerFormatProvider = (*Module)(nil)
	_ jobs.KindRegistrar             = (*Module)(nil)
)

// NewModule constructs a scanner module backed by sc.
func NewModule(sc *Scanner, opts ModuleOptions) (*Module, error) {
	if sc == nil {
		return nil, errors.New("scanner: scanner is required")
	}
	if opts.Workers < 0 {
		return nil, ErrInvalidWorkerCount
	}
	formats := opts.Formats
	if formats == nil {
		formats = container.NewFormatRegistry()
		if err := formats.Register(cbz.Format()); err != nil {
			return nil, fmt.Errorf("scanner: register cbz format: %w", err)
		}
	}
	return &Module{
		scanner: sc,
		workers: opts.Workers,
		formats: formats,
	}, nil
}

// Name returns the canonical module name ("scanner").
func (m *Module) Name() string {
	return ModuleName
}

// JobKinds declares the durable SCAN job kind and its RESTART recovery policy (ADR-0017 §7).
// Move, modify, and missing reconciliation require a complete seen-set over all roots, so a
// restarted attempt re-walks all roots while skipping unchanged committed files by size+mtime.
func (m *Module) JobKinds() []module.JobKind {
	return []module.JobKind{
		module.JobKindSpec{
			Kind:   domain.JobKindScan,
			Policy: domain.JobResumeRestart,
		},
	}
}

// ContainerFormats returns the container formats handled by the scanner module (ADR-0018 S8).
func (m *Module) ContainerFormats() []container.Format {
	return []container.Format{cbz.Format()}
}

// RegisterJobs attaches the SCAN execution handler to r.
func (m *Module) RegisterJobs(r *jobs.Runner) error {
	if r == nil {
		return errors.New("scanner: job runner is required")
	}
	if m == nil || m.scanner == nil {
		return errors.New("scanner: scanner is required")
	}
	return r.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeRestart,
		Handler:      jobs.HandlerFunc(m.handleScanJob),
	})
}

func decodeScanJobPayload(raw json.RawMessage) (ScanJobPayload, error) {
	var payload ScanJobPayload
	if len(bytes.TrimSpace(raw)) == 0 {
		return payload, fmt.Errorf("%w: scan job payload is required", jobs.ErrPermanentFailure)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf("%w: invalid scan job payload: %v", jobs.ErrPermanentFailure, err)
	}
	if strings.TrimSpace(payload.LibraryID) == "" {
		return payload, fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, domain.ErrEmptyLibraryID)
	}
	return payload, nil
}

func (m *Module) handleScanJob(ctx context.Context, exec *jobs.Execution) error {
	payload, err := decodeScanJobPayload(exec.Job().Payload)
	if err != nil {
		return err
	}

	res, err := m.scanner.ScanLibrary(ctx, payload.LibraryID, ScanOptions{
		Workers: m.workers,
		Formats: m.formats,
		OnProgress: func(scannedCount int64, currentPath string) {
			exec.ReportProgress(ctx, scannedCount, currentPath)
		},
		CheckCancel: func(checkCtx context.Context) error {
			cancelled, cancelErr := exec.CheckCancelled(checkCtx)
			if cancelErr != nil {
				return cancelErr
			}
			if cancelled {
				return jobs.ErrJobCancelled
			}
			return nil
		},
	})
	if err != nil {
		if errors.Is(err, jobs.ErrJobCancelled) {
			return err
		}
		if errors.Is(err, ErrLibraryNotFound) ||
			errors.Is(err, domain.ErrEmptyLibraryID) ||
			errors.Is(err, ErrInvalidWorkerCount) {
			return fmt.Errorf("%w: %w", jobs.ErrPermanentFailure, err)
		}
		return err
	}
	if !res.Offline && !res.Complete {
		return fmt.Errorf("%w: library %q", ErrIncompleteWalk, payload.LibraryID)
	}
	return nil
}
