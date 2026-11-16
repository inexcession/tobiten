package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Common domain errors for runtime job and domain-event types.
var (
	ErrEmptyJobID                    = errors.New("job ID cannot be empty")
	ErrEmptyJobKind                  = errors.New("job kind cannot be empty")
	ErrInvalidJobState               = errors.New("invalid job state")
	ErrInvalidJobResumePolicy        = errors.New("invalid job resume policy")
	ErrInvalidJobTransition          = errors.New("invalid job state transition")
	ErrStaleJobAttempt               = errors.New("stale job attempt")
	ErrInvalidJobAttempts            = errors.New("job attempts must be non-negative and max_attempts must be positive within 32-bit integer bounds")
	ErrInvalidJobJSON                = errors.New("job payload and checkpoint must be valid JSON")
	ErrEmptyDedupeKey                = errors.New("job dedupe_key cannot be empty when present")
	ErrEmptyEventKind                = errors.New("domain event kind cannot be empty")
	ErrInvalidEventVersion           = errors.New("domain event version must be within 1..MaxInt32")
	ErrEmptyAggregateType            = errors.New("domain event aggregate_type cannot be empty")
	ErrEmptyAggregateID              = errors.New("domain event aggregate_id cannot be empty")
	ErrInvalidEventPayload           = errors.New("domain event payload must be a valid JSON object")
	ErrUnexpectedPublicationMetadata = errors.New("unpublished outbox event must not set published_sequence or published_at")
	ErrUnexpectedEventID             = errors.New("new outbox event must not preset internal id")
	ErrInternalJobEventSuppressed    = errors.New("internal EVENT_DELIVERY jobs do not emit lifecycle events")
	ErrInvalidJobLifecycleEvent      = errors.New("invalid job lifecycle event kind or state")
	ErrInvalidPublicationPair        = errors.New("published_sequence and published_at must both be set or both be nil")
	ErrInvalidPublishedSeq           = errors.New("published_sequence must be positive")
	ErrEmptySubscriberID             = errors.New("event subscriber ID cannot be empty")
	ErrEmptySubscriberKind           = errors.New("event subscriber kind cannot be empty")
	ErrInvalidSubscriberState        = errors.New("invalid event subscriber state")
	ErrInvalidSubscriberPosition     = errors.New("event subscriber position cannot be negative")
	ErrInvalidStalledPair            = errors.New("subscriber state STALLED and stalled_at must be set together")
	ErrSubscriberNotActive           = errors.New("event subscriber is not active")
	ErrCursorSequenceMismatch        = errors.New("event subscriber cursor cannot skip earlier published events")
	ErrInvalidDeliveryPayload        = errors.New("invalid event delivery job payload")
	ErrInvalidDeliveryOutcome        = errors.New("invalid event delivery outcome")
	ErrInvalidEventDeliveryDedupeKey = errors.New("EVENT_DELIVERY job must use canonical subscriber dedupe_key")
	ErrEventDeliveryBlocked          = errors.New("event subscriber delivery blocked by terminal failure")
)

// DefaultJobMaxAttempts is the default maximum execution attempts for a durable job.
const DefaultJobMaxAttempts = 3

// JobKind identifies a durable background job handler kind (ADR-0017).
type JobKind string

const (
	JobKindScan          JobKind = "SCAN"
	JobKindIntake        JobKind = "INTAKE"
	JobKindRepack        JobKind = "REPACK"
	JobKindWriteBack     JobKind = "WRITE_BACK"
	JobKindScrape        JobKind = "SCRAPE"
	JobKindPurge         JobKind = "PURGE"
	JobKindEventDelivery JobKind = "EVENT_DELIVERY"
)

// IsInternal reports whether this job kind is an internal delivery job that must suppress
// its own lifecycle outbox events to avoid recursive delivery (ADR-0018 S3).
func (k JobKind) IsInternal() bool {
	return k == JobKindEventDelivery
}

// JobResumePolicy declares how an interrupted RUNNING job of a given kind
// is handled during startup recovery (ADR-0017 §1, §7).
type JobResumePolicy string

const (
	// JobResumeRestart discards partial checkpoint progress and restarts the job from the beginning on recovery.
	JobResumeRestart JobResumePolicy = "RESTART"
	// JobResumeCheckpoint preserves the persisted checkpoint so the job resumes from it on recovery.
	JobResumeCheckpoint JobResumePolicy = "RESUME_FROM_CHECKPOINT"
)

// IsValid reports whether p is a recognized JobResumePolicy.
func (p JobResumePolicy) IsValid() bool {
	return p == JobResumeRestart || p == JobResumeCheckpoint
}

// JobReconcileAction describes the state transition chosen during startup recovery
// for an interrupted RUNNING job (ADR-0017 §7).
type JobReconcileAction int

const (
	// JobReconcileRequeue returns an interrupted job to QUEUED (unless cancelled or exhausted).
	JobReconcileRequeue JobReconcileAction = iota
	// JobReconcileSucceeded records that the interrupted job completed its final step before shutdown.
	JobReconcileSucceeded
	// JobReconcileFailed marks the interrupted job as permanently FAILED.
	JobReconcileFailed
	// JobReconcileCancelled marks the interrupted job as CANCELLED.
	JobReconcileCancelled
)

// JobReconcileOutcome describes the reconciliation decision for an interrupted RUNNING job.
type JobReconcileOutcome struct {
	Action     JobReconcileAction
	Checkpoint json.RawMessage
	Error      string
}

// JobReconciler inspects an interrupted RUNNING job before startup recovery updates its state.
type JobReconciler interface {
	Reconcile(ctx context.Context, job Job) (JobReconcileOutcome, error)
}

// JobReconcileFunc adapts a function to the JobReconciler interface.
type JobReconcileFunc func(ctx context.Context, job Job) (JobReconcileOutcome, error)

// Reconcile implements JobReconciler.
func (f JobReconcileFunc) Reconcile(ctx context.Context, job Job) (JobReconcileOutcome, error) {
	return f(ctx, job)
}

// JobState represents the execution state of a durable job (ADR-0017, 0001_init.sql).
type JobState string

const (
	JobStateQueued    JobState = "QUEUED"
	JobStateRunning   JobState = "RUNNING"
	JobStateSucceeded JobState = "SUCCEEDED"
	JobStateFailed    JobState = "FAILED"
	JobStateCancelled JobState = "CANCELLED"
)

// ValidJobStates returns all defined JobState values.
func ValidJobStates() []JobState {
	return []JobState{
		JobStateQueued,
		JobStateRunning,
		JobStateSucceeded,
		JobStateFailed,
		JobStateCancelled,
	}
}

// IsValid reports whether s is a recognized JobState.
func (s JobState) IsValid() bool {
	return slices.Contains(ValidJobStates(), s)
}

// IsActive reports whether s is a non-terminal state (QUEUED or RUNNING).
func (s JobState) IsActive() bool {
	return s == JobStateQueued || s == JobStateRunning
}

// IsTerminal reports whether s is a completed state (SUCCEEDED, FAILED, or CANCELLED).
func (s JobState) IsTerminal() bool {
	return s == JobStateSucceeded || s == JobStateFailed || s == JobStateCancelled
}

// CanTransitionTo reports whether a job in state s may transition directly to next.
func (s JobState) CanTransitionTo(next JobState) bool {
	switch s {
	case JobStateQueued:
		return next == JobStateRunning || next == JobStateCancelled
	case JobStateRunning:
		return next == JobStateQueued || next == JobStateSucceeded || next == JobStateFailed || next == JobStateCancelled
	case JobStateSucceeded, JobStateFailed, JobStateCancelled:
		return false
	default:
		return false
	}
}

// JobRetryBackoff returns the default exponential retry backoff delay for a failed attempt.
func JobRetryBackoff(attempt int) time.Duration {
	const (
		baseDelay = 5 * time.Second
		maxDelay  = 5 * time.Minute
	)
	if attempt <= 1 {
		return baseDelay
	}
	shift := min(attempt-1, 10)
	delay := baseDelay << shift
	if delay > maxDelay || delay <= 0 {
		return maxDelay
	}
	return delay
}

// Job represents a durable background job stored in PostgreSQL (ADR-0017).
type Job struct {
	ID              string          `json:"id"`
	Kind            JobKind         `json:"kind"`
	State           JobState        `json:"state"`
	Payload         json.RawMessage `json:"payload"`
	Checkpoint      json.RawMessage `json:"checkpoint"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	CancelRequested bool            `json:"cancel_requested"`
	LastError       *string         `json:"last_error,omitempty"`
	CreatedBy       *string         `json:"created_by,omitempty"`
	DedupeKey       *string         `json:"dedupe_key,omitempty"`
	RunAfter        time.Time       `json:"run_after"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	UpdatedAt       time.Time       `json:"updated_at"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
}

// Validate checks Job invariants aligned with 0001_init.sql.
func (j *Job) Validate() error {
	if strings.TrimSpace(j.ID) == "" {
		return ErrEmptyJobID
	}
	if strings.TrimSpace(string(j.Kind)) == "" {
		return ErrEmptyJobKind
	}
	if !j.State.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidJobState, j.State)
	}
	if j.Attempts < 0 || j.MaxAttempts < 1 || j.Attempts > math.MaxInt32 || j.MaxAttempts > math.MaxInt32 {
		return ErrInvalidJobAttempts
	}
	if j.DedupeKey != nil && strings.TrimSpace(*j.DedupeKey) == "" {
		return ErrEmptyDedupeKey
	}
	return nil
}

// IsActive reports whether the job is queued or currently running.
func (j *Job) IsActive() bool {
	return j.State.IsActive()
}

// IsTerminal reports whether the job has reached a terminal state.
func (j *Job) IsTerminal() bool {
	return j.State.IsTerminal()
}

// CanRetry reports whether the job has remaining retry attempts.
func (j *Job) CanRetry() bool {
	return j.Attempts < j.MaxAttempts
}

// VisibleTo reports whether principal is permitted to view details of this job (ADR-0017, ADR-0018 S3).
// Automatically scheduled jobs (CreatedBy == nil) are visible only to principals holding jobs:read_all.
func (j *Job) VisibleTo(principal *Principal) bool {
	if principal == nil {
		return false
	}
	if principal.HasPermission(PermissionJobsReadAll) {
		return true
	}
	return j.CreatedBy != nil && *j.CreatedBy != "" && *j.CreatedBy == principal.User.ID
}

// EventKind identifies a transactional outbox domain event kind (ADR-0018 S3).
type EventKind string

const (
	EventAssetAdded            EventKind = "asset.added"
	EventAssetModified         EventKind = "asset.modified"
	EventAssetMoved            EventKind = "asset.moved"
	EventAssetRemoved          EventKind = "asset.removed"
	EventVolumeRemoved         EventKind = "volume.removed"
	EventSeriesRemoved         EventKind = "series.removed"
	EventVolumeMetadataChanged EventKind = "volume.metadata_changed"
	EventSeriesMetadataChanged EventKind = "series.metadata_changed"
	EventProgressUpdated       EventKind = "progress.updated"
	EventVolumeCompleted       EventKind = "volume.completed"
	EventJobStarted            EventKind = "job.started"
	EventJobFinished           EventKind = "job.finished"
	EventJobFailed             EventKind = "job.failed"
	EventJobCancelled          EventKind = "job.cancelled"
	EventSubscriberStalled     EventKind = "subscriber.stalled"
)

// DomainEvent represents a transactional outbox event (ADR-0018 S3, 0001_init.sql).
// ID is the internal BIGSERIAL row identity and is NEVER a replay cursor;
// PublishedSequence (assigned post-commit by the serialized dispatcher) is the canonical cursor.
type DomainEvent struct { //nolint:revive // DomainEvent matches the domain_events schema, postgres.DomainEvent row, and M1-T3.2 contract.
	ID                int64           `json:"id"`
	PublishedSequence *int64          `json:"published_sequence,omitempty"`
	PublishedAt       *time.Time      `json:"published_at,omitempty"`
	Kind              EventKind       `json:"kind"`
	Version           int             `json:"version"`
	AggregateType     string          `json:"aggregate_type"`
	AggregateID       string          `json:"aggregate_id"`
	Payload           json.RawMessage `json:"payload"`
	CreatedAt         time.Time       `json:"created_at"`
}

// Validate checks DomainEvent invariants aligned with 0001_init.sql.
func (e *DomainEvent) Validate() error {
	if strings.TrimSpace(string(e.Kind)) == "" {
		return ErrEmptyEventKind
	}
	if e.Version < 1 || e.Version > math.MaxInt32 {
		return ErrInvalidEventVersion
	}
	if strings.TrimSpace(e.AggregateType) == "" {
		return ErrEmptyAggregateType
	}
	if strings.TrimSpace(e.AggregateID) == "" {
		return ErrEmptyAggregateID
	}
	if len(e.Payload) > 0 && !isValidJSONObject(e.Payload) {
		return ErrInvalidEventPayload
	}
	if (e.PublishedSequence == nil) != (e.PublishedAt == nil) {
		return ErrInvalidPublicationPair
	}
	if e.PublishedSequence != nil && *e.PublishedSequence <= 0 {
		return ErrInvalidPublishedSeq
	}
	return nil
}

// ValidateUnpublished checks that e is valid for outbox append with no pre-assigned
// internal ID, published_sequence, or published_at.
func (e *DomainEvent) ValidateUnpublished() error {
	if e.ID != 0 {
		return ErrUnexpectedEventID
	}
	if e.PublishedSequence != nil || e.PublishedAt != nil {
		return ErrUnexpectedPublicationMetadata
	}
	return e.Validate()
}

// IsPublished reports whether the serialized dispatcher has assigned a publication sequence and timestamp.
func (e *DomainEvent) IsPublished() bool {
	return e.PublishedSequence != nil && e.PublishedAt != nil
}

// Cursor returns the published sequence cursor if the event has been published.
// Raw internal IDs are never returned as replay cursors (ADR-0018 S3).
func (e *DomainEvent) Cursor() (int64, bool) {
	if !e.IsPublished() {
		return 0, false
	}
	return *e.PublishedSequence, true
}

// SubscriberState represents the delivery state of a durable event subscriber (ADR-0018 S3, 0001_init.sql).
type SubscriberState string

const (
	SubscriberStateActive  SubscriberState = "ACTIVE"
	SubscriberStatePaused  SubscriberState = "PAUSED"
	SubscriberStateStalled SubscriberState = "STALLED"
)

// ValidSubscriberStates returns all defined SubscriberState values.
func ValidSubscriberStates() []SubscriberState {
	return []SubscriberState{
		SubscriberStateActive,
		SubscriberStatePaused,
		SubscriberStateStalled,
	}
}

// IsValid reports whether s is a recognized SubscriberState.
func (s SubscriberState) IsValid() bool {
	return slices.Contains(ValidSubscriberStates(), s)
}

// EventSubscriber tracks a durable subscriber's acknowledged publication-sequence cursor (ADR-0018 S3).
type EventSubscriber struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	Position  int64           `json:"position"`
	State     SubscriberState `json:"state"`
	StalledAt *time.Time      `json:"stalled_at,omitempty"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Validate checks EventSubscriber invariants aligned with 0001_init.sql.
func (s *EventSubscriber) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return ErrEmptySubscriberID
	}
	if strings.TrimSpace(s.Kind) == "" {
		return ErrEmptySubscriberKind
	}
	if !s.State.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidSubscriberState, s.State)
	}
	if s.Position < 0 {
		return ErrInvalidSubscriberPosition
	}
	if (s.State == SubscriberStateStalled) != (s.StalledAt != nil) {
		return ErrInvalidStalledPair
	}
	return nil
}
