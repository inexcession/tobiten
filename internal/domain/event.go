package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// EventVersion1 is the initial payload schema version for 1.0 domain events (ADR-0018 S3).
const EventVersion1 = 1

// ProgressTickEventName is the ephemeral SSE event name for worker progress ticks.
// Progress ticks are never persisted in domain_events, carry no SSE ID, and are never replayed (ADR-0018 S3).
const ProgressTickEventName = "job.progress"

// Canonical aggregate_type values for 1.0 domain_events rows.
const (
	AggregateTypeLibrary    = "library"
	AggregateTypeAsset      = "asset"
	AggregateTypeVolume     = "volume"
	AggregateTypeSeries     = "series"
	AggregateTypeJob        = "job"
	AggregateTypeSubscriber = "subscriber"
)

var eventKindAggregateTypes = map[EventKind]string{
	EventLibraryOffline:        AggregateTypeLibrary,
	EventLibraryOnline:         AggregateTypeLibrary,
	EventAssetAdded:            AggregateTypeAsset,
	EventAssetModified:         AggregateTypeAsset,
	EventAssetMoved:            AggregateTypeAsset,
	EventAssetMissing:          AggregateTypeAsset,
	EventAssetRestored:         AggregateTypeAsset,
	EventAssetRemoved:          AggregateTypeAsset,
	EventAssetInvalidUTF8Path:  AggregateTypeAsset,
	EventVolumeRemoved:         AggregateTypeVolume,
	EventSeriesRemoved:         AggregateTypeSeries,
	EventVolumeMetadataChanged: AggregateTypeVolume,
	EventSeriesMetadataChanged: AggregateTypeSeries,
	EventProgressUpdated:       AggregateTypeVolume,
	EventVolumeCompleted:       AggregateTypeVolume,
	EventJobStarted:            AggregateTypeJob,
	EventJobFinished:           AggregateTypeJob,
	EventJobFailed:             AggregateTypeJob,
	EventJobCancelled:          AggregateTypeJob,
	EventSubscriberStalled:     AggregateTypeSubscriber,
}

// ValidEventKinds returns all defined 1.0 domain event kinds (ADR-0018 S3).
func ValidEventKinds() []EventKind {
	return []EventKind{
		EventLibraryOffline,
		EventLibraryOnline,
		EventAssetAdded,
		EventAssetModified,
		EventAssetMoved,
		EventAssetMissing,
		EventAssetRestored,
		EventAssetRemoved,
		EventAssetInvalidUTF8Path,
		EventVolumeRemoved,
		EventSeriesRemoved,
		EventVolumeMetadataChanged,
		EventSeriesMetadataChanged,
		EventProgressUpdated,
		EventVolumeCompleted,
		EventJobStarted,
		EventJobFinished,
		EventJobFailed,
		EventJobCancelled,
		EventSubscriberStalled,
	}
}

// IsValid reports whether k is one of the defined 1.0 domain event kinds.
func (k EventKind) IsValid() bool {
	return slices.Contains(ValidEventKinds(), k)
}

// DefaultAggregateType returns the canonical aggregate_type for a defined 1.0 event kind.
func (k EventKind) DefaultAggregateType() (string, bool) {
	agg, ok := eventKindAggregateTypes[k]
	return agg, ok
}

// LibraryEventPayloadV1 is the version 1 payload for library.offline and
// library.online domain events (ADR-0018 S3, ADR-0019 §5).
type LibraryEventPayloadV1 struct {
	LibraryID     string   `json:"library_id"`
	FailedRootIDs []string `json:"failed_root_ids,omitempty"`
}

// JobEventPayloadV1 is the version 1 payload for job.started, job.finished,
// job.failed, and job.cancelled domain events (ADR-0017 §2, ADR-0018 S3).
type JobEventPayloadV1 struct {
	JobID     string   `json:"job_id"`
	Kind      JobKind  `json:"kind"`
	State     JobState `json:"state"`
	Attempts  int      `json:"attempts"`
	CreatedBy *string  `json:"created_by,omitempty"`
	LastError *string  `json:"last_error,omitempty"`
}

// AssetEventPayloadV1 is the version 1 payload for asset.added, asset.modified,
// asset.moved, asset.missing, asset.restored, asset.removed, and asset.invalid_utf8_path
// domain events (ADR-0003 §7, ADR-0018 S3).
type AssetEventPayloadV1 struct {
	AssetID             string      `json:"asset_id"`
	VolumeID            string      `json:"volume_id"`
	LibraryID           string      `json:"library_id"`
	LibraryRootID       string      `json:"library_root_id"`
	Format              MediaFormat `json:"format,omitempty"`
	PathDisplay         string      `json:"path_display,omitempty"`
	PreviousPathDisplay string      `json:"previous_path_display,omitempty"`
}

// VolumeEventPayloadV1 is the version 1 payload for volume.removed,
// volume.metadata_changed, and volume.completed domain events (ADR-0018 S3).
type VolumeEventPayloadV1 struct {
	VolumeID  string  `json:"volume_id"`
	LibraryID string  `json:"library_id"`
	SeriesID  *string `json:"series_id,omitempty"`
	UserID    *string `json:"user_id,omitempty"`
}

// SeriesEventPayloadV1 is the version 1 payload for series.removed and
// series.metadata_changed domain events (ADR-0018 S3).
type SeriesEventPayloadV1 struct {
	SeriesID  string `json:"series_id"`
	LibraryID string `json:"library_id"`
}

// ProgressUpdatedPayloadV1 is the version 1 payload for progress.updated domain events (ADR-0018 S3, S4).
type ProgressUpdatedPayloadV1 struct {
	UserID           string  `json:"user_id"`
	VolumeID         string  `json:"volume_id"`
	Locator          Locator `json:"locator"`
	TotalProgression float64 `json:"total_progression"`
	Completed        bool    `json:"completed"`
	Source           string  `json:"source"`
}

// SubscriberStalledPayloadV1 is the version 1 payload for subscriber.stalled domain events (ADR-0013 §3, ADR-0018 S3).
type SubscriberStalledPayloadV1 struct {
	SubscriberID   string    `json:"subscriber_id"`
	SubscriberKind string    `json:"subscriber_kind"`
	Position       int64     `json:"position"`
	StalledAt      time.Time `json:"stalled_at"`
	Reason         string    `json:"reason,omitempty"`
}

func isValidJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return false
	}
	return json.Valid(trimmed)
}

func marshalEventPayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return json.RawMessage("{}"), nil
	}
	var raw []byte
	switch v := payload.(type) {
	case json.RawMessage:
		if len(v) == 0 {
			return json.RawMessage("{}"), nil
		}
		raw = slices.Clone(v)
	case []byte:
		if len(v) == 0 {
			return json.RawMessage("{}"), nil
		}
		raw = slices.Clone(v)
	default:
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidEventPayload, err)
		}
		raw = encoded
	}
	if !isValidJSONObject(raw) {
		return nil, ErrInvalidEventPayload
	}
	return json.RawMessage(raw), nil
}

// NewDomainEvent builds and validates an unpublished outbox DomainEvent with a versioned JSON payload.
// PublishedSequence and PublishedAt are always left unset until the serialized dispatcher assigns them.
func NewDomainEvent(kind EventKind, version int, aggregateType, aggregateID string, payload any, createdAt time.Time) (DomainEvent, error) {
	raw, err := marshalEventPayload(payload)
	if err != nil {
		return DomainEvent{}, err
	}
	var ts time.Time
	if !createdAt.IsZero() {
		ts = createdAt.UTC()
	}
	ev := DomainEvent{
		Kind:          kind,
		Version:       version,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Payload:       raw,
		CreatedAt:     ts,
	}
	if err := ev.ValidateUnpublished(); err != nil {
		return DomainEvent{}, err
	}
	return ev, nil
}

var jobLifecycleStateByEvent = map[EventKind]JobState{
	EventJobStarted:   JobStateRunning,
	EventJobFinished:  JobStateSucceeded,
	EventJobFailed:    JobStateFailed,
	EventJobCancelled: JobStateCancelled,
}

// JobLifecycleEventKind returns the outbox EventKind corresponding to a committed job
// state transition, or false for states that do not emit a lifecycle event (such as QUEUED).
func JobLifecycleEventKind(state JobState) (EventKind, bool) {
	switch state {
	case JobStateRunning:
		return EventJobStarted, true
	case JobStateSucceeded:
		return EventJobFinished, true
	case JobStateFailed:
		return EventJobFailed, true
	case JobStateCancelled:
		return EventJobCancelled, true
	default:
		return "", false
	}
}

// JobProgressTick is an ephemeral, rate-limited worker progress update (ADR-0017 §2, ADR-0018 S3).
// It carries no SSE ID or publication sequence, is never stored in domain_events, and obeys
// the same creator/jobs:read_all visibility rules as Job details.
type JobProgressTick struct {
	JobID        string  `json:"job_id"`
	Kind         JobKind `json:"kind"`
	CreatedBy    *string `json:"created_by,omitempty"`
	ScannedCount int64   `json:"scanned_count"`
	CurrentPath  string  `json:"current_path,omitempty"`
	DurationMS   int64   `json:"duration_ms"`
}

// Validate checks that t identifies a job and carries non-negative progress counters.
func (t JobProgressTick) Validate() error {
	if strings.TrimSpace(t.JobID) == "" {
		return fmt.Errorf("%w: %w", ErrInvalidProgressTick, ErrEmptyJobID)
	}
	if strings.TrimSpace(string(t.Kind)) == "" {
		return fmt.Errorf("%w: %w", ErrInvalidProgressTick, ErrEmptyJobKind)
	}
	if t.ScannedCount < 0 {
		return fmt.Errorf("%w: scanned_count must be non-negative", ErrInvalidProgressTick)
	}
	if t.DurationMS < 0 {
		return fmt.Errorf("%w: duration_ms must be non-negative", ErrInvalidProgressTick)
	}
	return nil
}

// VisibleTo reports whether principal is permitted to receive progress ticks for this job
// (creator or jobs:read_all; automatic jobs with nil CreatedBy are jobs:read_all only).
func (t JobProgressTick) VisibleTo(principal *Principal) bool {
	j := Job{
		ID:        t.JobID,
		Kind:      t.Kind,
		CreatedBy: t.CreatedBy,
	}
	return j.VisibleTo(principal)
}

// Clone returns a copy of t with an independent CreatedBy string pointer.
func (t JobProgressTick) Clone() JobProgressTick {
	out := t
	if t.CreatedBy != nil {
		createdBy := *t.CreatedBy
		out.CreatedBy = &createdBy
	}
	return out
}

// NewJobLifecycleEvent constructs a version 1 job lifecycle outbox event for job.
// Internal EVENT_DELIVERY jobs are rejected to prevent recursive event emission (ADR-0018 S3).
func NewJobLifecycleEvent(kind EventKind, job Job, createdAt time.Time) (DomainEvent, error) {
	if job.Kind.IsInternal() {
		return DomainEvent{}, ErrInternalJobEventSuppressed
	}
	expectedState, ok := jobLifecycleStateByEvent[kind]
	if !ok {
		return DomainEvent{}, fmt.Errorf("%w: unsupported kind %q", ErrInvalidJobLifecycleEvent, kind)
	}
	if job.State != expectedState {
		return DomainEvent{}, fmt.Errorf("%w: kind %s requires state %s, got %s",
			ErrInvalidJobLifecycleEvent, kind, expectedState, job.State)
	}
	if err := job.Validate(); err != nil {
		return DomainEvent{}, err
	}
	payload := JobEventPayloadV1{
		JobID:     job.ID,
		Kind:      job.Kind,
		State:     job.State,
		Attempts:  job.Attempts,
		CreatedBy: job.CreatedBy,
		LastError: job.LastError,
	}
	return NewDomainEvent(kind, EventVersion1, AggregateTypeJob, job.ID, payload, createdAt)
}

// DecodeEventPayload unmarshals an event's versioned JSON payload into T, ignoring unknown
// fields for forward-compatible schema evolution (ADR-0018 S3).
func DecodeEventPayload[T any](e DomainEvent) (T, error) {
	var out T
	if e.Version < 1 {
		return out, ErrInvalidEventVersion
	}
	if !isValidJSONObject(e.Payload) {
		return out, ErrInvalidEventPayload
	}
	if err := json.Unmarshal(e.Payload, &out); err != nil {
		return out, fmt.Errorf("%w: %v", ErrInvalidEventPayload, err)
	}
	return out, nil
}

// EventDeliveryOutcome reports the authorized disposition of delivering a published
// domain event to a durable subscriber (ADR-0018 S3).
type EventDeliveryOutcome int

const (
	// DeliveryAcknowledged indicates the subscriber accepted/delivered the event
	// and its durable cursor may advance to PublishedSequence.
	DeliveryAcknowledged EventDeliveryOutcome = iota + 1

	// DeliverySkipped indicates an authorized filter or permission/label policy
	// excluded the event for this subscriber and its durable cursor may advance.
	DeliverySkipped
)

// IsValid reports whether o is a recognized EventDeliveryOutcome.
func (o EventDeliveryOutcome) IsValid() bool {
	return o == DeliveryAcknowledged || o == DeliverySkipped
}

// EventDeliveryPayloadV1 is the versioned job payload for internal EVENT_DELIVERY jobs
// (ADR-0017, ADR-0018 S3).
type EventDeliveryPayloadV1 struct {
	SubscriberID      string `json:"subscriber_id"`
	PublishedSequence int64  `json:"published_sequence"`
	EventID           int64  `json:"event_id,omitempty"`
}

// Validate checks that p identifies a subscriber and a positive publication sequence.
func (p EventDeliveryPayloadV1) Validate() error {
	if strings.TrimSpace(p.SubscriberID) == "" {
		return fmt.Errorf("%w: %w", ErrInvalidDeliveryPayload, ErrEmptySubscriberID)
	}
	if p.PublishedSequence <= 0 {
		return fmt.Errorf("%w: %w", ErrInvalidDeliveryPayload, ErrInvalidPublishedSeq)
	}
	return nil
}

// EventDeliveryDedupeKey returns the active-job deduplication key serializing
// EVENT_DELIVERY execution per durable subscriber (ADR-0017 §1, ADR-0018 S3).
func EventDeliveryDedupeKey(subscriberID string) (string, error) {
	if strings.TrimSpace(subscriberID) == "" {
		return "", ErrEmptySubscriberID
	}
	return "EVENT_DELIVERY:" + subscriberID, nil
}

// DecodeEventDeliveryPayload unmarshals and validates a durable EVENT_DELIVERY job payload.
func DecodeEventDeliveryPayload(raw json.RawMessage) (EventDeliveryPayloadV1, error) {
	var p EventDeliveryPayloadV1
	if !isValidJSONObject(raw) {
		return p, ErrInvalidDeliveryPayload
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("%w: %v", ErrInvalidDeliveryPayload, err)
	}
	if err := p.Validate(); err != nil {
		return p, err
	}
	return p, nil
}
