package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// EventVersion1 is the initial payload schema version for 1.0 domain events (ADR-0018 S3).
const EventVersion1 = 1

// Canonical aggregate_type values for 1.0 domain_events rows.
const (
	AggregateTypeAsset      = "asset"
	AggregateTypeVolume     = "volume"
	AggregateTypeSeries     = "series"
	AggregateTypeJob        = "job"
	AggregateTypeSubscriber = "subscriber"
)

var eventKindAggregateTypes = map[EventKind]string{
	EventAssetAdded:            AggregateTypeAsset,
	EventAssetModified:         AggregateTypeAsset,
	EventAssetMoved:            AggregateTypeAsset,
	EventAssetRemoved:          AggregateTypeAsset,
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
		EventAssetAdded,
		EventAssetModified,
		EventAssetMoved,
		EventAssetRemoved,
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
// asset.moved, and asset.removed domain events (ADR-0003 §7, ADR-0018 S3).
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
