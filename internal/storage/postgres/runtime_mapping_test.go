package postgres

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestRuntimePostgresMappingsRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	errMsg := "boom"
	creator := "user-1"
	dedupe := "SCAN:lib-1"
	seq := int64(17)

	job := domain.Job{
		ID:              "job-1",
		Kind:            domain.JobKindScan,
		State:           domain.JobStateRunning,
		Payload:         json.RawMessage(`{"library_id":"lib-1"}`),
		Checkpoint:      json.RawMessage(`{"progress_pct":50}`),
		Attempts:        1,
		MaxAttempts:     3,
		CancelRequested: false,
		LastError:       &errMsg,
		CreatedBy:       &creator,
		DedupeKey:       &dedupe,
		RunAfter:        now,
		CreatedAt:       now,
		StartedAt:       &now,
		UpdatedAt:       now,
	}
	roundTripJob := JobToDomain(JobFromDomain(job))
	if !reflect.DeepEqual(roundTripJob, job) {
		t.Fatalf("Job round-trip mismatch:\n got: %+v\nwant: %+v", roundTripJob, job)
	}

	unpubEvent := domain.DomainEvent{
		ID:            10,
		Kind:          domain.EventProgressUpdated,
		Version:       1,
		AggregateType: "volume",
		AggregateID:   "vol-1",
		Payload:       json.RawMessage(`{"page":0}`),
		CreatedAt:     now,
	}
	roundTripUnpub := DomainEventToDomain(DomainEventFromDomain(unpubEvent))
	if !reflect.DeepEqual(roundTripUnpub, unpubEvent) {
		t.Fatalf("unpublished DomainEvent round-trip mismatch:\n got: %+v\nwant: %+v", roundTripUnpub, unpubEvent)
	}

	pubEvent := unpubEvent
	pubEvent.PublishedSequence = &seq
	pubEvent.PublishedAt = &now
	roundTripPub := DomainEventToDomain(DomainEventFromDomain(pubEvent))
	if !reflect.DeepEqual(roundTripPub, pubEvent) {
		t.Fatalf("published DomainEvent round-trip mismatch:\n got: %+v\nwant: %+v", roundTripPub, pubEvent)
	}

	sub := domain.EventSubscriber{
		ID:        "notify:shoutrrr",
		Kind:      "shoutrrr",
		Position:  17,
		State:     domain.SubscriberStateStalled,
		StalledAt: &now,
		UpdatedAt: now,
	}
	roundTripSub := EventSubscriberToDomain(EventSubscriberFromDomain(sub))
	if !reflect.DeepEqual(roundTripSub, sub) {
		t.Fatalf("EventSubscriber round-trip mismatch:\n got: %+v\nwant: %+v", roundTripSub, sub)
	}
}
