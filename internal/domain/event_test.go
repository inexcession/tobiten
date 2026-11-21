package domain

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestValidEventKindsAndAggregateTypes(t *testing.T) {
	kinds := ValidEventKinds()
	if len(kinds) != 16 {
		t.Fatalf("len(ValidEventKinds()) = %d, want 16", len(kinds))
	}
	seen := make(map[EventKind]bool, len(kinds))
	for _, k := range kinds {
		if !k.IsValid() {
			t.Errorf("expected %q.IsValid() == true", k)
		}
		if seen[k] {
			t.Errorf("duplicate event kind %q", k)
		}
		seen[k] = true
		agg, ok := k.DefaultAggregateType()
		if !ok || agg == "" {
			t.Errorf("expected %q.DefaultAggregateType() to return non-empty aggregate type", k)
		}
	}
	if EventKind("custom.unknown").IsValid() {
		t.Error("expected unknown EventKind IsValid() == false")
	}
	if _, ok := EventKind("custom.unknown").DefaultAggregateType(); ok {
		t.Error("expected unknown EventKind DefaultAggregateType() == false")
	}
}

func TestNewDomainEventAndDecodeVersionedPayloads(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	seriesID := "ser-1"
	userID := "usr-1"
	creator := "usr-1"
	errMsg := "disk full"

	t.Run("job lifecycle payload round-trip", func(t *testing.T) {
		job := Job{
			ID:          "job-1",
			Kind:        JobKindScan,
			State:       JobStateFailed,
			Attempts:    3,
			MaxAttempts: 3,
			CreatedBy:   &creator,
			LastError:   &errMsg,
		}
		ev, err := NewJobLifecycleEvent(EventJobFailed, job, now)
		if err != nil {
			t.Fatalf("NewJobLifecycleEvent: %v", err)
		}
		if ev.Version != EventVersion1 || ev.AggregateType != AggregateTypeJob || ev.AggregateID != "job-1" {
			t.Fatalf("unexpected envelope: %+v", ev)
		}
		if ev.IsPublished() {
			t.Fatal("new outbox event must not be published")
		}
		got, err := DecodeEventPayload[JobEventPayloadV1](ev)
		if err != nil {
			t.Fatalf("DecodeEventPayload: %v", err)
		}
		want := JobEventPayloadV1{
			JobID:     "job-1",
			Kind:      JobKindScan,
			State:     JobStateFailed,
			Attempts:  3,
			CreatedBy: &creator,
			LastError: &errMsg,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("decoded payload = %+v, want %+v", got, want)
		}
	})

	t.Run("internal EVENT_DELIVERY job suppresses lifecycle events", func(t *testing.T) {
		job := Job{
			ID:          "job-deliv-1",
			Kind:        JobKindEventDelivery,
			State:       JobStateSucceeded,
			Attempts:    1,
			MaxAttempts: 3,
		}
		if _, err := NewJobLifecycleEvent(EventJobFinished, job, now); !errors.Is(err, ErrInternalJobEventSuppressed) {
			t.Fatalf("expected ErrInternalJobEventSuppressed, got %v", err)
		}
	})

	t.Run("job lifecycle kind/state mismatch rejected", func(t *testing.T) {
		job := Job{
			ID:          "job-2",
			Kind:        JobKindScan,
			State:       JobStateRunning,
			Attempts:    1,
			MaxAttempts: 3,
		}
		if _, err := NewJobLifecycleEvent(EventJobFinished, job, now); !errors.Is(err, ErrInvalidJobLifecycleEvent) {
			t.Fatalf("expected ErrInvalidJobLifecycleEvent, got %v", err)
		}
	})

	t.Run("typed v1 payloads round-trip and ignore unknown fields", func(t *testing.T) {
		assetEv, err := NewDomainEvent(EventAssetMoved, EventVersion1, AggregateTypeAsset, "asset-1", AssetEventPayloadV1{
			AssetID:             "asset-1",
			VolumeID:            "vol-1",
			LibraryID:           "lib-1",
			LibraryRootID:       "root-1",
			Format:              MediaFormatCBZ,
			PathDisplay:         "Series/v02.cbz",
			PreviousPathDisplay: "Series/v01.cbz",
		}, now)
		if err != nil {
			t.Fatalf("NewDomainEvent asset: %v", err)
		}
		gotAsset, err := DecodeEventPayload[AssetEventPayloadV1](assetEv)
		if err != nil || gotAsset.PreviousPathDisplay != "Series/v01.cbz" {
			t.Fatalf("decoded asset = %+v, err = %v", gotAsset, err)
		}

		invalidUTF8Ev, err := NewDomainEvent(EventAssetInvalidUTF8Path, EventVersion1, AggregateTypeAsset, "asset-2", AssetEventPayloadV1{
			AssetID:       "asset-2",
			VolumeID:      "vol-2",
			LibraryID:     "lib-1",
			LibraryRootID: "root-1",
			Format:        MediaFormatCBZ,
			PathDisplay:   `Series\xff/v01.cbz`,
		}, now)
		if err != nil {
			t.Fatalf("NewDomainEvent invalid_utf8_path: %v", err)
		}
		gotInvalidUTF8, err := DecodeEventPayload[AssetEventPayloadV1](invalidUTF8Ev)
		if err != nil || gotInvalidUTF8.PathDisplay != `Series\xff/v01.cbz` {
			t.Fatalf("decoded invalid_utf8_path = %+v, err = %v", gotInvalidUTF8, err)
		}

		volEv, err := NewDomainEvent(EventVolumeCompleted, EventVersion1, AggregateTypeVolume, "vol-1", VolumeEventPayloadV1{
			VolumeID:  "vol-1",
			LibraryID: "lib-1",
			SeriesID:  &seriesID,
			UserID:    &userID,
		}, now)
		if err != nil {
			t.Fatalf("NewDomainEvent volume: %v", err)
		}
		gotVol, err := DecodeEventPayload[VolumeEventPayloadV1](volEv)
		if err != nil || gotVol.UserID == nil || *gotVol.UserID != "usr-1" {
			t.Fatalf("decoded volume = %+v, err = %v", gotVol, err)
		}

		serEv, err := NewDomainEvent(EventSeriesMetadataChanged, EventVersion1, AggregateTypeSeries, "ser-1", SeriesEventPayloadV1{
			SeriesID:  "ser-1",
			LibraryID: "lib-1",
		}, now)
		if err != nil {
			t.Fatalf("NewDomainEvent series: %v", err)
		}
		gotSer, err := DecodeEventPayload[SeriesEventPayloadV1](serEv)
		if err != nil || gotSer.SeriesID != "ser-1" {
			t.Fatalf("decoded series = %+v, err = %v", gotSer, err)
		}

		progEv, err := NewDomainEvent(EventProgressUpdated, EventVersion1, AggregateTypeVolume, "vol-1", ProgressUpdatedPayloadV1{
			UserID:           "usr-1",
			VolumeID:         "vol-1",
			Locator:          NewPageLocator(0, 24, "asset-1", &LocatorDigest{Kind: DigestKindXXH364Partial, Value: "deadbeef"}),
			TotalProgression: 0.0,
			Completed:        false,
			Source:           "komga",
		}, now)
		if err != nil {
			t.Fatalf("NewDomainEvent progress: %v", err)
		}
		gotProg, err := DecodeEventPayload[ProgressUpdatedPayloadV1](progEv)
		if err != nil || gotProg.Locator.Page == nil || *gotProg.Locator.Page != 0 {
			t.Fatalf("decoded progress = %+v, err = %v", gotProg, err)
		}

		stallEv, err := NewDomainEvent(EventSubscriberStalled, EventVersion1, AggregateTypeSubscriber, "notify:shoutrrr", SubscriberStalledPayloadV1{
			SubscriberID:   "notify:shoutrrr",
			SubscriberKind: "shoutrrr",
			Position:       42,
			StalledAt:      now,
			Reason:         "backlog older than 7d retention",
		}, now)
		if err != nil {
			t.Fatalf("NewDomainEvent subscriber: %v", err)
		}
		gotStall, err := DecodeEventPayload[SubscriberStalledPayloadV1](stallEv)
		if err != nil || gotStall.Position != 42 {
			t.Fatalf("decoded stall = %+v, err = %v", gotStall, err)
		}

		// Additive v2 payload with an unknown field must still decode cleanly into v1 struct.
		v2Event, err := NewDomainEvent(EventSeriesMetadataChanged, 2, AggregateTypeSeries, "ser-1",
			json.RawMessage(`{"series_id":"ser-1","library_id":"lib-1","future_field":"ignored"}`), now)
		if err != nil {
			t.Fatalf("NewDomainEvent v2: %v", err)
		}
		gotV2AsV1, err := DecodeEventPayload[SeriesEventPayloadV1](v2Event)
		if err != nil || gotV2AsV1.SeriesID != "ser-1" || gotV2AsV1.LibraryID != "lib-1" {
			t.Fatalf("forward-compatible decode failed: %+v, err = %v", gotV2AsV1, err)
		}
	})

	t.Run("validation guards", func(t *testing.T) {
		if _, err := NewDomainEvent(EventJobStarted, 0, AggregateTypeJob, "job-1", nil, now); !errors.Is(err, ErrInvalidEventVersion) {
			t.Fatalf("expected ErrInvalidEventVersion for version 0, got %v", err)
		}
		if _, err := NewDomainEvent(EventJobStarted, math.MaxInt32+1, AggregateTypeJob, "job-1", nil, now); !errors.Is(err, ErrInvalidEventVersion) {
			t.Fatalf("expected ErrInvalidEventVersion for overflow version, got %v", err)
		}
		if _, err := NewDomainEvent(EventJobStarted, EventVersion1, AggregateTypeJob, "job-1", json.RawMessage(`not-json`), now); !errors.Is(err, ErrInvalidEventPayload) {
			t.Fatalf("expected ErrInvalidEventPayload for invalid JSON, got %v", err)
		}
		if _, err := NewDomainEvent(EventJobStarted, EventVersion1, AggregateTypeJob, "job-1", json.RawMessage(`[]`), now); !errors.Is(err, ErrInvalidEventPayload) {
			t.Fatalf("expected ErrInvalidEventPayload for array payload, got %v", err)
		}
		var nilPtr *SeriesEventPayloadV1
		if _, err := NewDomainEvent(EventSeriesRemoved, EventVersion1, AggregateTypeSeries, "ser-1", nilPtr, now); !errors.Is(err, ErrInvalidEventPayload) {
			t.Fatalf("expected ErrInvalidEventPayload for nil typed pointer marshaling to null, got %v", err)
		}
		nilPayloadEv, err := NewDomainEvent(EventSeriesRemoved, EventVersion1, AggregateTypeSeries, "ser-1", nil, now)
		if err != nil {
			t.Fatalf("expected nil payload to default to {}: %v", err)
		}
		if string(nilPayloadEv.Payload) != "{}" {
			t.Fatalf("payload = %s, want {}", nilPayloadEv.Payload)
		}
		seq := int64(1)
		preSeq := nilPayloadEv
		preSeq.PublishedSequence = &seq
		preSeq.PublishedAt = &now
		if err := preSeq.ValidateUnpublished(); !errors.Is(err, ErrUnexpectedPublicationMetadata) {
			t.Fatalf("expected ErrUnexpectedPublicationMetadata, got %v", err)
		}
		preID := nilPayloadEv
		preID.ID = 7
		if err := preID.ValidateUnpublished(); !errors.Is(err, ErrUnexpectedEventID) {
			t.Fatalf("expected ErrUnexpectedEventID, got %v", err)
		}
	})
}

func TestEventDeliveryHelpers(t *testing.T) {
	if !DeliveryAcknowledged.IsValid() || !DeliverySkipped.IsValid() {
		t.Fatal("expected DeliveryAcknowledged and DeliverySkipped to be valid")
	}
	if EventDeliveryOutcome(0).IsValid() || EventDeliveryOutcome(99).IsValid() {
		t.Fatal("expected unknown EventDeliveryOutcome values to be invalid")
	}

	key, err := EventDeliveryDedupeKey("notify:shoutrrr")
	if err != nil || key != "EVENT_DELIVERY:notify:shoutrrr" {
		t.Fatalf("EventDeliveryDedupeKey = (%q, %v), want (EVENT_DELIVERY:notify:shoutrrr, nil)", key, err)
	}
	if _, err := EventDeliveryDedupeKey("   "); !errors.Is(err, ErrEmptySubscriberID) {
		t.Fatalf("empty EventDeliveryDedupeKey err = %v, want ErrEmptySubscriberID", err)
	}

	raw, err := json.Marshal(EventDeliveryPayloadV1{
		SubscriberID:      "notify:shoutrrr",
		PublishedSequence: 7,
		EventID:           42,
	})
	if err != nil {
		t.Fatalf("Marshal EventDeliveryPayloadV1: %v", err)
	}
	decoded, err := DecodeEventDeliveryPayload(raw)
	if err != nil {
		t.Fatalf("DecodeEventDeliveryPayload: %v", err)
	}
	if decoded.SubscriberID != "notify:shoutrrr" || decoded.PublishedSequence != 7 || decoded.EventID != 42 {
		t.Fatalf("decoded = %+v", decoded)
	}

	for _, bad := range []json.RawMessage{
		json.RawMessage(`not-json`),
		json.RawMessage(`[]`),
		json.RawMessage(`{"subscriber_id":"","published_sequence":1}`),
		json.RawMessage(`{"subscriber_id":"sub-1","published_sequence":0}`),
		json.RawMessage(`{"subscriber_id":"sub-1","published_sequence":-5}`),
	} {
		if _, err := DecodeEventDeliveryPayload(bad); !errors.Is(err, ErrInvalidDeliveryPayload) {
			t.Fatalf("DecodeEventDeliveryPayload(%s) err = %v, want ErrInvalidDeliveryPayload", bad, err)
		}
	}
}

func TestJobLifecycleEventKindAndProgressTick(t *testing.T) {
	cases := []struct {
		state  JobState
		want   EventKind
		wantOK bool
	}{
		{JobStateRunning, EventJobStarted, true},
		{JobStateSucceeded, EventJobFinished, true},
		{JobStateFailed, EventJobFailed, true},
		{JobStateCancelled, EventJobCancelled, true},
		{JobStateQueued, "", false},
		{JobState("UNKNOWN"), "", false},
	}
	for _, tc := range cases {
		got, ok := JobLifecycleEventKind(tc.state)
		if got != tc.want || ok != tc.wantOK {
			t.Fatalf("JobLifecycleEventKind(%s) = (%q, %v), want (%q, %v)", tc.state, got, ok, tc.want, tc.wantOK)
		}
	}

	if ProgressTickEventName != "job.progress" {
		t.Fatalf("ProgressTickEventName = %q, want job.progress", ProgressTickEventName)
	}
	if EventKind(ProgressTickEventName).IsValid() {
		t.Fatal("progress tick event name must not be a persisted outbox EventKind")
	}

	creatorID := "usr-creator"
	otherID := "usr-other"
	tick := JobProgressTick{
		JobID:        "job-1",
		Kind:         JobKindScan,
		CreatedBy:    &creatorID,
		ScannedCount: 42,
		CurrentPath:  "/comics/Series/v01.cbz",
		DurationMS:   150,
	}
	if err := tick.Validate(); err != nil {
		t.Fatalf("valid JobProgressTick failed: %v", err)
	}

	cloned := tick.Clone()
	*cloned.CreatedBy = "mutated"
	if *tick.CreatedBy != creatorID {
		t.Fatal("Clone must deep-copy CreatedBy")
	}

	creator := NewPrincipal(User{ID: creatorID, Username: "alice"}, []Role{mustBuiltInRole(t, RoleReaderID)}, CredentialKindPassword, "c1", nil)
	other := NewPrincipal(User{ID: otherID, Username: "bob"}, []Role{mustBuiltInRole(t, RoleReaderID)}, CredentialKindPassword, "c2", nil)
	admin := NewPrincipal(User{ID: otherID, Username: "admin"}, []Role{mustBuiltInRole(t, RoleAdminID)}, CredentialKindPassword, "c3", nil)

	if tick.VisibleTo(nil) || tick.VisibleTo(other) || !tick.VisibleTo(creator) || !tick.VisibleTo(admin) {
		t.Fatal("unexpected visibility result for user-created progress tick")
	}
	autoTick := tick
	autoTick.CreatedBy = nil
	if autoTick.VisibleTo(creator) || !autoTick.VisibleTo(admin) {
		t.Fatal("unexpected visibility result for automatic progress tick")
	}

	for _, bad := range []JobProgressTick{
		{JobID: " ", Kind: JobKindScan},
		{JobID: "job-1", Kind: ""},
		{JobID: "job-1", Kind: JobKindScan, ScannedCount: -1},
		{JobID: "job-1", Kind: JobKindScan, DurationMS: -1},
	} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidProgressTick) {
			t.Fatalf("Validate(%+v) = %v, want ErrInvalidProgressTick", bad, err)
		}
	}
}
