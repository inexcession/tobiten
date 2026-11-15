package postgres

import (
	"encoding/json"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/jackc/pgx/v5/pgtype"
)

// JobToDomain converts a sqlc-generated Job row into a domain Job.
func JobToDomain(row Job) domain.Job {
	return domain.Job{
		ID:              row.ID,
		Kind:            domain.JobKind(row.Kind),
		State:           domain.JobState(row.State),
		Payload:         json.RawMessage(row.Payload),
		Checkpoint:      json.RawMessage(row.Checkpoint),
		Attempts:        int(row.Attempts),
		MaxAttempts:     int(row.MaxAttempts),
		CancelRequested: row.CancelRequested,
		LastError:       row.LastError,
		CreatedBy:       row.CreatedBy,
		DedupeKey:       row.DedupeKey,
		RunAfter:        row.RunAfter,
		CreatedAt:       row.CreatedAt,
		StartedAt:       row.StartedAt,
		UpdatedAt:       row.UpdatedAt,
		FinishedAt:      row.FinishedAt,
	}
}

// JobFromDomain converts a domain Job into a sqlc-generated Job row.
func JobFromDomain(j domain.Job) Job {
	return Job{
		ID:              j.ID,
		Kind:            string(j.Kind),
		State:           string(j.State),
		Payload:         []byte(j.Payload),
		Checkpoint:      []byte(j.Checkpoint),
		Attempts:        int32(j.Attempts),
		MaxAttempts:     int32(j.MaxAttempts),
		CancelRequested: j.CancelRequested,
		LastError:       j.LastError,
		CreatedBy:       j.CreatedBy,
		DedupeKey:       j.DedupeKey,
		RunAfter:        j.RunAfter,
		CreatedAt:       j.CreatedAt,
		StartedAt:       j.StartedAt,
		UpdatedAt:       j.UpdatedAt,
		FinishedAt:      j.FinishedAt,
	}
}

// DomainEventToDomain converts a sqlc-generated DomainEvent row into a domain DomainEvent.
func DomainEventToDomain(row DomainEvent) domain.DomainEvent {
	var publishedSeq *int64
	if row.PublishedSequence.Valid {
		v := row.PublishedSequence.Int64
		publishedSeq = &v
	}
	return domain.DomainEvent{
		ID:                row.ID,
		PublishedSequence: publishedSeq,
		PublishedAt:       row.PublishedAt,
		Kind:              domain.EventKind(row.Kind),
		Version:           int(row.Version),
		AggregateType:     row.AggregateType,
		AggregateID:       row.AggregateID,
		Payload:           json.RawMessage(row.Payload),
		CreatedAt:         row.CreatedAt,
	}
}

// DomainEventFromDomain converts a domain DomainEvent into a sqlc-generated DomainEvent row.
func DomainEventFromDomain(e domain.DomainEvent) DomainEvent {
	var publishedSeq pgtype.Int8
	if e.PublishedSequence != nil {
		publishedSeq = pgtype.Int8{
			Int64: *e.PublishedSequence,
			Valid: true,
		}
	}
	return DomainEvent{
		ID:                e.ID,
		PublishedSequence: publishedSeq,
		PublishedAt:       e.PublishedAt,
		Kind:              string(e.Kind),
		Version:           int32(e.Version),
		AggregateType:     e.AggregateType,
		AggregateID:       e.AggregateID,
		Payload:           []byte(e.Payload),
		CreatedAt:         e.CreatedAt,
	}
}

// EventSubscriberToDomain converts a sqlc-generated EventSubscriber row into a domain EventSubscriber.
func EventSubscriberToDomain(row EventSubscriber) domain.EventSubscriber {
	return domain.EventSubscriber{
		ID:        row.ID,
		Kind:      row.Kind,
		Position:  row.Position,
		State:     domain.SubscriberState(row.State),
		StalledAt: row.StalledAt,
		UpdatedAt: row.UpdatedAt,
	}
}

// EventSubscriberFromDomain converts a domain EventSubscriber into a sqlc-generated EventSubscriber row.
func EventSubscriberFromDomain(s domain.EventSubscriber) EventSubscriber {
	return EventSubscriber{
		ID:        s.ID,
		Kind:      s.Kind,
		Position:  s.Position,
		State:     string(s.State),
		StalledAt: s.StalledAt,
		UpdatedAt: s.UpdatedAt,
	}
}
