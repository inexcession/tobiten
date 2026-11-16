package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupStreamTestDB(t *testing.T) (
	*testdb.TestDB,
	*postgres.EventRepository,
	*postgres.JobRepository,
	*postgres.UserRepository,
	*postgres.SessionRepository,
) {
	t.Helper()
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("applying core migrations: %v", err)
	}
	return db,
		postgres.NewEventRepository(db.Pool),
		postgres.NewJobRepository(db.Pool),
		postgres.NewUserRepository(db.Pool),
		postgres.NewSessionRepository(db.Pool)
}

func createStreamTestUserWithCred(
	ctx context.Context,
	t *testing.T,
	userRepo *postgres.UserRepository,
	username, roleID string,
	credKind domain.CredentialKind,
	scopes []domain.PATScope,
	expiresAt *time.Time,
) (domain.User, *domain.Principal) {
	t.Helper()
	u := &domain.User{ID: domain.MustNewID(), Username: username}
	if err := userRepo.CreateUser(ctx, u); err != nil {
		t.Fatalf("CreateUser(%s): %v", username, err)
	}
	if roleID != "" {
		if err := userRepo.AssignUserRole(ctx, u.ID, roleID); err != nil {
			t.Fatalf("AssignUserRole(%s): %v", username, err)
		}
	}
	secret := "hash-" + username
	scheme := "argon2id"
	cred := &domain.Credential{
		ID:         domain.MustNewID(),
		UserID:     u.ID,
		Kind:       credKind,
		Scopes:     scopes,
		SecretHash: &secret,
		HashScheme: &scheme,
		ExpiresAt:  expiresAt,
	}
	if err := userRepo.CreateCredential(ctx, cred); err != nil {
		t.Fatalf("CreateCredential(%s): %v", username, err)
	}
	roles, err := userRepo.ListUserRoles(ctx, u.ID)
	if err != nil {
		t.Fatalf("ListUserRoles(%s): %v", username, err)
	}
	p := domain.NewPrincipal(*u, roles, cred.Kind, cred.ID, cred.Scopes)
	return *u, p
}

func TestStreamService_ValidationGuards(t *testing.T) {
	t.Parallel()
	var nilSvc *events.StreamService
	p := testPrincipalWithPerms("u-1", domain.PermissionLibraryRead)

	if err := nilSvc.Stream(context.Background(), nil, app.EventStreamFilter{}, nil, nil); !errors.Is(err, app.ErrUnauthorized) {
		t.Fatalf("nil principal err = %v, want ErrUnauthorized", err)
	}
	if err := nilSvc.Stream(context.Background(), p, app.EventStreamFilter{}, nil, func(app.StreamEvent) error { return nil }); !errors.Is(err, app.ErrServiceBusy) {
		t.Fatalf("nil service err = %v, want ErrServiceBusy", err)
	}
}

func TestStreamService_OrderedDrainVisibilityCatchupAndProgress(t *testing.T) {
	t.Parallel()
	db, eventRepo, jobRepo, userRepo, sessionRepo := setupStreamTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	alice, alicePrincipal := createStreamTestUserWithCred(ctx, t, userRepo, "stream-alice", domain.RoleReaderID, domain.CredentialKindPassword, nil, nil)
	bob, bobPrincipal := createStreamTestUserWithCred(ctx, t, userRepo, "stream-bob", domain.RoleReaderID, domain.CredentialKindPassword, nil, nil)
	_, readAllPrincipal := createStreamTestUserWithCred(ctx, t, userRepo, "stream-admin", domain.RoleAdminID, domain.CredentialKindPassword, nil, nil)
	tempCreator := &domain.User{ID: domain.MustNewID(), Username: "stream-temp"}
	if err := userRepo.CreateUser(ctx, tempCreator); err != nil {
		t.Fatalf("CreateUser(temp): %v", err)
	}

	progressHub, err := events.NewProgressHub(events.ProgressHubOptions{MinInterval: time.Nanosecond})
	if err != nil {
		t.Fatalf("NewProgressHub: %v", err)
	}
	dispatcher := events.NewDispatcher(eventRepo, events.WithDispatcherPollInterval(10*time.Second))
	if err := dispatcher.Start(ctx); err != nil {
		t.Fatalf("dispatcher.Start: %v", err)
	}
	defer dispatcher.Stop()

	// Use a very long fallback PollInterval so the test proves dispatcher wake signals drive the drain.
	svc := events.NewStreamService(eventRepo, jobRepo, events.StreamOptions{
		Users:        userRepo,
		Sessions:     sessionRepo,
		ProgressHub:  progressHub,
		WakeSource:   dispatcher,
		PollInterval: 30 * time.Second,
	})

	// Seed a historical job + lifecycle event BEFORE any stream attaches (no M2 replay yet).
	preJob := &domain.Job{Kind: domain.JobKindScan, CreatedBy: &alice.ID}
	if _, err := jobRepo.Enqueue(ctx, preJob); err != nil {
		t.Fatalf("Enqueue preJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindScan, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimNext preJob: %v", err)
	}
	if _, err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce preJob: %v", err)
	}

	startStream := func(p *domain.Principal, jobFilter string) (<-chan app.StreamEvent, context.CancelFunc) {
		streamCtx, cancelStream := context.WithCancel(ctx)
		ch := make(chan app.StreamEvent, 32)
		attached := make(chan struct{})
		go func() {
			_ = svc.Stream(streamCtx, p, app.EventStreamFilter{JobID: jobFilter}, func() error {
				close(attached)
				return nil
			}, func(ev app.StreamEvent) error {
				ch <- ev
				return nil
			})
		}()
		select {
		case <-attached:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for stream attach")
		}
		return ch, cancelStream
	}

	aliceCh, cancelAlice := startStream(alicePrincipal, "")
	defer cancelAlice()
	bobCh, cancelBob := startStream(bobPrincipal, "")
	defer cancelBob()
	adminCh, cancelAdmin := startStream(readAllPrincipal, "")
	defer cancelAdmin()

	// Append a non-job domain event (must be skipped by M1A job stream while advancing cursor)
	// and an internal EVENT_DELIVERY job (must emit no lifecycle events).
	if err := eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		_, err := events.NewOutbox().AppendPayload(ctx, txRepo, domain.EventAssetAdded, domain.EventVersion1, domain.AggregateTypeAsset, "asset-1", domain.AssetEventPayloadV1{
			AssetID:       "asset-1",
			VolumeID:      "vol-1",
			LibraryID:     "lib-1",
			LibraryRootID: "root-1",
		})
		return err
	}); err != nil {
		t.Fatalf("append asset.added: %v", err)
	}
	delivKey := "EVENT_DELIVERY:sub-test"
	delivJob := &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   json.RawMessage(`{"subscriber_id":"sub-test","published_sequence":1}`),
		DedupeKey: &delivKey,
	}
	if _, err := jobRepo.Enqueue(ctx, delivJob); err != nil {
		t.Fatalf("Enqueue delivJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindEventDelivery, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimNext delivJob: %v", err)
	}

	// Trigger Alice's job: ClaimNext -> ProgressHub tick -> MarkSucceeded, waking dispatcher on each transition.
	aliceJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &alice.ID}
	if _, err := jobRepo.Enqueue(ctx, aliceJob); err != nil {
		t.Fatalf("Enqueue aliceJob: %v", err)
	}
	claimedAlice, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext aliceJob: %v", err)
	}
	dispatcher.Notify()

	// Emit ephemeral progress tick (carries no SSE ID).
	if !progressHub.Publish(domain.JobProgressTick{
		JobID:        aliceJob.ID,
		Kind:         domain.JobKindRepack,
		CreatedBy:    &alice.ID,
		ScannedCount: 5,
		CurrentPath:  "/comics/ch1.cbz",
		DurationMS:   42,
	}) {
		t.Fatal("expected ProgressHub.Publish to succeed")
	}

	if _, err := jobRepo.MarkSucceeded(ctx, claimedAlice.ID, claimedAlice.Attempts, time.Now().UTC()); err != nil {
		t.Fatalf("MarkSucceeded aliceJob: %v", err)
	}
	dispatcher.Notify()

	// Also run an automatic (creatorless) job and a temp-creator job whose creator is deleted before dispatch!
	autoJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: nil}
	if _, err := jobRepo.Enqueue(ctx, autoJob); err != nil {
		t.Fatalf("Enqueue autoJob: %v", err)
	}
	claimedAuto, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext autoJob: %v", err)
	}
	if _, err := jobRepo.MarkSucceeded(ctx, claimedAuto.ID, claimedAuto.Attempts, time.Now().UTC()); err != nil {
		t.Fatalf("MarkSucceeded autoJob: %v", err)
	}

	deletedCreatorJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &tempCreator.ID}
	if _, err := jobRepo.Enqueue(ctx, deletedCreatorJob); err != nil {
		t.Fatalf("Enqueue deletedCreatorJob: %v", err)
	}
	claimedDel, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext deletedCreatorJob: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM users WHERE id = $1`, tempCreator.ID); err != nil {
		t.Fatalf("delete tempCreator: %v", err)
	}
	dispatcher.Notify()

	// Verify Alice receives her job.started, job.progress (no ID!), and job.finished in order, and NO preJob or autoJob or deletedCreatorJob!
	var aliceLifecycle []app.StreamEvent
	var aliceProgress []app.StreamEvent
	deadline := time.After(5 * time.Second)
	for len(aliceLifecycle) < 2 || len(aliceProgress) < 1 {
		select {
		case ev := <-aliceCh:
			if ev.Name == domain.ProgressTickEventName {
				if ev.ID != "" {
					t.Fatalf("progress tick carried SSE ID %q, want empty", ev.ID)
				}
				aliceProgress = append(aliceProgress, ev)
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err != nil {
				t.Fatalf("unmarshal PublicEvent: %v", err)
			}
			if pub.AggregateID == preJob.ID {
				t.Fatalf("Alice received pre-attach event for %s (replay should not run in M1A)", preJob.ID)
			}
			if pub.AggregateID != aliceJob.ID {
				t.Fatalf("Alice received event for non-owned job %s (%s)", pub.AggregateID, ev.Name)
			}
			aliceLifecycle = append(aliceLifecycle, ev)
		case <-deadline:
			t.Fatalf("timed out waiting for Alice events (lifecycle=%d progress=%d)", len(aliceLifecycle), len(aliceProgress))
		}
	}
	if aliceLifecycle[0].Name != string(domain.EventJobStarted) || aliceLifecycle[1].Name != string(domain.EventJobFinished) {
		t.Fatalf("Alice lifecycle order = [%s %s], want [job.started job.finished]", aliceLifecycle[0].Name, aliceLifecycle[1].Name)
	}
	if aliceLifecycle[0].ID == "" || aliceLifecycle[1].ID == "" || aliceLifecycle[0].ID >= aliceLifecycle[1].ID {
		t.Fatalf("Alice lifecycle IDs = [%q %q], want ascending sequence IDs", aliceLifecycle[0].ID, aliceLifecycle[1].ID)
	}

	// Now trigger a job owned by Bob; Bob must receive ONLY Bob's job events (nothing from Alice/auto/deleted).
	bobJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &bob.ID}
	if _, err := jobRepo.Enqueue(ctx, bobJob); err != nil {
		t.Fatalf("Enqueue bobJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimNext bobJob: %v", err)
	}
	dispatcher.Notify()

	select {
	case ev := <-bobCh:
		var pub app.PublicEvent
		if err := json.Unmarshal(ev.Data, &pub); err != nil {
			t.Fatalf("unmarshal Bob event: %v", err)
		}
		if pub.AggregateID != bobJob.ID {
			t.Fatalf("non-creator Bob received event for %s (%s), want only %s", pub.AggregateID, ev.Name, bobJob.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bob's own job.started event")
	}

	// Admin with jobs:read_all receives Alice's, autoJob's, deletedCreatorJob's, and Bob's lifecycle events.
	seenByAdmin := make(map[string][]string)
	adminDeadline := time.After(5 * time.Second)
	for len(seenByAdmin[aliceJob.ID]) < 2 || len(seenByAdmin[autoJob.ID]) < 2 || len(seenByAdmin[claimedDel.ID]) < 1 || len(seenByAdmin[bobJob.ID]) < 1 {
		select {
		case ev := <-adminCh:
			if ev.Name == domain.ProgressTickEventName {
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err != nil {
				t.Fatalf("unmarshal admin event: %v", err)
			}
			seenByAdmin[pub.AggregateID] = append(seenByAdmin[pub.AggregateID], ev.Name)
		case <-adminDeadline:
			t.Fatalf("timed out waiting for admin events, saw: %+v", seenByAdmin)
		}
	}
	if len(seenByAdmin[delivJob.ID]) != 0 {
		t.Fatalf("admin saw internal EVENT_DELIVERY lifecycle events: %v", seenByAdmin[delivJob.ID])
	}
}

func TestStreamService_LiveCredentialRevocationExpirySessionAndDemotion(t *testing.T) {
	t.Parallel()
	_, eventRepo, jobRepo, userRepo, sessionRepo := setupStreamTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var clockMu sync.Mutex
	now := time.Now().UTC()
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	progressHub, err := events.NewProgressHub(events.ProgressHubOptions{
		MinInterval: time.Nanosecond,
		Now:         nowFn,
	})
	if err != nil {
		t.Fatalf("NewProgressHub: %v", err)
	}
	dispatcher := events.NewDispatcher(eventRepo, events.WithDispatcherPollInterval(10*time.Second))
	if err := dispatcher.Start(ctx); err != nil {
		t.Fatalf("dispatcher.Start: %v", err)
	}
	defer dispatcher.Stop()

	svc := events.NewStreamService(eventRepo, jobRepo, events.StreamOptions{
		Users:        userRepo,
		Sessions:     sessionRepo,
		ProgressHub:  progressHub,
		WakeSource:   dispatcher,
		PollInterval: 30 * time.Second,
		Now:          nowFn,
	})

	startStream := func(p *domain.Principal) (<-chan app.StreamEvent, <-chan error, context.CancelFunc) {
		streamCtx, cancelStream := context.WithCancel(ctx)
		ch := make(chan app.StreamEvent, 32)
		done := make(chan error, 1)
		attached := make(chan struct{})
		go func() {
			done <- svc.Stream(streamCtx, p, app.EventStreamFilter{}, func() error {
				close(attached)
				return nil
			}, func(ev app.StreamEvent) error {
				ch <- ev
				return nil
			})
		}()
		select {
		case <-attached:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for stream attach")
		}
		return ch, done, cancelStream
	}

	// 1. PAT revocation and PAT expiry terminate open streams and drop buffered progress frames.
	revokedUser, revokedPAT := createStreamTestUserWithCred(
		ctx, t, userRepo, "pat-revoked", domain.RoleAdminID, domain.CredentialKindPAT, []domain.PATScope{domain.ScopeAdmin}, nil,
	)
	expAt := nowFn().Add(5 * time.Minute)
	_, expiringPAT := createStreamTestUserWithCred(
		ctx, t, userRepo, "pat-expiring", domain.RoleAdminID, domain.CredentialKindPAT, []domain.PATScope{domain.ScopeAdmin}, &expAt,
	)
	sessUser, sessBasePrincipal := createStreamTestUserWithCred(
		ctx, t, userRepo, "sess-revoked", domain.RoleAdminID, domain.CredentialKindPassword, nil, nil,
	)
	sessID := domain.MustNewID()
	credObj, err := userRepo.GetCredentialByID(ctx, sessBasePrincipal.CredentialID)
	if err != nil {
		t.Fatalf("GetCredentialByID: %v", err)
	}
	if err := sessionRepo.CreateSession(ctx, &domain.Session{
		ID:                   sessID,
		UserID:               sessUser.ID,
		CredentialID:         credObj.ID,
		Kind:                 domain.SessionKindNative,
		TokenHash:            domain.HashSHA256Hex("tok-" + sessID),
		CSRFTokenHash:        domain.HashSHA256Hex("csrf-" + sessID),
		CredentialHashDigest: domain.HashSHA256Hex(*credObj.SecretHash),
		CreatedAt:            nowFn(),
		ExpiresAt:            nowFn().Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sessPrincipal := domain.NewPrincipal(sessUser, sessBasePrincipal.Roles, credObj.Kind, credObj.ID, nil)
	sessPrincipal.SessionID = sessID

	// 2. Role downgrade user (Admin -> Reader) + positive control Admin.
	demoteUser, demotePrincipal := createStreamTestUserWithCred(
		ctx, t, userRepo, "demote-admin", domain.RoleAdminID, domain.CredentialKindPassword, nil, nil,
	)
	_, stayAdminPrincipal := createStreamTestUserWithCred(
		ctx, t, userRepo, "stay-admin", domain.RoleAdminID, domain.CredentialKindPassword, nil, nil,
	)

	revokedCh, revokedDone, cancelRevoked := startStream(revokedPAT)
	defer cancelRevoked()
	expiringCh, expiringDone, cancelExpiring := startStream(expiringPAT)
	defer cancelExpiring()
	sessCh, sessDone, cancelSess := startStream(sessPrincipal)
	defer cancelSess()
	demoteCh, _, cancelDemote := startStream(demotePrincipal)
	defer cancelDemote()
	stayAdminCh, _, cancelStay := startStream(stayAdminPrincipal)
	defer cancelStay()

	// Pause emission for demoteCh? Even simpler: revoke/expire/demote BEFORE emitting new progress/lifecycle events,
	// AND test buffered progress by blocking emit on a test channel for demotePrincipal so progressCh buffers a tick
	// while demotePrincipal is still Admin, then downgrade demoteUser's role before unblocking emit!
	bufferedGate := make(chan struct{})
	bufferedEntered := make(chan struct{})
	bufferedEvents := make(chan app.StreamEvent, 8)
	bufCtx, cancelBuf := context.WithCancel(ctx)
	defer cancelBuf()
	bufAttached := make(chan struct{})
	var gateOnce sync.Once
	go func() {
		_ = svc.Stream(bufCtx, demotePrincipal, app.EventStreamFilter{}, func() error {
			close(bufAttached)
			return nil
		}, func(ev app.StreamEvent) error {
			gateOnce.Do(func() {
				close(bufferedEntered)
				<-bufferedGate
			})
			bufferedEvents <- ev
			return nil
		})
	}()
	select {
	case <-bufAttached:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for buffered test stream attach")
	}

	// Enqueue an automatic (creatorless) job to trigger the first emit (blocking bufferedGate) and buffer a progress tick.
	gateJob := &domain.Job{Kind: domain.JobKindScan, CreatedBy: nil, RunAfter: nowFn(), CreatedAt: nowFn()}
	if _, err := jobRepo.Enqueue(ctx, gateJob); err != nil {
		t.Fatalf("Enqueue gateJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindScan, nowFn()); err != nil {
		t.Fatalf("ClaimNext gateJob: %v", err)
	}
	dispatcher.Notify()
	select {
	case <-bufferedEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for buffered stream first emit")
	}

	// Publish a progress tick for gateJob into ProgressHub while demoteUser is still Admin (it sits buffered in progressCh).
	if !progressHub.Publish(domain.JobProgressTick{
		JobID:        gateJob.ID,
		Kind:         domain.JobKindScan,
		CreatedBy:    nil,
		ScannedCount: 99,
		CurrentPath:  "/secret-auto.cbz",
		DurationMS:   15,
	}) {
		t.Fatal("expected buffered progress Publish to succeed")
	}

	// Now revoke PAT, expire expiringPAT via clock advance, revoke session, and downgrade demoteUser -> Reader!
	if err := userRepo.RevokeCredential(ctx, revokedPAT.CredentialID, nowFn()); err != nil {
		t.Fatalf("RevokeCredential: %v", err)
	}
	advanceClock(10 * time.Minute)
	if err := sessionRepo.RevokeSession(ctx, sessID, nowFn()); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
	if err := userRepo.RemoveUserRole(ctx, demoteUser.ID, domain.RoleAdminID); err != nil {
		t.Fatalf("RemoveUserRole(demoteUser, Admin): %v", err)
	}
	if err := userRepo.AssignUserRole(ctx, demoteUser.ID, domain.RoleReaderID); err != nil {
		t.Fatalf("AssignUserRole(demoteUser, Reader): %v", err)
	}

	// Release the buffered stream and verify the buffered automatic-job progress tick is DROPPED!
	close(bufferedGate)

	// Trigger a new automatic job + a demoteUser-owned job after revocation/expiry/downgrade.
	autoAfter := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: nil, RunAfter: nowFn(), CreatedAt: nowFn()}
	if _, err := jobRepo.Enqueue(ctx, autoAfter); err != nil {
		t.Fatalf("Enqueue autoAfter: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, nowFn()); err != nil {
		t.Fatalf("ClaimNext autoAfter: %v", err)
	}
	_ = progressHub.Publish(domain.JobProgressTick{
		JobID:        autoAfter.ID,
		Kind:         domain.JobKindRepack,
		CreatedBy:    nil,
		ScannedCount: 1,
		CurrentPath:  "/auto-after.cbz",
		DurationMS:   5,
	})

	// Trigger a job created by revokedUser (must NOT be received on revokedCh because credential was revoked!).
	revokedOwnJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &revokedUser.ID, RunAfter: nowFn(), CreatedAt: nowFn()}
	if _, err := jobRepo.Enqueue(ctx, revokedOwnJob); err != nil {
		t.Fatalf("Enqueue revokedOwnJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, nowFn()); err != nil {
		t.Fatalf("ClaimNext revokedOwnJob: %v", err)
	}

	// Trigger a job created by demoteUser (demoteUser lost jobs:read_all, so they must NOT see gateJob/autoAfter,
	// ONLY demoteOwnJob!).
	demoteOwnJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &demoteUser.ID, RunAfter: nowFn(), CreatedAt: nowFn()}
	if _, err := jobRepo.Enqueue(ctx, demoteOwnJob); err != nil {
		t.Fatalf("Enqueue demoteOwnJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, nowFn()); err != nil {
		t.Fatalf("ClaimNext demoteOwnJob: %v", err)
	}
	_ = progressHub.Publish(domain.JobProgressTick{
		JobID:        demoteOwnJob.ID,
		Kind:         domain.JobKindRepack,
		CreatedBy:    &demoteUser.ID,
		ScannedCount: 2,
		CurrentPath:  "/own.cbz",
		DurationMS:   7,
	})
	dispatcher.Notify()

	// Revoked PAT, expired PAT, and revoked session streams must terminate cleanly without delivering post-revocation events.
	for name, done := range map[string]<-chan error{
		"revoked PAT":     revokedDone,
		"expiring PAT":    expiringDone,
		"revoked session": sessDone,
	} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s Stream returned err = %v, want nil", name, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s Stream to terminate", name)
		}
	}
	for name, ch := range map[string]<-chan app.StreamEvent{
		"revoked PAT":     revokedCh,
		"expiring PAT":    expiringCh,
		"revoked session": sessCh,
	} {
		for len(ch) > 0 {
			ev := <-ch
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err == nil && (pub.AggregateID == autoAfter.ID || pub.AggregateID == revokedOwnJob.ID || pub.AggregateID == demoteOwnJob.ID) {
				t.Fatalf("%s received post-invalidation event %+v", name, ev)
			}
		}
	}

	// Verify demoteCh receives ONLY demoteOwnJob (never autoAfter or revokedOwnJob), and bufferedEvents drops gateJob's buffered tick!
	gotOwnLifecycle := false
	gotOwnProgress := false
	demoteDeadline := time.After(5 * time.Second)
	for !gotOwnLifecycle || !gotOwnProgress {
		select {
		case ev := <-demoteCh:
			if ev.Name == domain.ProgressTickEventName {
				var tick domain.JobProgressTick
				if err := json.Unmarshal(ev.Data, &tick); err != nil {
					t.Fatalf("unmarshal demote tick: %v", err)
				}
				if tick.JobID != demoteOwnJob.ID && tick.JobID != gateJob.ID {
					t.Fatalf("demoted user leaked progress tick for %q", tick.JobID)
				}
				if tick.JobID == gateJob.ID {
					// gateJob tick was published before demotion; let's check bufferedEvents too.
					continue
				}
				gotOwnProgress = true
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err != nil {
				t.Fatalf("unmarshal demote lifecycle: %v", err)
			}
			if pub.AggregateID == autoAfter.ID || pub.AggregateID == revokedOwnJob.ID {
				t.Fatalf("demoted user received unauthorized post-demotion event for %q", pub.AggregateID)
			}
			if pub.AggregateID == demoteOwnJob.ID {
				gotOwnLifecycle = true
			}
		case <-demoteDeadline:
			t.Fatalf("timed out waiting for demoted user's own job events (lifecycle=%v progress=%v)", gotOwnLifecycle, gotOwnProgress)
		}
	}

	// Check bufferedEvents: first event was gateJob job.started; subsequent events must NOT include gateJob's buffered progress tick or autoAfter!
	var seenBuffered []app.StreamEvent
	bufDeadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-bufferedEvents:
			seenBuffered = append(seenBuffered, ev)
			if ev.Name == string(domain.EventJobStarted) {
				var pub app.PublicEvent
				if err := json.Unmarshal(ev.Data, &pub); err == nil && pub.AggregateID == demoteOwnJob.ID {
					goto checkedBuffered
				}
			}
		case <-bufDeadline:
			t.Fatalf("timed out waiting for demoteOwnJob on buffered stream, got: %+v", seenBuffered)
		}
	}
checkedBuffered:
	for _, ev := range seenBuffered {
		if ev.Name == domain.ProgressTickEventName {
			var tick domain.JobProgressTick
			if err := json.Unmarshal(ev.Data, &tick); err == nil && (tick.JobID == gateJob.ID || tick.JobID == autoAfter.ID) {
				t.Fatalf("demoted buffered stream emitted unauthorized progress tick for %q", tick.JobID)
			}
		} else {
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err == nil && pub.AggregateID == autoAfter.ID {
				t.Fatalf("demoted buffered stream emitted unauthorized lifecycle event for %q", pub.AggregateID)
			}
		}
	}

	// Positive control: stayAdminCh receives autoAfter, revokedOwnJob, and demoteOwnJob.
	seenStay := make(map[string]bool)
	stayDeadline := time.After(5 * time.Second)
	for !seenStay[autoAfter.ID] || !seenStay[revokedOwnJob.ID] || !seenStay[demoteOwnJob.ID] {
		select {
		case ev := <-stayAdminCh:
			if ev.Name == domain.ProgressTickEventName {
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err == nil {
				seenStay[pub.AggregateID] = true
			}
		case <-stayDeadline:
			t.Fatalf("timed out waiting for positive-control admin events, saw: %+v", seenStay)
		}
	}
}
