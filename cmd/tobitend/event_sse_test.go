package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

type sseFrame struct {
	ID    string
	Event string
	Data  string
}

type sseClientStream struct {
	resp   *http.Response
	cancel context.CancelFunc
	frames chan sseFrame
	errCh  chan error
}

func openSSEStream(t *testing.T, url, user, pass, bearerToken string) *sseClientStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("NewRequest(%s): %v", url, err)
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		t.Fatalf("GET %s status = %d, want 200 (body: %s)", url, resp.StatusCode, string(body))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		_ = resp.Body.Close()
		cancel()
		t.Fatalf("GET %s Content-Type = %q, want text/event-stream", url, ct)
	}

	s := &sseClientStream{
		resp:   resp,
		cancel: cancel,
		frames: make(chan sseFrame, 64),
		errCh:  make(chan error, 1),
	}

	go func() {
		defer close(s.frames)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var cur sseFrame
		var dataLines []string
		hasFields := false

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if hasFields {
					cur.Data = strings.Join(dataLines, "\n")
					s.frames <- cur
					cur = sseFrame{}
					dataLines = nil
					hasFields = false
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue
			}
			field, val, hasColon := strings.Cut(line, ":")
			if hasColon {
				val = strings.TrimPrefix(val, " ")
			}
			switch field {
			case "id":
				cur.ID = val
				hasFields = true
			case "event":
				cur.Event = val
				hasFields = true
			case "data":
				dataLines = append(dataLines, val)
				hasFields = true
			}
		}
		if err := scanner.Err(); err != nil {
			s.errCh <- err
		}
	}()

	t.Cleanup(func() {
		s.close()
	})
	return s
}

func (s *sseClientStream) close() {
	if s == nil {
		return
	}
	s.cancel()
	if s.resp != nil && s.resp.Body != nil {
		_ = s.resp.Body.Close()
	}
}

func (s *sseClientStream) nextFrame(t *testing.T) sseFrame {
	t.Helper()
	select {
	case f, ok := <-s.frames:
		if !ok {
			select {
			case err := <-s.errCh:
				t.Fatalf("SSE stream closed with error: %v", err)
			default:
				t.Fatal("SSE stream closed unexpectedly")
			}
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SSE frame")
		return sseFrame{}
	}
}

type threadSafeLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *threadSafeLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestDaemonEventsSSE_RealStreamOrderVisibilityAndLogging(t *testing.T) {
	db := testdb.New(t)
	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=sysadmin",
		"TOBITEN_ADMIN_PASS=admin-secret-123",
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	originalRegistry := buildDaemonRegistry
	originalServer := startServer
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistry
		startServer = originalServer
		slog.SetDefault(originalLogger)
	})

	var mu sync.Mutex
	var sharedRunner *jobs.Runner

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		jobRepo := postgres.NewJobRepository(db.Pool)

		// Include a colliding /api/v1 module so compositeHandler fallbackRecorder is exercised
		// alongside the OpenAPI /api/v1/events SSE stream.
		colliderMux := http.NewServeMux()
		colliderMux.HandleFunc("GET /api/v1/sse-collider-ping", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("pong"))
		})
		_ = reg.Register(&module.SimpleModule{
			ModuleName: "sse-collider",
			Mounts: []module.RouteMount{
				{ModuleName: "sse-collider", Prefix: "/api/v1", Handler: colliderMux},
			},
		})

		probeMod := &testJobRegistrarModule{
			SimpleModule: &module.SimpleModule{
				ModuleName: "sse-job-probe",
				Jobs: []module.JobKind{
					module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint, Workers: 1},
				},
				Mounts: []module.RouteMount{
					{
						ModuleName: "sse-job-probe",
						Prefix:     "/api/v1/test-jobs",
						Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.Method != http.MethodPost || r.URL.Path != "/api/v1/test-jobs/trigger" {
								http.NotFound(w, r)
								return
							}
							p := domain.MustPrincipalFromContext(r.Context())
							var body struct {
								Automatic bool `json:"automatic"`
							}
							if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
								middleware.WriteAppError(w, r, app.ErrInvalidInput)
								return
							}
							var createdBy *string
							if !body.Automatic {
								uid := p.User.ID
								createdBy = &uid
							}
							mu.Lock()
							notifier := sharedRunner
							mu.Unlock()
							middleware.HandleJobTrigger(w, r, func(ctx context.Context) (string, error) {
								return jobs.TriggerJob(ctx, jobRepo, notifier, jobs.TriggerRequest{
									Kind:      domain.JobKindScan,
									Payload:   json.RawMessage(`{"source":"sse-probe"}`),
									CreatedBy: createdBy,
								}, time.Now().UTC())
							})
						}),
					},
				},
			},
			register: func(r *jobs.Runner) error {
				mu.Lock()
				sharedRunner = r
				mu.Unlock()
				return r.RegisterKind(jobs.KindConfig{
					Kind:         domain.JobKindScan,
					ResumePolicy: domain.JobResumeCheckpoint,
					Handler: jobs.HandlerFunc(func(execCtx context.Context, exec *jobs.Execution) error {
						exec.ReportProgress(execCtx, 1, "/library/vol1.cbz")
						return nil
					}),
				})
			},
		}
		if err := reg.Register(probeMod); err != nil {
			t.Fatalf("register probeMod: %v", err)
		}
		return reg
	}

	var logBuf threadSafeLogBuffer

	startServer = func(ctx context.Context, cfg *config.Config, _ *slog.Logger, services *app.Services) error {
		if services == nil || services.Events == nil {
			t.Fatal("expected daemon to wire services.Events")
		}
		testLogger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		reg := buildDaemonRegistry(cfg, services)
		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   testLogger,
			Registry: reg,
		})
		if err != nil {
			return err
		}
		srvCtx, cancelSrv := context.WithCancel(ctx)
		srvDone := make(chan error, 1)
		go func() {
			srvDone <- srv.Run(srvCtx)
		}()
		defer func() {
			cancelSrv()
			if srvErr := <-srvDone; srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
				t.Errorf("srv.Run: %v", srvErr)
			}
		}()

		baseURL := srv.URL()
		userRepo := postgres.NewUserRepository(db.Pool)
		sysadmin, err := userRepo.GetUserByUsername(ctx, "sysadmin")
		if err != nil {
			t.Fatalf("GetUserByUsername(sysadmin): %v", err)
		}
		alice := createTestUserWithPassword(ctx, t, userRepo, "sse-alice", "alice-pass-123", domain.RoleReaderID)
		bob := createTestUserWithPassword(ctx, t, userRepo, "sse-bob", "bob-pass-123", domain.RoleReaderID)

		adminReadPAT := createTestPAT(ctx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeRead})
		adminFullPAT := createTestPAT(ctx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeAdmin})

		httpClient := &http.Client{Timeout: 5 * time.Second}

		// 1. Unauthenticated /api/v1/events and /comics/api/v1/events fail closed with 401.
		for _, path := range []string{"/api/v1/events", "/comics/api/v1/events"} {
			resp, err := httpClient.Get(baseURL + path)
			if err != nil {
				t.Fatalf("unauthenticated GET %s: %v", path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated GET %s status = %d, want 401", path, resp.StatusCode)
			}
			if !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Basic") {
				t.Errorf("unauthenticated GET %s missing Basic challenge", path)
			}
		}

		// 2. Open live SSE streams for Alice (creator), Bob (non-creator, via /comics subpath),
		//    Admin with full admin PAT (has jobs:read_all), and Admin with read-only PAT (no jobs:read_all).
		aliceStream := openSSEStream(t, baseURL+"/api/v1/events", alice.Username, "alice-pass-123", "")
		bobStream := openSSEStream(t, baseURL+"/comics/api/v1/events", bob.Username, "bob-pass-123", "")
		adminFullStream := openSSEStream(t, baseURL+"/api/v1/events", "", "", adminFullPAT)
		adminReadStream := openSSEStream(t, baseURL+"/api/v1/events", "", "", adminReadPAT)

		trigger := func(user, pass, body string) string {
			req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/test-jobs/trigger", strings.NewReader(body))
			if err != nil {
				t.Fatalf("NewRequest trigger: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth(user, pass)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("POST trigger: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("trigger status = %d, want 202 (body: %s)", resp.StatusCode, string(b))
			}
			var out middleware.JobAcceptedResponse
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatalf("decode trigger response: %v", err)
			}
			return out.JobID
		}

		// 3. Alice triggers her own job.
		aliceJobID := trigger(alice.Username, "alice-pass-123", `{"automatic":false}`)

		// Alice receives flushed frames while the stream stays open: job.started, optional job.progress (no ID!), and job.finished in order.
		var aliceLifecycle []sseFrame
		var sawAliceProgress bool
		for len(aliceLifecycle) < 2 || !sawAliceProgress {
			f := aliceStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				if f.ID != "" {
					t.Fatalf("Alice progress frame unexpectedly carried SSE ID %q", f.ID)
				}
				var tick domain.JobProgressTick
				if err := json.Unmarshal([]byte(f.Data), &tick); err != nil {
					t.Fatalf("unmarshal Alice progress tick: %v", err)
				}
				if tick.JobID != aliceJobID {
					t.Fatalf("Alice progress tick job_id = %q, want %q", tick.JobID, aliceJobID)
				}
				sawAliceProgress = true
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal Alice lifecycle frame %+v: %v", f, err)
			}
			if pub.AggregateID != aliceJobID {
				t.Fatalf("Alice received unexpected job %q, want %q", pub.AggregateID, aliceJobID)
			}
			seq, err := strconv.ParseInt(f.ID, 10, 64)
			if err != nil || seq != pub.Sequence || seq <= 0 {
				t.Fatalf("Alice SSE id=%q vs pub.Sequence=%d invalid", f.ID, pub.Sequence)
			}
			aliceLifecycle = append(aliceLifecycle, f)
		}
		if aliceLifecycle[0].Event != string(domain.EventJobStarted) || aliceLifecycle[1].Event != string(domain.EventJobFinished) {
			t.Fatalf("Alice events = [%s %s], want [job.started job.finished]", aliceLifecycle[0].Event, aliceLifecycle[1].Event)
		}
		seq0, _ := strconv.ParseInt(aliceLifecycle[0].ID, 10, 64)
		seq1, _ := strconv.ParseInt(aliceLifecycle[1].ID, 10, 64)
		if seq0 >= seq1 {
			t.Fatalf("Alice sequence order = [%d %d], want strictly increasing", seq0, seq1)
		}

		// Admin with full admin PAT also receives Alice's job.started then job.finished.
		var adminAlice []sseFrame
		for len(adminAlice) < 2 {
			f := adminFullStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				if f.ID != "" {
					t.Fatalf("Admin progress frame unexpectedly carried SSE ID %q", f.ID)
				}
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal Admin frame: %v", err)
			}
			if pub.AggregateID != aliceJobID {
				t.Fatalf("Admin expected Alice job %q, got %q", aliceJobID, pub.AggregateID)
			}
			adminAlice = append(adminAlice, f)
		}
		if adminAlice[0].Event != string(domain.EventJobStarted) || adminAlice[1].Event != string(domain.EventJobFinished) {
			t.Fatalf("Admin Alice events = [%s %s], want [job.started job.finished]", adminAlice[0].Event, adminAlice[1].Event)
		}

		// 4. Trigger an automatic (creatorless) job; only Admin with jobs:read_all receives it!
		autoJobID := trigger("sysadmin", "admin-secret-123", `{"automatic":true}`)

		var adminAuto []sseFrame
		for len(adminAuto) < 2 {
			f := adminFullStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				if f.ID != "" {
					t.Fatalf("Admin auto progress frame carried SSE ID %q", f.ID)
				}
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal Admin auto frame: %v", err)
			}
			if pub.AggregateID != autoJobID {
				t.Fatalf("Admin expected auto job %q, got %q", autoJobID, pub.AggregateID)
			}
			adminAuto = append(adminAuto, f)
		}
		if adminAuto[0].Event != string(domain.EventJobStarted) || adminAuto[1].Event != string(domain.EventJobFinished) {
			t.Fatalf("Admin auto events = [%s %s], want [job.started job.finished]", adminAuto[0].Event, adminAuto[1].Event)
		}

		// 5. Bob triggers Bob's own job: Bob's very first received frame must be Bob's job (proving zero leakage
		//    of Alice's job or the automatic job onto Bob's open stream).
		bobJobID := trigger(bob.Username, "bob-pass-123", `{"automatic":false}`)
		var bobLifecycle []sseFrame
		for len(bobLifecycle) < 2 {
			f := bobStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				if f.ID != "" {
					t.Fatalf("Bob progress frame carried SSE ID %q", f.ID)
				}
				var tick domain.JobProgressTick
				if err := json.Unmarshal([]byte(f.Data), &tick); err != nil {
					t.Fatalf("unmarshal Bob progress tick: %v", err)
				}
				if tick.JobID != bobJobID {
					t.Fatalf("non-creator Bob leaked progress tick for %q (want %q)", tick.JobID, bobJobID)
				}
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal Bob frame: %v", err)
			}
			if pub.AggregateID != bobJobID {
				t.Fatalf("non-creator Bob received event for %q (%s), want only %q", pub.AggregateID, f.Event, bobJobID)
			}
			bobLifecycle = append(bobLifecycle, f)
		}
		if bobLifecycle[0].Event != string(domain.EventJobStarted) || bobLifecycle[1].Event != string(domain.EventJobFinished) {
			t.Fatalf("Bob events = [%s %s], want [job.started job.finished]", bobLifecycle[0].Event, bobLifecycle[1].Event)
		}

		// 6. Admin with read-only PAT triggers nothing of its own and receives nothing from Alice, auto, or Bob;
		//    to prove its stream is live without leakage, trigger a sysadmin-created job and verify that is its first event.
		sysOwnedJobID := trigger("sysadmin", "admin-secret-123", `{"automatic":false}`)
		var readPATLifecycle []sseFrame
		for len(readPATLifecycle) < 2 {
			f := adminReadStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				var tick domain.JobProgressTick
				if err := json.Unmarshal([]byte(f.Data), &tick); err != nil {
					t.Fatalf("unmarshal readPAT progress tick: %v", err)
				}
				if tick.JobID != sysOwnedJobID {
					t.Fatalf("read-only PAT leaked progress tick for %q (want %q)", tick.JobID, sysOwnedJobID)
				}
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal readPAT frame: %v", err)
			}
			if pub.AggregateID != sysOwnedJobID {
				t.Fatalf("read-only PAT leaked event for %q (%s), want only %q", pub.AggregateID, f.Event, sysOwnedJobID)
			}
			readPATLifecycle = append(readPATLifecycle, f)
		}

		// 7. Close Alice's stream and verify RequestLogger recorded status 200 and bytes > 0 for /api/v1/events.
		aliceStream.close()
		bobStream.close()
		adminFullStream.close()
		adminReadStream.close()

		logDeadline := time.Now().Add(3 * time.Second)
		var sseLog map[string]any
		for time.Now().Before(logDeadline) {
			for line := range strings.SplitSeq(strings.TrimSpace(logBuf.String()), "\n") {
				var m map[string]any
				if err := json.Unmarshal([]byte(line), &m); err == nil &&
					m["msg"] == "http request" &&
					m["path"] == "/api/v1/events" &&
					m["status"] == float64(http.StatusOK) {
					sseLog = m
					break
				}
			}
			if sseLog != nil {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		if sseLog == nil {
			t.Fatalf("expected RequestLogger http request entry for /api/v1/events, got:\n%s", logBuf.String())
		}
		if st, ok := sseLog["status"].(float64); !ok || int(st) != http.StatusOK {
			t.Errorf("SSE RequestLogger status = %v, want 200", sseLog["status"])
		}
		if b, ok := sseLog["bytes"].(float64); !ok || int(b) <= 0 {
			t.Errorf("SSE RequestLogger bytes = %v, want > 0", sseLog["bytes"])
		}
		if strings.Contains(logBuf.String(), "alice-pass-123") || strings.Contains(logBuf.String(), adminFullPAT) {
			t.Fatal("RequestLogger leaked credentials")
		}

		return nil
	}

	if err := runDaemon(context.Background(), cfg, startServer); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("runDaemon failed: %v", err)
	}
}

func openSSEStreamWithCookie(t *testing.T, url string, cookie *http.Cookie) *sseClientStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("NewRequest(%s): %v", url, err)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		t.Fatalf("GET %s status = %d, want 200 (body: %s)", url, resp.StatusCode, string(body))
	}

	s := &sseClientStream{
		resp:   resp,
		cancel: cancel,
		frames: make(chan sseFrame, 64),
		errCh:  make(chan error, 1),
	}
	go func() {
		defer close(s.frames)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var cur sseFrame
		var dataLines []string
		hasFields := false
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if hasFields {
					cur.Data = strings.Join(dataLines, "\n")
					s.frames <- cur
					cur = sseFrame{}
					dataLines = nil
					hasFields = false
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue
			}
			field, val, hasColon := strings.Cut(line, ":")
			if hasColon {
				val = strings.TrimPrefix(val, " ")
			}
			switch field {
			case "id":
				cur.ID = val
				hasFields = true
			case "event":
				cur.Event = val
				hasFields = true
			case "data":
				dataLines = append(dataLines, val)
				hasFields = true
			}
		}
		if err := scanner.Err(); err != nil {
			s.errCh <- err
		}
	}()
	t.Cleanup(func() {
		s.close()
	})
	return s
}

func (s *sseClientStream) waitClosed(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-s.frames:
			if !ok {
				return
			}
			t.Fatalf("expected SSE stream to close without delivering post-revocation frame %+v", f)
		case <-deadline:
			t.Fatalf("timed out after %s waiting for revoked SSE stream to close", timeout)
		}
	}
}

func TestDaemonEventsSSE_OpenStreamRoleDowngradeRevocationAndGracefulShutdown(t *testing.T) {
	db := testdb.New(t)
	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=sysadmin",
		"TOBITEN_ADMIN_PASS=admin-secret-123",
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	originalRegistry := buildDaemonRegistry
	originalServer := startServer
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistry
		startServer = originalServer
		slog.SetDefault(originalLogger)
	})

	var mu sync.Mutex
	var sharedRunner *jobs.Runner
	slowStarted := make(chan struct{})
	allowSlowFinish := make(chan struct{})

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		jobRepo := postgres.NewJobRepository(db.Pool)

		probeMod := &testJobRegistrarModule{
			SimpleModule: &module.SimpleModule{
				ModuleName: "sse-live-auth-probe",
				Jobs: []module.JobKind{
					module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint, Workers: 1},
				},
				Mounts: []module.RouteMount{
					{
						ModuleName: "sse-live-auth-probe",
						Prefix:     "/api/v1/test-jobs",
						Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.Method == http.MethodGet && r.URL.Path == "/api/v1/test-jobs/slow" {
								close(slowStarted)
								select {
								case <-allowSlowFinish:
								case <-r.Context().Done():
									http.Error(w, "unexpected request cancellation", http.StatusRequestTimeout)
									return
								}
								w.WriteHeader(http.StatusOK)
								_, _ = w.Write([]byte("slow-ok"))
								return
							}
							if r.Method != http.MethodPost || r.URL.Path != "/api/v1/test-jobs/trigger" {
								http.NotFound(w, r)
								return
							}
							p := domain.MustPrincipalFromContext(r.Context())
							var body struct {
								Automatic bool `json:"automatic"`
							}
							if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
								middleware.WriteAppError(w, r, app.ErrInvalidInput)
								return
							}
							var createdBy *string
							if !body.Automatic {
								uid := p.User.ID
								createdBy = &uid
							}
							mu.Lock()
							notifier := sharedRunner
							mu.Unlock()
							middleware.HandleJobTrigger(w, r, func(ctx context.Context) (string, error) {
								return jobs.TriggerJob(ctx, jobRepo, notifier, jobs.TriggerRequest{
									Kind:      domain.JobKindScan,
									Payload:   json.RawMessage(`{"source":"live-auth-probe"}`),
									CreatedBy: createdBy,
								}, time.Now().UTC())
							})
						}),
					},
				},
			},
			register: func(r *jobs.Runner) error {
				mu.Lock()
				sharedRunner = r
				mu.Unlock()
				return r.RegisterKind(jobs.KindConfig{
					Kind:         domain.JobKindScan,
					ResumePolicy: domain.JobResumeCheckpoint,
					Handler: jobs.HandlerFunc(func(execCtx context.Context, exec *jobs.Execution) error {
						exec.ReportProgress(execCtx, 1, "/library/live.cbz")
						return nil
					}),
				})
			},
		}
		if err := reg.Register(probeMod); err != nil {
			t.Fatalf("register probeMod: %v", err)
		}
		return reg
	}

	daemonCtx, cancelDaemon := context.WithCancel(context.Background())
	defer cancelDaemon()

	idleSSEEOF := make(chan error, 1)
	slowResultCh := make(chan struct {
		status int
		body   string
		err    error
	}, 1)

	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		reg := buildDaemonRegistry(cfg, services)
		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   logger,
			Registry: reg,
		})
		if err != nil {
			return err
		}
		srvDone := make(chan error, 1)
		go func() {
			srvDone <- srv.Run(ctx)
		}()

		baseURL := srv.URL()
		userRepo := postgres.NewUserRepository(db.Pool)
		sessionRepo := postgres.NewSessionRepository(db.Pool)

		demotedAdmin := createTestUserWithPassword(ctx, t, userRepo, "demoted-sse-admin", "demoted-pass-123", domain.RoleAdminID)
		revokedUser := createTestUserWithPassword(ctx, t, userRepo, "revoked-sse-user", "revoked-pass-123", domain.RoleAdminID)
		stayAdmin := createTestUserWithPassword(ctx, t, userRepo, "stay-sse-admin", "stay-pass-123", domain.RoleAdminID)

		revokedPAT := createTestPAT(ctx, t, userRepo, revokedUser.ID, []domain.PATScope{domain.ScopeAdmin})
		revokedCreds, err := userRepo.ListCredentialsByUserID(ctx, revokedUser.ID)
		if err != nil {
			t.Fatalf("ListCredentialsByUserID: %v", err)
		}
		var revokedPATCredID string
		for _, c := range revokedCreds {
			if c.Kind == domain.CredentialKindPAT {
				revokedPATCredID = c.ID
				break
			}
		}
		if revokedPATCredID == "" {
			t.Fatal("failed to locate PAT credential ID")
		}

		// Session-backed SSE user whose session is revoked while stream stays open.
		sessUser := createTestUserWithPassword(ctx, t, userRepo, "sess-sse-user", "sess-pass-123", domain.RoleAdminID)
		_, sessCookie, _ := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"sess-sse-user","password":"sess-pass-123"}`, "198.51.100.88:5000", nil)

		demotedStream := openSSEStream(t, baseURL+"/api/v1/events", demotedAdmin.Username, "demoted-pass-123", "")
		revokedPATStream := openSSEStream(t, baseURL+"/api/v1/events", "", "", revokedPAT)
		revokedSessStream := openSSEStreamWithCookie(t, baseURL+"/api/v1/events", sessCookie)
		stayAdminStream := openSSEStream(t, baseURL+"/api/v1/events", stayAdmin.Username, "stay-pass-123", "")

		// 1. Downgrade demotedAdmin -> Reader, revoke revokedPAT, and revoke sessCookie's session while all streams are open.
		if err := userRepo.RemoveUserRole(ctx, demotedAdmin.ID, domain.RoleAdminID); err != nil {
			t.Fatalf("RemoveUserRole: %v", err)
		}
		if err := userRepo.AssignUserRole(ctx, demotedAdmin.ID, domain.RoleReaderID); err != nil {
			t.Fatalf("AssignUserRole: %v", err)
		}
		if err := userRepo.RevokeCredential(ctx, revokedPATCredID, time.Now().UTC()); err != nil {
			t.Fatalf("RevokeCredential(PAT): %v", err)
		}
		if _, err := db.Exec(ctx, `UPDATE sessions SET revoked_at = NOW() WHERE user_id = $1`, sessUser.ID); err != nil {
			t.Fatalf("revoke user session: %v", err)
		}
		_ = sessionRepo

		httpClient := &http.Client{Timeout: 5 * time.Second}
		trigger := func(user, pass string, automatic bool) string {
			body := `{"automatic":false}`
			if automatic {
				body = `{"automatic":true}`
			}
			req, reqErr := http.NewRequest(http.MethodPost, baseURL+"/api/v1/test-jobs/trigger", strings.NewReader(body))
			if reqErr != nil {
				t.Fatalf("NewRequest trigger: %v", reqErr)
			}
			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth(user, pass)
			resp, doErr := httpClient.Do(req)
			if doErr != nil {
				t.Fatalf("POST trigger: %v", doErr)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("trigger status = %d, want 202", resp.StatusCode)
			}
			var out middleware.JobAcceptedResponse
			if decErr := json.NewDecoder(resp.Body).Decode(&out); decErr != nil {
				t.Fatalf("decode trigger: %v", decErr)
			}
			return out.JobID
		}

		// 2. Trigger an automatic job and a revokedUser-owned job:
		//    - revokedPATStream and revokedSessStream must close without delivering any post-revocation events.
		//    - demotedStream must NOT receive the automatic job or revokedUser's job.
		//    - stayAdminStream (positive control) receives both.
		autoJobID := trigger(stayAdmin.Username, "stay-pass-123", true)
		revokedOwnJobID := trigger(revokedUser.Username, "revoked-pass-123", false)

		revokedPATStream.waitClosed(t, 4*time.Second)
		revokedSessStream.waitClosed(t, 4*time.Second)

		// Positive control: demotedAdmin triggers their own job and still receives it on demotedStream
		// as their very first lifecycle/progress job (proving autoJobID and revokedOwnJobID were filtered out).
		demotedOwnJobID := trigger(demotedAdmin.Username, "demoted-pass-123", false)

		var demotedSeen []string
		for len(demotedSeen) < 2 {
			f := demotedStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				var tick domain.JobProgressTick
				if err := json.Unmarshal([]byte(f.Data), &tick); err != nil {
					t.Fatalf("unmarshal demoted tick: %v", err)
				}
				if tick.JobID != demotedOwnJobID {
					t.Fatalf("demoted admin leaked progress tick for %q (want %q)", tick.JobID, demotedOwnJobID)
				}
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal demoted frame: %v", err)
			}
			if pub.AggregateID != demotedOwnJobID {
				t.Fatalf("demoted admin received unauthorized job %q (%s), want only %q", pub.AggregateID, f.Event, demotedOwnJobID)
			}
			demotedSeen = append(demotedSeen, f.Event)
		}

		staySeen := make(map[string]int)
		for staySeen[autoJobID] < 2 || staySeen[revokedOwnJobID] < 2 || staySeen[demotedOwnJobID] < 2 {
			f := stayAdminStream.nextFrame(t)
			if f.Event == domain.ProgressTickEventName {
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
				t.Fatalf("unmarshal stayAdmin frame: %v", err)
			}
			staySeen[pub.AggregateID]++
		}

		// Close active test streams before running live grant / secret-replacement checks.
		demotedStream.close()
		stayAdminStream.close()

		// 2.5 Same-ID Basic secret replacement + live visibility grant (including progress ticks).
		rotatedAdmin := createTestUserWithPassword(ctx, t, userRepo, "rotated-sse-admin", "rot-old-pass-123", domain.RoleAdminID)
		promotedReader := createTestUserWithPassword(ctx, t, userRepo, "promoted-sse-reader", "reader-pass-123", domain.RoleReaderID)

		rotatedOldStream := openSSEStream(t, baseURL+"/api/v1/events", rotatedAdmin.Username, "rot-old-pass-123", "")
		promotedReaderStream := openSSEStream(t, baseURL+"/api/v1/events", promotedReader.Username, "reader-pass-123", "")

		// Replace PASSWORD secret_hash in place on the exact same credential row.
		newRotatedHash, err := auth.HashPassword("rot-new-pass-456")
		if err != nil {
			t.Fatalf("HashPassword(new): %v", err)
		}
		if _, err := db.Exec(ctx, `UPDATE credentials SET secret_hash = $1 WHERE user_id = $2 AND kind = 'PASSWORD'`, newRotatedHash, rotatedAdmin.ID); err != nil {
			t.Fatalf("replace rotatedAdmin secret_hash: %v", err)
		}

		// Freshly authenticated Basic stream using the replacement secret must succeed.
		rotatedNewStream := openSSEStream(t, baseURL+"/api/v1/events", rotatedAdmin.Username, "rot-new-pass-456", "")

		// Grant jobs:read_all live by promoting promotedReader to Admin while its stream stays open.
		if err := userRepo.AssignUserRole(ctx, promotedReader.ID, domain.RoleAdminID); err != nil {
			t.Fatalf("AssignUserRole(Admin): %v", err)
		}

		// Trigger first automatic job to wake the stream dispatcher so rotatedOldStream terminates
		// and promotedReaderStream syncs its ProgressHub visibility principal to Admin.
		warmupAutoJobID := trigger(rotatedAdmin.Username, "rot-new-pass-456", true)
		rotatedOldStream.waitClosed(t, 4*time.Second)

		waitForJobLifecycle := func(name string, s *sseClientStream, targetJobID string) {
			for {
				f := s.nextFrame(t)
				if f.Event == domain.ProgressTickEventName {
					continue
				}
				var pub app.PublicEvent
				if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
					t.Fatalf("unmarshal %s frame: %v", name, err)
				}
				if pub.AggregateID == targetJobID && f.Event == string(domain.EventJobFinished) {
					return
				}
			}
		}
		waitForJobLifecycle("promotedReaderStream warmup", promotedReaderStream, warmupAutoJobID)
		waitForJobLifecycle("rotatedNewStream warmup", rotatedNewStream, warmupAutoJobID)

		// Trigger a second automatic job: promotedReaderStream and rotatedNewStream MUST receive
		// BOTH its lifecycle event and its live job.progress tick.
		grantedAutoJobID := trigger(rotatedAdmin.Username, "rot-new-pass-456", true)
		waitForLifecycleAndProgress := func(name string, s *sseClientStream, targetJobID string) {
			gotFinished := false
			gotProgress := false
			for !gotFinished || !gotProgress {
				f := s.nextFrame(t)
				if f.Event == domain.ProgressTickEventName {
					var tick domain.JobProgressTick
					if err := json.Unmarshal([]byte(f.Data), &tick); err != nil {
						t.Fatalf("unmarshal %s progress tick: %v", name, err)
					}
					if tick.JobID == targetJobID {
						gotProgress = true
					}
					continue
				}
				var pub app.PublicEvent
				if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
					t.Fatalf("unmarshal %s lifecycle frame: %v", name, err)
				}
				if pub.AggregateID == targetJobID && f.Event == string(domain.EventJobFinished) {
					gotFinished = true
				}
			}
		}
		waitForLifecycleAndProgress("promotedReaderStream", promotedReaderStream, grantedAutoJobID)
		waitForLifecycleAndProgress("rotatedNewStream", rotatedNewStream, grantedAutoJobID)
		promotedReaderStream.close()
		rotatedNewStream.close()

		// 3. Open a fresh raw http.Get SSE client that stays unclosed until daemon shutdown, alongside an in-flight /slow request!
		idleReq, err := http.NewRequest(http.MethodGet, baseURL+"/api/v1/events", nil)
		if err != nil {
			t.Fatalf("NewRequest idle SSE: %v", err)
		}
		idleReq.SetBasicAuth(stayAdmin.Username, "stay-pass-123")
		idleResp, err := (&http.Client{}).Do(idleReq)
		if err != nil {
			t.Fatalf("open idle SSE: %v", err)
		}
		defer idleResp.Body.Close()
		if idleResp.StatusCode != http.StatusOK {
			t.Fatalf("idle SSE status = %d, want 200", idleResp.StatusCode)
		}
		go func() {
			_, rErr := io.ReadAll(idleResp.Body)
			idleSSEEOF <- rErr
		}()

		go func() {
			req, reqErr := http.NewRequest(http.MethodGet, baseURL+"/api/v1/test-jobs/slow", nil)
			if reqErr != nil {
				slowResultCh <- struct {
					status int
					body   string
					err    error
				}{err: reqErr}
				return
			}
			req.SetBasicAuth(stayAdmin.Username, "stay-pass-123")
			resp, doErr := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if doErr != nil {
				slowResultCh <- struct {
					status int
					body   string
					err    error
				}{err: doErr}
				return
			}
			defer resp.Body.Close()
			b, rErr := io.ReadAll(resp.Body)
			slowResultCh <- struct {
				status int
				body   string
				err    error
			}{status: resp.StatusCode, body: string(b), err: rErr}
		}()

		select {
		case <-slowStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for /slow to start")
		}

		// Trigger daemon shutdown while both idleResp and stayAdminStream remain open on the client side!
		cancelDaemon()

		// Idle SSE client must receive clean EOF promptly when serving stops.
		select {
		case rErr := <-idleSSEEOF:
			if rErr != nil {
				t.Fatalf("idle SSE ReadAll err = %v, want clean EOF", rErr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for idle SSE client to close during daemon shutdown")
		}

		// Ordinary in-flight /slow request must still be alive until allowed to finish.
		select {
		case res := <-slowResultCh:
			t.Fatalf("/slow finished prematurely on shutdown: %+v", res)
		case srvErr := <-srvDone:
			t.Fatalf("srv.Run returned before in-flight /slow finished: %v", srvErr)
		case <-time.After(50 * time.Millisecond):
		}

		close(allowSlowFinish)
		select {
		case res := <-slowResultCh:
			if res.err != nil || res.status != http.StatusOK || res.body != "slow-ok" {
				t.Fatalf("/slow result = %+v, want 200 slow-ok", res)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for /slow completion")
		}

		return <-srvDone
	}

	if err := runDaemon(daemonCtx, cfg, startServer); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("runDaemon graceful shutdown failed: %v", err)
	}
}
