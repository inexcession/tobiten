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
