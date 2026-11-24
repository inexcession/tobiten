// Package curation implements content curation use-cases including bounded
// in-process cover generation, cover selection, page suppression, and trash
// management (ADR-0010, ADR-0015, ADR-0017).
package curation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/image"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
)

const (
	// DefaultCoverWorkers is the default bounded worker count for in-process
	// cover generation (ADR-0017 §6).
	DefaultCoverWorkers = 2

	// DefaultCoverWaitTimeout is the default caller wait timeout for on-demand
	// cover generation (ADR-0015 §4, M1B-D8).
	DefaultCoverWaitTimeout = 5 * time.Second
)

// Sentinel errors returned by CoverService operations.
var (
	// ErrCoverWaitTimeout means a caller waited past the configured wait timeout
	// while shared cover generation continues in the background.
	ErrCoverWaitTimeout = errors.New("curation: cover generation wait timed out")
	// ErrCoverUnavailable means the target asset or its cover source cannot be served.
	ErrCoverUnavailable = errors.New("curation: cover source unavailable")
	// ErrCoverPoolClosed means the cover generation worker pool has been stopped.
	ErrCoverPoolClosed = errors.New("curation: cover generation pool is closed")
	// ErrCoverPoolNotRunning means the cover generation worker pool has not been started.
	ErrCoverPoolNotRunning = errors.New("curation: cover generation pool is not running")
	// ErrCoverPoolAlreadyRunning means Start was called on an active CoverService.
	ErrCoverPoolAlreadyRunning = errors.New("curation: cover generation pool is already running")
	// ErrInvalidCoverWorkers means CoverServiceOptions.Workers is negative.
	ErrInvalidCoverWorkers = errors.New("curation: cover generation worker count must be positive")
	// ErrInvalidCoverWaitTimeout means the configured wait timeout is negative.
	ErrInvalidCoverWaitTimeout = errors.New("curation: cover generation wait timeout must be positive")
	// ErrMissingCoverCache means CoverServiceOptions.Cache is nil.
	ErrMissingCoverCache = errors.New("curation: cover cache is required")
	// ErrMissingImageRegistry means CoverServiceOptions.Pipelines is nil.
	ErrMissingImageRegistry = errors.New("curation: image pipeline registry is required")
	// ErrMissingTransactor means CoverServiceOptions.Tx is nil when querying assets.
	ErrMissingTransactor = errors.New("curation: storage transactor is required")
)

// CoverWaitTimeoutError reports that a caller waited past WaitTimeout while
// shared in-process cover generation continues in the background (ADR-0015 §4, M1B-D8).
type CoverWaitTimeoutError struct {
	Key     image.CoverKey
	Timeout time.Duration
}

func (e *CoverWaitTimeoutError) Error() string {
	return fmt.Sprintf("%s after %s", ErrCoverWaitTimeout, e.Timeout)
}

// Unwrap returns ErrCoverWaitTimeout for errors.Is matching.
func (e *CoverWaitTimeoutError) Unwrap() error {
	return ErrCoverWaitTimeout
}

// IsTimeout reports that this error represents a wait timeout.
func (e *CoverWaitTimeoutError) IsTimeout() bool {
	return true
}

// CoverUnavailableError reports that an asset or its cover source is
// unavailable (missing, retired, OFFLINE root, absent file, digest mismatch,
// or no displayable page).
type CoverUnavailableError struct {
	AssetID string
	Reason  string
	Err     error
}

func (e *CoverUnavailableError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s (asset=%s, reason=%s): %v", ErrCoverUnavailable, e.AssetID, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s (asset=%s, reason=%s)", ErrCoverUnavailable, e.AssetID, e.Reason)
}

// Unwrap exposes ErrCoverUnavailable and any underlying filesystem cause.
func (e *CoverUnavailableError) Unwrap() []error {
	if e.Err != nil {
		return []error{ErrCoverUnavailable, e.Err}
	}
	return []error{ErrCoverUnavailable}
}

// ReadAtCloser is the read-only random-access handle used to open archives.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type statReadAtCloser interface {
	Stat() (os.FileInfo, error)
}

// CoverAssetSource describes a media asset and its library root needed to look
// up or generate a cover rendition without holding a database transaction open.
type CoverAssetSource struct {
	AssetID         string
	LibraryRootID   string
	RootPathBytes   []byte
	RootState       domain.LibraryRootState
	RelPathBytes    []byte
	FileSize        int64
	Format          domain.MediaFormat
	DigestKind      domain.DigestKind
	Digest          string
	PageCount       int
	CoverPageIndex  int
	SuppressedPages []int
	MissingSince    *time.Time
	RetiredAt       *time.Time
}

// CoverAssetSourceFromDomain projects a domain MediaAsset and its LibraryRoot
// into a CoverAssetSource, cloning byte and page slices.
func CoverAssetSourceFromDomain(asset domain.MediaAsset, root domain.LibraryRoot) CoverAssetSource {
	var (
		digestKind domain.DigestKind
		digestVal  string
	)
	if cur, ok := asset.CurrentDigest(domain.DigestKindXXH364Partial); ok {
		digestKind = cur.Kind
		digestVal = cur.Value
	}
	return CoverAssetSource{
		AssetID:         asset.ID,
		LibraryRootID:   asset.LibraryRootID,
		RootPathBytes:   slices.Clone(root.Path.Bytes),
		RootState:       root.State,
		RelPathBytes:    slices.Clone(asset.RelPathBytes),
		FileSize:        asset.FileSize,
		Format:          asset.Format,
		DigestKind:      digestKind,
		Digest:          digestVal,
		PageCount:       asset.PageCount,
		CoverPageIndex:  asset.CoverPageIndex,
		SuppressedPages: slices.Clone(asset.SuppressedPages),
		MissingSince:    asset.MissingSince,
		RetiredAt:       asset.RetiredAt,
	}
}

// CoverServiceOptions configures a CoverService and its daemon-owned bounded
// cover generation worker pool (ADR-0015 §4, ADR-0017 §6, M1B-D8).
type CoverServiceOptions struct {
	Tx               storage.Transactor
	Cache            *image.CoverCache
	Pipelines        *image.Registry
	Formats          *container.FormatRegistry
	Workers          int
	WaitTimeout      time.Duration
	DefaultRendition image.RenderOptions
	Logger           *slog.Logger
	OpenFile         func(ctx context.Context, rawPath []byte) (ReadAtCloser, error)
}

// CoverRequestOptions configures an individual cover lookup/generation call.
type CoverRequestOptions struct {
	Rendition   *image.RenderOptions
	WaitTimeout time.Duration
}

type coverTask struct {
	key        image.CoverKey
	done       chan struct{}
	entry      image.CoverEntry
	err        error
	generateFn func(ctx context.Context) (image.CoverEntry, error)
}

type archiveCoverSpec struct {
	key           image.CoverKey
	assetID       string
	joinedPath    []byte
	fileSize      int64
	pageIdx       int
	suppressedSet map[int]bool
	rendition     image.RenderOptions
}

// CoverService serves cached cover renditions and runs cache-miss generation
// on a daemon-owned bounded in-process worker pool deduplicated by full CoverKey
// (ADR-0015 §4, ADR-0017 §6, M1B-D8).
type CoverService struct {
	tx               storage.Transactor
	cache            *image.CoverCache
	pipelines        *image.Registry
	formats          *container.FormatRegistry
	workers          int
	waitTimeout      time.Duration
	defaultRendition image.RenderOptions
	logger           *slog.Logger
	openFile         func(ctx context.Context, rawPath []byte) (ReadAtCloser, error)

	mu        sync.Mutex
	cond      *sync.Cond
	running   bool
	closed    bool
	genCtx    context.Context
	genCancel context.CancelFunc
	stopAfter func() bool
	inflight  map[image.CoverKey]*coverTask
	queue     []*coverTask
	wg        sync.WaitGroup
}

// NewCoverService constructs a CoverService with the provided dependencies and options.
func NewCoverService(opts CoverServiceOptions) (*CoverService, error) {
	if opts.Cache == nil {
		return nil, ErrMissingCoverCache
	}
	if opts.Pipelines == nil {
		return nil, ErrMissingImageRegistry
	}
	if opts.Workers < 0 {
		return nil, ErrInvalidCoverWorkers
	}
	if opts.WaitTimeout < 0 {
		return nil, ErrInvalidCoverWaitTimeout
	}

	workers := opts.Workers
	if workers == 0 {
		workers = DefaultCoverWorkers
	}
	waitTimeout := opts.WaitTimeout
	if waitTimeout == 0 {
		waitTimeout = DefaultCoverWaitTimeout
	}

	rendition, err := normalizeDefaultRendition(opts.DefaultRendition)
	if err != nil {
		return nil, err
	}

	formats := opts.Formats
	if formats == nil {
		formats = container.NewFormatRegistry()
		if err := formats.Register(cbz.Format()); err != nil {
			return nil, fmt.Errorf("curation: register cbz format: %w", err)
		}
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	openFn := opts.OpenFile
	if openFn == nil {
		openFn = defaultOpenRawArchive
	}

	svc := &CoverService{
		tx:               opts.Tx,
		cache:            opts.Cache,
		pipelines:        opts.Pipelines,
		formats:          formats,
		workers:          workers,
		waitTimeout:      waitTimeout,
		defaultRendition: rendition,
		logger:           logger,
		openFile:         openFn,
		inflight:         make(map[image.CoverKey]*coverTask),
	}
	svc.cond = sync.NewCond(&svc.mu)
	return svc, nil
}

func normalizeDefaultRendition(raw image.RenderOptions) (image.RenderOptions, error) {
	if raw == (image.RenderOptions{}) {
		return image.DefaultCoverOptions(), nil
	}
	return raw.Validate()
}

func defaultOpenRawArchive(ctx context.Context, rawPath []byte) (ReadAtCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(rawPath) == 0 || bytes.IndexByte(rawPath, 0) >= 0 {
		return nil, paths.ErrInvalidRawPath
	}
	return os.OpenFile(string(rawPath), os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

// Workers returns the configured bounded worker count.
func (s *CoverService) Workers() int {
	return s.workers
}

// WaitTimeout returns the configured default caller wait timeout.
func (s *CoverService) WaitTimeout() time.Duration {
	return s.waitTimeout
}

// Start launches the bounded cover generation worker pool bound to the daemon
// lifecycle context ctx (ADR-0017 §6).
func (s *CoverService) Start(ctx context.Context) error {
	if s == nil {
		return ErrCoverPoolNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrCoverPoolAlreadyRunning
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	genCtx, genCancel := context.WithCancel(ctx)
	s.genCtx = genCtx
	s.genCancel = genCancel
	s.closed = false
	s.running = true
	s.inflight = make(map[image.CoverKey]*coverTask)
	s.queue = nil
	s.stopAfter = context.AfterFunc(genCtx, func() {
		s.mu.Lock()
		s.closeQueueLocked()
		s.mu.Unlock()
	})

	for range s.workers {
		s.wg.Add(1)
		go s.runWorker(genCtx)
	}
	return nil
}

// Stop cancels pending cover generation work and waits for all active workers to exit.
func (s *CoverService) Stop() {
	if s == nil {
		return
	}

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel := s.genCancel
	stopAfter := s.stopAfter
	s.closeQueueLocked()
	s.mu.Unlock()

	if stopAfter != nil {
		stopAfter()
	}
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()

	s.mu.Lock()
	s.running = false
	s.genCancel = nil
	s.stopAfter = nil
	s.mu.Unlock()
}

func (s *CoverService) closeQueueLocked() {
	s.closed = true
	for _, task := range s.queue {
		delete(s.inflight, task.key)
		task.err = ErrCoverPoolClosed
		close(task.done)
	}
	s.queue = nil
	s.cond.Broadcast()
}

// GetOrGenerateAssetCover loads assetID and its library root from PostgreSQL,
// checks availability, and returns the cached or newly generated cover rendition.
func (s *CoverService) GetOrGenerateAssetCover(
	ctx context.Context,
	assetID string,
	opts ...CoverRequestOptions,
) (image.CoverEntry, error) {
	if s == nil {
		return image.CoverEntry{}, ErrCoverPoolNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return image.CoverEntry{}, err
	}
	if strings.TrimSpace(assetID) == "" {
		return image.CoverEntry{}, fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyAssetID)
	}
	if err := s.ensurePoolOpen(); err != nil {
		return image.CoverEntry{}, err
	}

	src, err := s.loadAssetSource(ctx, assetID)
	if err != nil {
		return image.CoverEntry{}, err
	}
	return s.GetOrGenerateCover(ctx, src, opts...)
}

func (s *CoverService) loadAssetSource(ctx context.Context, assetID string) (CoverAssetSource, error) {
	if s.tx == nil {
		return CoverAssetSource{}, ErrMissingTransactor
	}
	var src CoverAssetSource
	err := s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		libRepo := tx.Libraries()
		if cw == nil || libRepo == nil {
			return errors.New("curation: catalogue write and library repositories are required")
		}
		asset, err := cw.GetAssetByID(ctx, assetID)
		if err != nil {
			return err
		}
		root, err := libRepo.GetLibraryRootByID(ctx, asset.LibraryRootID)
		if err != nil {
			return err
		}
		src = CoverAssetSourceFromDomain(*asset, *root)
		return nil
	})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return CoverAssetSource{}, fmt.Errorf("%w: asset %q", app.ErrNotFound, assetID)
		}
		return CoverAssetSource{}, err
	}
	return src, nil
}

// GetOrGenerateCover validates src availability, checks CoverCache, and on a
// miss awaits shared bounded-pool generation by full CoverKey within the
// configured wait timeout.
func (s *CoverService) GetOrGenerateCover(
	ctx context.Context,
	src CoverAssetSource,
	opts ...CoverRequestOptions,
) (image.CoverEntry, error) {
	if s == nil {
		return image.CoverEntry{}, ErrCoverPoolNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return image.CoverEntry{}, err
	}
	if err := s.ensurePoolOpen(); err != nil {
		return image.CoverEntry{}, err
	}

	rendition, waitTimeout, err := s.resolveRequestOptions(opts)
	if err != nil {
		return image.CoverEntry{}, err
	}

	spec, err := prepareArchiveCoverSpec(src, rendition)
	if err != nil {
		return image.CoverEntry{}, err
	}

	cached, err := s.cache.Get(ctx, spec.key)
	if err == nil {
		return cached, nil
	}
	if !errors.Is(err, image.ErrCacheMiss) {
		return image.CoverEntry{}, err
	}

	task, err := s.joinOrEnqueue(spec.key, func(genCtx context.Context) (image.CoverEntry, error) {
		return s.generateArchiveCover(genCtx, spec)
	})
	if err != nil {
		return image.CoverEntry{}, err
	}

	return waitForTask(ctx, spec.key, task, waitTimeout)
}

func (s *CoverService) ensurePoolOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrCoverPoolClosed
	}
	if !s.running {
		return ErrCoverPoolNotRunning
	}
	return nil
}

func (s *CoverService) resolveRequestOptions(opts []CoverRequestOptions) (image.RenderOptions, time.Duration, error) {
	rendition := s.defaultRendition
	waitTimeout := s.waitTimeout
	if len(opts) == 0 {
		return rendition, waitTimeout, nil
	}
	req := opts[0]
	if req.WaitTimeout < 0 {
		return image.RenderOptions{}, 0, ErrInvalidCoverWaitTimeout
	}
	if req.WaitTimeout > 0 {
		waitTimeout = req.WaitTimeout
	}
	if req.Rendition != nil {
		validated, err := req.Rendition.Validate()
		if err != nil {
			return image.RenderOptions{}, 0, err
		}
		rendition = validated
	}
	return rendition, waitTimeout, nil
}

func prepareArchiveCoverSpec(src CoverAssetSource, rendition image.RenderOptions) (archiveCoverSpec, error) {
	if src.RetiredAt != nil {
		return archiveCoverSpec{}, &CoverUnavailableError{AssetID: src.AssetID, Reason: "retired"}
	}
	if src.MissingSince != nil {
		return archiveCoverSpec{}, &CoverUnavailableError{AssetID: src.AssetID, Reason: "missing"}
	}
	if src.RootState != domain.LibraryRootStateOnline {
		return archiveCoverSpec{}, &CoverUnavailableError{AssetID: src.AssetID, Reason: "root_offline"}
	}
	if src.DigestKind != domain.DigestKindXXH364Partial || strings.TrimSpace(src.Digest) == "" {
		return archiveCoverSpec{}, &CoverUnavailableError{AssetID: src.AssetID, Reason: "missing_digest"}
	}

	pageIdx, suppressedSet, err := resolveCoverPageIndex(src.PageCount, src.CoverPageIndex, src.SuppressedPages)
	if err != nil {
		return archiveCoverSpec{}, &CoverUnavailableError{
			AssetID: src.AssetID,
			Reason:  "no_displayable_page",
			Err:     err,
		}
	}

	joined, err := paths.JoinRootRelative(src.RootPathBytes, src.RelPathBytes)
	if err != nil {
		return archiveCoverSpec{}, fmt.Errorf("%w: %v", app.ErrInvalidInput, err)
	}

	normKey, err := image.ArchiveCoverKey(src.Digest, pageIdx, rendition).Normalize()
	if err != nil {
		return archiveCoverSpec{}, err
	}

	return archiveCoverSpec{
		key:           normKey,
		assetID:       src.AssetID,
		joinedPath:    joined,
		fileSize:      src.FileSize,
		pageIdx:       pageIdx,
		suppressedSet: suppressedSet,
		rendition:     rendition,
	}, nil
}

func resolveCoverPageIndex(pageCount, recordedCoverIndex int, suppressedPages []int) (int, map[int]bool, error) {
	if pageCount <= 0 {
		return 0, nil, errors.New("asset has no image pages")
	}

	var suppressedSet map[int]bool
	if len(suppressedPages) > 0 {
		suppressedSet = make(map[int]bool, len(suppressedPages))
		for _, idx := range suppressedPages {
			if idx >= 0 && idx < pageCount {
				suppressedSet[idx] = true
			}
		}
	}

	if recordedCoverIndex >= 0 && recordedCoverIndex < pageCount && !suppressedSet[recordedCoverIndex] {
		return recordedCoverIndex, suppressedSet, nil
	}

	for i := range pageCount {
		if !suppressedSet[i] {
			return i, suppressedSet, nil
		}
	}

	return 0, suppressedSet, errors.New("all pages are suppressed")
}

func (s *CoverService) joinOrEnqueue(
	key image.CoverKey,
	generateFn func(ctx context.Context) (image.CoverEntry, error),
) (*coverTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrCoverPoolClosed
	}
	if !s.running {
		return nil, ErrCoverPoolNotRunning
	}

	if existing, ok := s.inflight[key]; ok {
		return existing, nil
	}

	task := &coverTask{
		key:        key,
		done:       make(chan struct{}),
		generateFn: generateFn,
	}
	s.inflight[key] = task
	s.queue = append(s.queue, task)
	s.cond.Signal()
	return task, nil
}

func waitForTask(
	ctx context.Context,
	key image.CoverKey,
	task *coverTask,
	waitTimeout time.Duration,
) (image.CoverEntry, error) {
	select {
	case <-task.done:
		return task.entry, task.err
	default:
	}

	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	select {
	case <-task.done:
		return task.entry, task.err
	case <-timer.C:
		select {
		case <-task.done:
			return task.entry, task.err
		default:
		}
		return image.CoverEntry{}, &CoverWaitTimeoutError{
			Key:     key,
			Timeout: waitTimeout,
		}
	case <-ctx.Done():
		select {
		case <-task.done:
			return task.entry, task.err
		default:
		}
		return image.CoverEntry{}, ctx.Err()
	}
}

func (s *CoverService) runWorker(genCtx context.Context) {
	defer s.wg.Done()

	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed && genCtx.Err() == nil {
			s.cond.Wait()
		}
		if s.closed || genCtx.Err() != nil {
			s.mu.Unlock()
			return
		}
		task := s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
		if len(s.queue) == 0 {
			s.queue = nil
		}
		s.mu.Unlock()

		entry, err := s.executeTask(genCtx, task)

		s.mu.Lock()
		task.entry = entry
		task.err = err
		delete(s.inflight, task.key)
		close(task.done)
		s.mu.Unlock()
	}
}

func (s *CoverService) executeTask(genCtx context.Context, task *coverTask) (image.CoverEntry, error) {
	if err := genCtx.Err(); err != nil {
		return image.CoverEntry{}, err
	}
	cached, err := s.cache.Get(genCtx, task.key)
	if err == nil {
		return cached, nil
	}
	if !errors.Is(err, image.ErrCacheMiss) {
		return image.CoverEntry{}, err
	}
	return task.generateFn(genCtx)
}

func (s *CoverService) generateArchiveCover(genCtx context.Context, spec archiveCoverSpec) (image.CoverEntry, error) {
	f, err := s.openFile(genCtx, spec.joinedPath)
	if err != nil {
		if genCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return image.CoverEntry{}, err
		}
		if errors.Is(err, os.ErrNotExist) {
			return image.CoverEntry{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "file_missing", Err: err}
		}
		return image.CoverEntry{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "file_unreadable", Err: err}
	}
	defer func() {
		_ = f.Close()
	}()

	size, err := verifyOpenedArchiveFingerprint(genCtx, f, spec)
	if err != nil {
		return image.CoverEntry{}, err
	}

	cr, err := s.formats.Open(f, size)
	if err != nil {
		return image.CoverEntry{}, fmt.Errorf("curation: open archive %s: %w", spec.assetID, err)
	}
	defer func() {
		_ = cr.Close()
	}()

	pages := cr.Pages()
	if spec.pageIdx < 0 || spec.pageIdx >= len(pages) || spec.suppressedSet[spec.pageIdx] {
		return image.CoverEntry{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "no_displayable_page"}
	}

	pageEntry := pages[spec.pageIdx]
	rc, err := cr.Open(pageEntry.Index)
	if err != nil {
		return image.CoverEntry{}, fmt.Errorf("curation: open cover page %d of %s: %w", spec.pageIdx, spec.assetID, err)
	}
	defer func() {
		_ = rc.Close()
	}()

	renderOpts := spec.rendition
	if codec := image.NormalizeCodec(container.PageCodec(pageEntry.NameBytes)); codec != "" {
		renderOpts.SourceCodec = codec
	}

	rendered, err := s.pipelines.RenderCover(genCtx, rc, renderOpts)
	if err != nil {
		return image.CoverEntry{}, fmt.Errorf("curation: render cover page %d of %s: %w", spec.pageIdx, spec.assetID, err)
	}

	if _, err := verifyOpenedArchiveFingerprint(genCtx, f, spec); err != nil {
		return image.CoverEntry{}, err
	}

	entry, err := s.cache.Put(genCtx, spec.key, rendered.Data)
	if err != nil {
		return image.CoverEntry{}, fmt.Errorf("curation: cache cover %s: %w", spec.assetID, err)
	}
	return entry, nil
}

func verifyOpenedArchiveFingerprint(ctx context.Context, f ReadAtCloser, spec archiveCoverSpec) (int64, error) {
	size := spec.fileSize
	if statable, ok := f.(statReadAtCloser); ok {
		info, err := statable.Stat()
		if err != nil {
			return 0, &CoverUnavailableError{AssetID: spec.assetID, Reason: "file_unreadable", Err: err}
		}
		if !info.Mode().IsRegular() {
			return 0, &CoverUnavailableError{AssetID: spec.assetID, Reason: "non_regular_file"}
		}
		size = info.Size()
	}

	digest, err := scanner.PartialFingerprint(f, size)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, &CoverUnavailableError{AssetID: spec.assetID, Reason: "file_unreadable", Err: err}
	}
	if digest != spec.key.Digest {
		return 0, &CoverUnavailableError{
			AssetID: spec.assetID,
			Reason:  "digest_mismatch",
			Err:     fmt.Errorf("archive fingerprint %s differs from expected %s", digest, spec.key.Digest),
		}
	}
	return size, nil
}
