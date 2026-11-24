// Package curation implements content curation use-cases including bounded
// in-process cover generation, cover selection, page suppression, and trash
// management (ADR-0010, ADR-0015, ADR-0017).
package curation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
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
// concurrent source modification, or no displayable page).
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
	ReadTx           storage.ReadTransactor
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

var _ app.CurationService = (*CoverService)(nil)

// CoverService serves cached cover renditions and runs cache-miss generation
// on a daemon-owned bounded in-process worker pool deduplicated by full CoverKey
// (ADR-0015 §4, ADR-0017 §6, M1B-D8).
type CoverService struct {
	tx               storage.Transactor
	readTx           storage.ReadTransactor
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

	readTx := opts.ReadTx
	if readTx == nil {
		readTx, _ = opts.Tx.(storage.ReadTransactor)
	}

	svc := &CoverService{
		tx:               opts.Tx,
		readTx:           readTx,
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

	obs0, err := verifyOpenedArchive(genCtx, f, spec, nil)
	if err != nil {
		return image.CoverEntry{}, err
	}

	cr, err := s.formats.Open(f, obs0.size)
	if err != nil {
		if chErr := checkOpenedArchiveUnchanged(f, spec, obs0); chErr != nil {
			return image.CoverEntry{}, chErr
		}
		return image.CoverEntry{}, classifySourceCoverError(spec.assetID, fmt.Errorf("curation: open archive %s: %w", spec.assetID, err))
	}
	defer func() {
		_ = cr.Close()
	}()

	pages := cr.Pages()
	if spec.pageIdx < 0 || spec.pageIdx >= len(pages) || spec.suppressedSet[spec.pageIdx] {
		if chErr := checkOpenedArchiveUnchanged(f, spec, obs0); chErr != nil {
			return image.CoverEntry{}, chErr
		}
		return image.CoverEntry{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "no_displayable_page"}
	}

	pageEntry := pages[spec.pageIdx]
	rc, err := cr.Open(pageEntry.Index)
	if err != nil {
		if chErr := checkOpenedArchiveUnchanged(f, spec, obs0); chErr != nil {
			return image.CoverEntry{}, chErr
		}
		return image.CoverEntry{}, classifySourceCoverError(spec.assetID, fmt.Errorf("curation: open cover page %d of %s: %w", spec.pageIdx, spec.assetID, err))
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
		if genCtx.Err() == nil {
			if chErr := checkOpenedArchiveUnchanged(f, spec, obs0); chErr != nil {
				return image.CoverEntry{}, chErr
			}
		}
		return image.CoverEntry{}, classifySourceCoverError(spec.assetID, fmt.Errorf("curation: render cover page %d of %s: %w", spec.pageIdx, spec.assetID, err))
	}

	if _, err := verifyOpenedArchive(genCtx, f, spec, &obs0); err != nil {
		return image.CoverEntry{}, err
	}

	entry, err := s.cache.Put(genCtx, spec.key, rendered.Data)
	if err != nil {
		return image.CoverEntry{}, fmt.Errorf("curation: cache cover %s: %w", spec.assetID, err)
	}
	return entry, nil
}

type archiveObservation struct {
	hasStat bool
	size    int64
	modTime time.Time
}

func observeOpenedArchive(f ReadAtCloser, spec archiveCoverSpec) (archiveObservation, error) {
	statable, ok := f.(statReadAtCloser)
	if !ok {
		return archiveObservation{size: spec.fileSize}, nil
	}
	info, err := statable.Stat()
	if err != nil {
		return archiveObservation{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "file_unreadable", Err: err}
	}
	if !info.Mode().IsRegular() {
		return archiveObservation{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "non_regular_file"}
	}
	return archiveObservation{
		hasStat: true,
		size:    info.Size(),
		modTime: info.ModTime(),
	}, nil
}

func checkObservationUnchanged(assetID string, before, after archiveObservation) error {
	if !before.hasStat || !after.hasStat {
		return nil
	}
	if after.size != before.size || !after.modTime.Equal(before.modTime) {
		return &CoverUnavailableError{
			AssetID: assetID,
			Reason:  "source_changed",
			Err: fmt.Errorf("opened archive changed during cover generation (size %d->%d, mtime %s->%s)",
				before.size, after.size, before.modTime.UTC().Format(time.RFC3339Nano), after.modTime.UTC().Format(time.RFC3339Nano)),
		}
	}
	return nil
}

func checkOpenedArchiveUnchanged(f ReadAtCloser, spec archiveCoverSpec, baseline archiveObservation) error {
	cur, err := observeOpenedArchive(f, spec)
	if err != nil {
		return err
	}
	return checkObservationUnchanged(spec.assetID, baseline, cur)
}

func verifyOpenedArchive(
	ctx context.Context,
	f ReadAtCloser,
	spec archiveCoverSpec,
	baseline *archiveObservation,
) (archiveObservation, error) {
	obs, err := observeOpenedArchive(f, spec)
	if err != nil {
		return archiveObservation{}, err
	}

	digest, err := scanner.PartialFingerprint(f, obs.size)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return archiveObservation{}, ctxErr
		}
		return archiveObservation{}, &CoverUnavailableError{AssetID: spec.assetID, Reason: "file_unreadable", Err: err}
	}
	if digest != spec.key.Digest {
		return archiveObservation{}, &CoverUnavailableError{
			AssetID: spec.assetID,
			Reason:  "digest_mismatch",
			Err:     fmt.Errorf("archive fingerprint %s differs from expected %s", digest, spec.key.Digest),
		}
	}

	expected := obs
	if baseline != nil {
		if err := checkObservationUnchanged(spec.assetID, *baseline, obs); err != nil {
			return archiveObservation{}, err
		}
		expected = *baseline
	}

	afterFP, err := observeOpenedArchive(f, spec)
	if err != nil {
		return archiveObservation{}, err
	}
	if err := checkObservationUnchanged(spec.assetID, expected, afterFP); err != nil {
		return archiveObservation{}, err
	}
	return expected, nil
}

const seriesCoverBatchSize = 8

func classifySourceCoverError(assetID string, err error) error {
	switch {
	case errors.Is(err, container.ErrCorrupt),
		errors.Is(err, container.ErrUnknownFormat),
		errors.Is(err, container.ErrEncrypted),
		errors.Is(err, container.ErrUnsupported),
		errors.Is(err, container.ErrEntryNotFound):
		return &CoverUnavailableError{AssetID: assetID, Reason: "corrupt_or_unsupported_archive", Err: err}
	case errors.Is(err, image.ErrCorrupt),
		errors.Is(err, image.ErrUnsupportedFormat),
		errors.Is(err, image.ErrPixelLimitExceeded),
		errors.Is(err, image.ErrPreflightLimitExceeded):
		return &CoverUnavailableError{AssetID: assetID, Reason: "corrupt_or_unsupported_image", Err: err}
	default:
		return err
	}
}

func isCandidateUnavailableError(err error) bool {
	return errors.Is(err, ErrCoverUnavailable) ||
		errors.Is(err, container.ErrCorrupt) ||
		errors.Is(err, container.ErrUnknownFormat) ||
		errors.Is(err, container.ErrEncrypted) ||
		errors.Is(err, container.ErrUnsupported) ||
		errors.Is(err, container.ErrEntryNotFound) ||
		errors.Is(err, image.ErrCorrupt) ||
		errors.Is(err, image.ErrUnsupportedFormat) ||
		errors.Is(err, image.ErrPixelLimitExceeded) ||
		errors.Is(err, image.ErrPreflightLimitExceeded)
}

// GetCover resolves targetID under principal's SERVER-label policies and serves
// its rendered JPEG cover and strong ETag through the daemon-owned cover pool
// (ADR-0006 §5, ADR-0010 §2, ADR-0015 §4).
func (s *CoverService) GetCover(ctx context.Context, principal *domain.Principal, targetID string) (*app.CoverRendition, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: %w", app.ErrServiceBusy, ErrCoverPoolNotRunning)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if principal == nil || strings.TrimSpace(principal.User.ID) == "" {
		return nil, app.ErrUnauthorized
	}
	if strings.TrimSpace(targetID) == "" {
		return nil, fmt.Errorf("%w: target id is required", app.ErrInvalidInput)
	}
	targetKind, hasTargetKind := app.CoverTargetKindFromContext(ctx)
	if hasTargetKind && targetKind != domain.ItemTypeVolume && targetKind != domain.ItemTypeSeries {
		return nil, fmt.Errorf("%w: unsupported cover target kind %q", app.ErrInvalidInput, targetKind)
	}
	if s.readTx == nil {
		return nil, ErrMissingTransactor
	}

	scope := storage.CatalogueReadScope{
		Principal:      principal,
		IncludeTrashed: false,
	}

	if !hasTargetKind || targetKind == domain.ItemTypeVolume {
		rendition, handled, err := s.getVolumeCover(ctx, scope, targetID)
		if handled {
			return rendition, err
		}
		if hasTargetKind && targetKind == domain.ItemTypeVolume {
			return nil, app.ErrNotFound
		}
	}

	return s.getSeriesCover(ctx, scope, targetID)
}

func (s *CoverService) getVolumeCover(
	ctx context.Context,
	scope storage.CatalogueReadScope,
	volumeID string,
) (*app.CoverRendition, bool, error) {
	var files []storage.AuthorizedAssetFile
	err := s.readTx.WithRepeatableReadTx(ctx, func(tx storage.ReadTx) error {
		catRepo := tx.Catalogue()
		if catRepo == nil {
			return errors.New("curation: read transaction missing catalogue repository")
		}
		var listErr error
		files, listErr = catRepo.ListAuthorizedVolumeAssetFiles(ctx, scope, volumeID)
		return listErr
	})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, false, nil
		}
		return nil, true, err
	}
	if len(files) == 0 {
		return nil, false, nil
	}

	rendition, sawOffline, lastUnavail, err := s.tryVolumeAssetCandidates(ctx, domain.ItemTypeVolume, volumeID, volumeID, files)
	if err != nil {
		return nil, true, err
	}
	if rendition != nil {
		return rendition, true, nil
	}
	if sawOffline {
		return nil, true, fmt.Errorf("%w: %w", app.ErrServiceBusy, lastUnavail)
	}
	if lastUnavail != nil {
		return nil, true, fmt.Errorf("%w: %w", app.ErrNotFound, lastUnavail)
	}
	return nil, true, app.ErrNotFound
}

func (s *CoverService) getSeriesCover(
	ctx context.Context,
	scope storage.CatalogueReadScope,
	seriesID string,
) (*app.CoverRendition, error) {
	var lastUnavail error
	for offset := 0; ; {
		var batch []storage.SeriesCoverCandidate
		err := s.readTx.WithRepeatableReadTx(ctx, func(tx storage.ReadTx) error {
			catRepo := tx.Catalogue()
			if catRepo == nil {
				return errors.New("curation: read transaction missing catalogue repository")
			}
			var listErr error
			batch, listErr = catRepo.ListSeriesCoverCandidates(ctx, scope, seriesID, seriesCoverBatchSize, offset)
			return listErr
		})
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, app.ErrNotFound
			}
			return nil, err
		}
		if len(batch) == 0 {
			break
		}

		for _, volCand := range batch {
			rendition, _, unavailErr, tryErr := s.tryVolumeAssetCandidates(ctx, domain.ItemTypeSeries, seriesID, volCand.VolumeID, volCand.Files)
			if tryErr != nil {
				return nil, tryErr
			}
			if rendition != nil {
				return rendition, nil
			}
			if unavailErr != nil {
				lastUnavail = unavailErr
			}
		}

		if len(batch) < seriesCoverBatchSize {
			break
		}
		offset += len(batch)
	}

	if lastUnavail != nil {
		return nil, fmt.Errorf("%w: %w", app.ErrNotFound, lastUnavail)
	}
	return nil, app.ErrNotFound
}

func (s *CoverService) tryVolumeAssetCandidates(
	ctx context.Context,
	targetKind domain.ItemType,
	targetID string,
	volumeID string,
	files []storage.AuthorizedAssetFile,
) (rendition *app.CoverRendition, sawOffline bool, lastUnavail error, err error) {
	for i := range files {
		src := coverAssetSourceFromAuthorizedFile(&files[i])
		entry, genErr := s.GetOrGenerateCover(ctx, src)
		if genErr == nil {
			return &app.CoverRendition{
				TargetID:    targetID,
				ContentType: entry.ContentType,
				Data:        entry.Data,
				ETag:        computeCoverETag(targetKind, targetID, volumeID, src.AssetID, entry),
			}, false, nil, nil
		}
		if errors.Is(genErr, ErrCoverPoolNotRunning) || errors.Is(genErr, ErrCoverPoolClosed) || errors.Is(genErr, ErrCoverWaitTimeout) {
			return nil, false, nil, fmt.Errorf("%w: %w", app.ErrServiceBusy, genErr)
		}
		if isCandidateUnavailableError(genErr) {
			var unavail *CoverUnavailableError
			if errors.As(genErr, &unavail) && unavail.Reason == "root_offline" {
				sawOffline = true
			}
			lastUnavail = genErr
			continue
		}
		return nil, false, nil, genErr
	}
	return nil, sawOffline, lastUnavail, nil
}

func coverAssetSourceFromAuthorizedFile(file *storage.AuthorizedAssetFile) CoverAssetSource {
	if file == nil {
		return CoverAssetSource{}
	}
	return CoverAssetSource{
		AssetID:         file.AssetID,
		LibraryRootID:   file.LibraryRootID,
		RootPathBytes:   slices.Clone(file.RootPathBytes),
		RootState:       domain.LibraryRootState(file.RootState),
		RelPathBytes:    slices.Clone(file.RelPathBytes),
		FileSize:        file.FileSize,
		Format:          file.Format,
		DigestKind:      file.DigestKind,
		Digest:          file.Digest,
		PageCount:       file.PageCount,
		CoverPageIndex:  file.CoverPageIndex,
		SuppressedPages: slices.Clone(file.SuppressedPages),
		MissingSince:    file.MissingSince,
		RetiredAt:       file.RetiredAt,
	}
}

func computeCoverETag(targetKind domain.ItemType, targetID, volumeID, assetID string, entry image.CoverEntry) string {
	h := sha256.New()
	writeETagField(h, string(targetKind))
	writeETagField(h, targetID)
	writeETagField(h, volumeID)
	writeETagField(h, assetID)
	writeETagField(h, string(entry.Key.SourceKind))
	writeETagField(h, entry.Key.Digest)
	writeETagField(h, strconv.Itoa(entry.Key.PageIndex))
	writeETagField(h, string(entry.Key.Rendition.OutputFormat))
	writeETagField(h, strconv.Itoa(entry.Key.Rendition.MaxWidth))
	writeETagField(h, strconv.Itoa(entry.Key.Rendition.MaxHeight))
	writeETagField(h, strconv.Itoa(entry.Key.Rendition.Quality))
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(entry.Data)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(entry.Data)
	return fmt.Sprintf(`"%x"`, h.Sum(nil))
}

func writeETagField(h hash.Hash, val string) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(val)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(val))
}

// HidePage is scheduled for M2 (ADR-0010 §3).
func (s *CoverService) HidePage(context.Context, *domain.Principal, app.HidePageRequest) error {
	return app.ErrNotImplemented
}

// StageTrash is scheduled for M2 (ADR-0010 §4).
func (s *CoverService) StageTrash(context.Context, *domain.Principal, app.StageTrashRequest) error {
	return app.ErrNotImplemented
}

// ListTrash is scheduled for M2 (ADR-0010 §4).
func (s *CoverService) ListTrash(context.Context, *domain.Principal) ([]app.TrashItem, error) {
	return nil, app.ErrNotImplemented
}

// RestoreTrash is scheduled for M2 (ADR-0010 §4).
func (s *CoverService) RestoreTrash(context.Context, *domain.Principal, string) error {
	return app.ErrNotImplemented
}

// PurgeTrash is scheduled for M2 (ADR-0010 §4).
func (s *CoverService) PurgeTrash(context.Context, *domain.Principal, app.PurgeTrashRequest) (string, error) {
	return "", app.ErrNotImplemented
}
