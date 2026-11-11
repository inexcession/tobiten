package module

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	pathPkg "path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/inexcession/tobiten/internal/config"
)

// Migration ranges for core and modules (ADR-0018 §2).
// Core migrations use versions 1..999.
// Modules use dedicated non-overlapping 1,000-version blocks.
const (
	CoreMinVersion = int64(1)
	CoreMaxVersion = int64(999)

	ModuleKomgaMinVersion   = int64(1000)
	ModuleKomgaMaxVersion   = int64(1999)
	ModuleOPDSMinVersion    = int64(2000)
	ModuleOPDSMaxVersion    = int64(2999)
	ModuleScraperMinVersion = int64(3000)
	ModuleScraperMaxVersion = int64(3999)
	ModuleManagedMinVersion = int64(4000)
	ModuleManagedMaxVersion = int64(4999)
	ModuleNotifyMinVersion  = int64(5000)
	ModuleNotifyMaxVersion  = int64(5999)
	ModuleTestMinVersion    = int64(9000)
	ModuleTestMaxVersion    = int64(9999)
)

// Sentinel errors returned by the module registry.
var (
	ErrDuplicateModule       = errors.New("module already registered")
	ErrModuleNotFound        = errors.New("module not found")
	ErrInvalidMigrationRange = errors.New("invalid module migration range")
	ErrCoreRangeConflict     = errors.New("module migration conflicts with core migration range (1..999)")
	ErrDuplicateMigration    = errors.New("duplicate migration version across modules")
)

// Module is the base interface that every protocol adapter and optional feature implements (ADR-0018 §1).
type Module interface {
	// Name returns the unique, canonical name of the module (e.g. "komga", "opds", "openapi").
	Name() string
}

// Routable indicates a module provides HTTP routes under one or more prefixes.
type Routable interface {
	RouteMounts() []RouteMount
}

// RouteMount defines an HTTP route prefix and handler provided by a module.
type RouteMount struct {
	ModuleName string
	Prefix     string
	Handler    http.Handler
}

// Migratable indicates a module contributes plain-SQL database schema migrations.
type Migratable interface {
	MigrationFS() fs.FS
}

// MigrationRanger indicates a module declares an expected inclusive version range [min, max].
type MigrationRanger interface {
	MigrationRange() (minVersion, maxVersion int64)
}

// AuthenticatorProvider indicates a module provides authenticators (ADR-0018 S1).
type AuthenticatorProvider interface {
	Authenticators() []Authenticator
}

// Authenticator represents a declared pluggable authentication mechanism.
type Authenticator interface {
	Name() string
}

// JobKindProvider indicates a module provides background job kinds (ADR-0017, ADR-0018).
type JobKindProvider interface {
	JobKinds() []JobKind
}

// JobKind represents a declared background job kind.
type JobKind interface {
	Name() string
}

// EventSubscriberProvider indicates a module provides domain event subscribers (ADR-0018 S3).
type EventSubscriberProvider interface {
	EventSubscribers() []EventSubscriber
}

// EventSubscriber represents a declared domain event subscriber.
type EventSubscriber interface {
	Name() string
}

// Registry manages compiled-in modules, their lifecycle, route mounts, and extension seams.
type Registry interface {
	// Register registers a compiled-in module. Returns ErrDuplicateModule if already registered.
	Register(m Module) error

	// Module returns the registered module with the given name, or nil if absent.
	Module(name string) Module

	// Modules returns all registered compiled-in modules in registration order.
	Modules() []Module

	// IsEnabled reports whether the named module is currently enabled.
	IsEnabled(name string) bool

	// SetEnabled dynamically enables or disables a module by name.
	SetEnabled(name string, enabled bool)

	// ApplyConfig updates module enablement based on the provided Config.
	ApplyConfig(cfg *config.Config)

	// EnabledModules returns all registered modules that are currently enabled.
	EnabledModules() []Module

	// Routes returns all RouteMounts from enabled modules.
	Routes() []RouteMount

	// Authenticators returns all Authenticators from enabled modules.
	Authenticators() []Authenticator

	// JobKinds returns all JobKinds from enabled modules.
	JobKinds() []JobKind

	// EventSubscribers returns all EventSubscribers from enabled modules.
	EventSubscribers() []EventSubscriber

	// MigrationSources returns migration filesystems from ALL registered modules
	// (both enabled and disabled), ensuring applied migrations are never missing (ADR-0018 §2).
	MigrationSources() []fs.FS

	// ValidateMigrations verifies that module migration versions do not collide with the core range,
	// stay within any declared ranges, and do not collide with other modules.
	ValidateMigrations() error
}

type registryImpl struct {
	mu       sync.RWMutex
	modules  []Module
	byName   map[string]Module
	disabled map[string]bool
}

// NewRegistry constructs a new, empty module registry.
func NewRegistry() Registry {
	return &registryImpl{
		modules:  make([]Module, 0),
		byName:   make(map[string]Module),
		disabled: make(map[string]bool),
	}
}

func canonicalName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func (r *registryImpl) Register(m Module) error {
	if m == nil {
		return errors.New("cannot register nil module")
	}

	name := canonicalName(m.Name())
	if name == "" {
		return errors.New("module name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byName[name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateModule, m.Name())
	}

	if ranger, ok := m.(MigrationRanger); ok {
		minV, maxV := ranger.MigrationRange()
		if minV != 0 || maxV != 0 {
			if minV <= 0 || maxV < minV || minV <= CoreMaxVersion {
				return fmt.Errorf("%w: module %q range [%d, %d] invalid (must be > %d)",
					ErrInvalidMigrationRange, m.Name(), minV, maxV, CoreMaxVersion)
			}
		}
	}

	r.modules = append(r.modules, m)
	r.byName[name] = m
	return nil
}

func (r *registryImpl) Module(name string) Module {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byName[canonicalName(name)]
}

func (r *registryImpl) Modules() []Module {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Module, len(r.modules))
	copy(out, r.modules)
	return out
}

func (r *registryImpl) IsEnabled(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cName := canonicalName(name)
	if _, ok := r.byName[cName]; !ok {
		return false
	}
	return !r.disabled[cName]
}

func (r *registryImpl) SetEnabled(name string, enabled bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cName := canonicalName(name)
	if enabled {
		delete(r.disabled, cName)
	} else {
		r.disabled[cName] = true
	}
}

func (r *registryImpl) ApplyConfig(cfg *config.Config) {
	if cfg == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range r.modules {
		cName := canonicalName(m.Name())
		if cfg.IsModuleEnabled(m.Name()) {
			delete(r.disabled, cName)
		} else {
			r.disabled[cName] = true
		}
	}
}

func (r *registryImpl) EnabledModules() []Module {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Module
	for _, m := range r.modules {
		if !r.disabled[canonicalName(m.Name())] {
			out = append(out, m)
		}
	}
	return out
}

func (r *registryImpl) Routes() []RouteMount {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var mounts []RouteMount
	for _, m := range r.modules {
		if r.disabled[canonicalName(m.Name())] {
			continue
		}
		if routable, ok := m.(Routable); ok {
			for _, mount := range routable.RouteMounts() {
				if mount.ModuleName == "" {
					mount.ModuleName = m.Name()
				}
				mounts = append(mounts, mount)
			}
		}
	}
	return mounts
}

func (r *registryImpl) Authenticators() []Authenticator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var auths []Authenticator
	for _, m := range r.modules {
		if r.disabled[canonicalName(m.Name())] {
			continue
		}
		if provider, ok := m.(AuthenticatorProvider); ok {
			auths = append(auths, provider.Authenticators()...)
		}
	}
	return auths
}

func (r *registryImpl) JobKinds() []JobKind {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var jobs []JobKind
	for _, m := range r.modules {
		if r.disabled[canonicalName(m.Name())] {
			continue
		}
		if provider, ok := m.(JobKindProvider); ok {
			jobs = append(jobs, provider.JobKinds()...)
		}
	}
	return jobs
}

func (r *registryImpl) EventSubscribers() []EventSubscriber {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var subs []EventSubscriber
	for _, m := range r.modules {
		if r.disabled[canonicalName(m.Name())] {
			continue
		}
		if provider, ok := m.(EventSubscriberProvider); ok {
			subs = append(subs, provider.EventSubscribers()...)
		}
	}
	return subs
}

// MigrationSources returns migration filesystems from ALL registered modules
// (both enabled and disabled). Disabling a module must not remove its migrations
// from the sequence (ADR-0018 §2).
func (r *registryImpl) MigrationSources() []fs.FS {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var sources []fs.FS
	for _, m := range r.modules {
		if migratable, ok := m.(Migratable); ok {
			if fsys := migratable.MigrationFS(); fsys != nil {
				sources = append(sources, fsys)
			}
		}
	}
	return sources
}

var migrationFilenamePattern = regexp.MustCompile(`^([0-9]+)(?:_([a-zA-Z0-9_\-]+))?\.sql$`)

func (r *registryImpl) ValidateMigrations() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seenVersions := make(map[int64]string) // version -> moduleName

	for _, m := range r.modules {
		migratable, ok := m.(Migratable)
		if !ok {
			continue
		}
		fsys := migratable.MigrationFS()
		if fsys == nil {
			continue
		}

		var minRange, maxRange int64
		hasRange := false
		if ranger, ok := m.(MigrationRanger); ok {
			minRange, maxRange = ranger.MigrationRange()
			if minRange != 0 || maxRange != 0 {
				hasRange = true
			}
		}

		err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			filename := pathPkg.Base(p)
			if !strings.HasSuffix(filename, ".sql") {
				return nil
			}

			matches := migrationFilenamePattern.FindStringSubmatch(filename)
			if len(matches) == 0 {
				return fmt.Errorf("module %q: filename %q does not match pattern <version>_<description>.sql", m.Name(), filename)
			}

			version, err := strconv.ParseInt(matches[1], 10, 64)
			if err != nil {
				return fmt.Errorf("module %q: invalid version in %q: %w", m.Name(), filename, err)
			}

			if version <= CoreMaxVersion {
				return fmt.Errorf("%w: module %q migration %d (%s) must be >= %d",
					ErrCoreRangeConflict, m.Name(), version, filename, CoreMaxVersion+1)
			}

			if hasRange && (version < minRange || version > maxRange) {
				return fmt.Errorf("%w: module %q migration %d (%s) outside declared range [%d, %d]",
					ErrInvalidMigrationRange, m.Name(), version, filename, minRange, maxRange)
			}

			if prevMod, exists := seenVersions[version]; exists {
				return fmt.Errorf("%w: version %d found in both module %q and module %q",
					ErrDuplicateMigration, version, prevMod, m.Name())
			}

			seenVersions[version] = m.Name()
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// SimpleModule is a convenience struct implementing Module and seam interfaces.
type SimpleModule struct {
	ModuleName  string
	Mounts      []RouteMount
	Migrations  fs.FS
	MinVersion  int64
	MaxVersion  int64
	Auths       []Authenticator
	Jobs        []JobKind
	Subscribers []EventSubscriber
}

// NewSimpleModule creates a new SimpleModule.
func NewSimpleModule(name string) *SimpleModule {
	return &SimpleModule{
		ModuleName: name,
	}
}

func (s *SimpleModule) Name() string {
	return s.ModuleName
}

func (s *SimpleModule) RouteMounts() []RouteMount {
	return s.Mounts
}

func (s *SimpleModule) MigrationFS() fs.FS {
	return s.Migrations
}

func (s *SimpleModule) MigrationRange() (int64, int64) {
	return s.MinVersion, s.MaxVersion
}

func (s *SimpleModule) Authenticators() []Authenticator {
	return s.Auths
}

func (s *SimpleModule) JobKinds() []JobKind {
	return s.Jobs
}

func (s *SimpleModule) EventSubscribers() []EventSubscriber {
	return s.Subscribers
}

// NamedAuthenticator is a simple string-backed Authenticator descriptor.
type NamedAuthenticator string

func (n NamedAuthenticator) Name() string { return string(n) }

// NamedJobKind is a simple string-backed JobKind descriptor.
type NamedJobKind string

func (n NamedJobKind) Name() string { return string(n) }

// NamedEventSubscriber is a simple string-backed EventSubscriber descriptor.
type NamedEventSubscriber string

func (n NamedEventSubscriber) Name() string { return string(n) }
