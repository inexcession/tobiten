package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/api/openapi"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	tobitenimage "github.com/inexcession/tobiten/internal/image"
	"github.com/inexcession/tobiten/internal/image/purego"
	"github.com/inexcession/tobiten/internal/library/roots"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/module"
)

// Version is the build version of tobitend.
var Version = "0.1.0-dev"

func versionString() string {
	return fmt.Sprintf("tobitend %s", Version)
}

// bootstrapLogger uses canonical JSON and service metadata before stored logging
// settings can be read. Its defaults are constants validated by the logging package.
func bootstrapLogger() *slog.Logger {
	logger, err := logging.New(logging.Options{})
	if err != nil {
		panic(err)
	}
	return logger
}

func setupLogging(cfg *config.Config) error {
	noColor := true
	if cfg.LogFormat == logging.FormatTable {
		if os.Getenv("NO_COLOR") == "" && logging.IsTerminal(os.Stdout) {
			noColor = false
		}
	}
	logger, err := logging.New(logging.Options{
		Format:  cfg.LogFormat,
		Level:   cfg.LogLevel,
		NoColor: noColor,
	})
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	return nil
}

// RegistryDeps holds runtime module dependencies supplied by the daemon composition root.
type RegistryDeps struct {
	Scanner        *scanner.Scanner
	ScannerWorkers int
	Relocator      *roots.Relocator
}

// BuildRegistry constructs the composition root module registry for tobitend.
// Compiled-in modules are registered here (ADR-0018 §1).
// Defaults fail closed with nil services; the daemon supplies its owned application services.
func BuildRegistry(cfg *config.Config) module.Registry {
	return BuildRegistryWithServices(cfg, nil)
}

// BuildRegistryWithServices constructs the composition root module registry
// wired with the specified application services.
func BuildRegistryWithServices(cfg *config.Config, services *app.Services) module.Registry {
	return BuildRegistryWithDeps(cfg, services, RegistryDeps{})
}

// BuildRegistryWithDeps constructs the composition root module registry wired with
// application services and runtime module dependencies.
func BuildRegistryWithDeps(cfg *config.Config, services *app.Services, deps RegistryDeps) module.Registry {
	reg := module.NewRegistry()

	// Register compiled-in modules
	_ = reg.Register(api.NewSystemModule())
	_ = reg.Register(openapi.NewModule(services))
	var auths []module.Authenticator
	if services != nil {
		if authenticator, ok := services.BasicAuth.(module.Authenticator); ok {
			auths = append(auths, authenticator)
		}
		if authenticator, ok := services.PATAuth.(module.Authenticator); ok {
			auths = append(auths, authenticator)
		}
		if authenticator, ok := services.SessionAuth.(module.Authenticator); ok {
			auths = append(auths, authenticator)
		}
	}
	if len(auths) > 0 {
		_ = reg.Register(&module.SimpleModule{
			ModuleName: "auth",
			Auths:      auths,
		})
	}
	if deps.Scanner != nil {
		mod, err := scanner.NewModule(deps.Scanner, scanner.ModuleOptions{
			Workers: deps.ScannerWorkers,
		})
		if err != nil {
			slog.Error("failed to initialize scanner module", logging.Err(err))
		} else {
			_ = reg.Register(mod)
		}
	}
	if deps.Relocator != nil {
		mod, err := roots.NewModule(deps.Relocator)
		if err != nil {
			slog.Error("failed to initialize roots module", logging.Err(err))
		} else {
			_ = reg.Register(mod)
		}
	}
	if puregoPipeline, err := purego.New(purego.Options{}); err != nil {
		slog.Error("failed to initialize purego image pipeline", logging.Err(err))
	} else {
		_ = reg.Register(&module.SimpleModule{
			ModuleName: purego.PipelineName,
			Pipelines:  []tobitenimage.ImagePipeline{puregoPipeline},
		})
	}

	// Apply configuration (e.g. disabled modules) after registering real modules
	if cfg != nil {
		reg.ApplyConfig(cfg)
	}
	return reg
}

var startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
	reg := BuildRegistryWithServices(cfg, services)
	srv, err := api.NewServer(api.ServerOptions{
		Config:   cfg,
		Logger:   logger,
		Registry: reg,
	})
	if err != nil {
		return err
	}
	return srv.Run(ctx)
}

var startDaemon = runDaemon

func runContext(ctx context.Context, args []string) int {
	cfg, err := config.Load(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			config.PrintUsage(os.Stderr)
			return 0
		}
		if errors.Is(err, config.ErrInvalidFlag) {
			fmt.Fprintf(os.Stderr, "tobitend: %v\n", err)
			return 2
		}
		fmt.Fprintf(os.Stderr, "tobitend: %v\n", err)
		return 1
	}

	if cfg.ShowVersion {
		fmt.Println(versionString())
		return 0
	}

	if ctx.Err() != nil {
		return 0
	}
	slog.SetDefault(bootstrapLogger())
	if err := startDaemon(ctx, cfg, startServer); err != nil {
		slog.Error("server error", logging.Err(err))
		return 1
	}

	return 0
}

func run(args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runContext(ctx, args)
}

func main() {
	os.Exit(run(os.Args[1:]))
}
