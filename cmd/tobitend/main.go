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
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/module"
)

// Version is the build version of tobitend.
var Version = "0.1.0-dev"

func versionString() string {
	return fmt.Sprintf("tobitend %s", Version)
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

// BuildRegistry constructs the composition root module registry for tobitend.
// Compiled-in modules are registered here (ADR-0018 §1).
func BuildRegistry(cfg *config.Config) module.Registry {
	return BuildRegistryWithServices(cfg, app.NewStubServices())
}

// BuildRegistryWithServices constructs the composition root module registry
// wired with the specified application services.
func BuildRegistryWithServices(cfg *config.Config, services *app.Services) module.Registry {
	reg := module.NewRegistry()
	if services == nil {
		services = app.NewStubServices()
	}

	// Register compiled-in modules
	_ = reg.Register(api.NewSystemModule())
	_ = reg.Register(openapi.NewModule(services))

	// Apply configuration (e.g. disabled modules) after registering real modules
	if cfg != nil {
		reg.ApplyConfig(cfg)
	}
	return reg
}

var startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	reg := BuildRegistry(cfg)
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

	if err := setupLogging(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "tobitend: failed to initialize logger: %v\n", err)
		return 1
	}

	if ctx.Err() != nil {
		return 0
	}

	slog.Info("starting server daemon", slog.String("version", Version))

	if err := startServer(ctx, cfg, slog.Default()); err != nil {
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
