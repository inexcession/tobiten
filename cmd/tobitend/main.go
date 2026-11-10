package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/logging"
)

// Version is the build version of tobitend.
var Version = "0.1.0-dev"

func versionString() string {
	return fmt.Sprintf("tobitend %s", Version)
}

func setupLogging(cfg *config.Config) error {
	logger, err := logging.New(logging.Options{
		Format: cfg.LogFormat,
		Level:  cfg.LogLevel,
	})
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	return nil
}

func run(args []string) int {
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

	// Hello-world output for initial M1-T28.1 slice
	fmt.Println(versionString())
	return 0
}

func main() {
	os.Exit(run(os.Args[1:]))
}
