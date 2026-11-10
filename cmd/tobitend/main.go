package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/inexcession/tobiten/internal/config"
)

// Version is the build version of tobitend.
var Version = "0.1.0-dev"

func versionString() string {
	return fmt.Sprintf("tobitend %s", Version)
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

	// Hello-world output for initial M1-T28.1 slice
	fmt.Println(versionString())
	return 0
}

func main() {
	os.Exit(run(os.Args[1:]))
}
