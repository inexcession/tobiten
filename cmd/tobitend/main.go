package main

import (
	"flag"
	"fmt"
	"os"
)

// Version is the build version of tobitend.
var Version = "0.1.0-dev"

func versionString() string {
	return fmt.Sprintf("tobitend %s", Version)
}

func run(args []string) int {
	fs := flag.NewFlagSet("tobitend", flag.ContinueOnError)
	versionFlag := fs.Bool("version", false, "print version and exit")
	vFlag := fs.Bool("v", false, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *versionFlag || *vFlag {
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
