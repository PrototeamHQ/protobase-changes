// Command protobase-changes follows the app databases its config lists.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
)

// version is set at release builds with -ldflags "-X main.version=<version>".
var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "protobase-changes:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("protobase-changes", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path of the JSON config listing the apps to follow")
	showVersion := flags.Bool("version", false, "print the version and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		return printVersion(stdout)
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}

	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	cfg, err := config.Parse(f)
	f.Close()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = follow(ctx, cfg, slog.New(slog.NewJSONHandler(stderr, nil)))
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func printVersion(w io.Writer) error {
	_, err := fmt.Fprintln(w, version)
	return err
}
