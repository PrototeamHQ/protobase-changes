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
	"github.com/PrototeamHQ/protobase-changes/internal/destination"
	"github.com/PrototeamHQ/protobase-changes/internal/feed"
	"github.com/PrototeamHQ/protobase-changes/internal/serve"
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
	runMigrate := flags.Bool("migrate", false, "apply the destination stores' schema and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		return printVersion(stdout)
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}
	recreateLost, err := config.RecreateLostSlots(os.LookupEnv)
	if err != nil {
		return err
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
	ss, err := stores(cfg)
	if err != nil {
		return err
	}
	defer closeStores(ss)
	if *runMigrate {
		return migrate(ctx, ss)
	}

	log := slog.New(slog.NewJSONHandler(stderr, nil))
	fs, err := feeds(cfg, log)
	if err != nil {
		return err
	}
	hs, err := hubs(ctx, cfg, fs, ss, log)
	if err != nil {
		return err
	}
	err = serveAndFollow(ctx, cfg, fs, hs, recreateLost, log)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// serveAndFollow runs the readers, the destinations and the endpoints until
// ctx ends or the endpoints fail, which stops the others. Readers and
// destinations run again on their own after a failure.
func serveAndFollow(ctx context.Context, cfg config.Config, fs map[string]*feed.Feed, hs map[string]*destination.Hub, recreateLost bool, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, 3)
	go func() { errs <- follow(ctx, cfg, fs, recreateLost, log) }()
	go func() { errs <- deliver(ctx, hs, log) }()
	go func() { errs <- listen(ctx, cfg.Listen, serve.New(fs, hs).Handler()) }()
	first := <-errs
	cancel()
	<-errs
	<-errs
	return first
}

func printVersion(w io.Writer) error {
	_, err := fmt.Fprintln(w, version)
	return err
}
