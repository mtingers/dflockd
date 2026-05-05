// Command dflockd is the distributed FIFO lock server.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/httpapi"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// version is set by the release tooling via -ldflags="-X main.version=...".
var version = "dev"

func main() {
	cfg := mustLoadConfig()
	if cfg.Version {
		fmt.Println(version)
		return
	}
	os.Exit(run(cfg))
}

// mustLoadConfig parses argv or exits with a friendly error.
func mustLoadConfig() *config.Config {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}
	return cfg
}

// run wires up the lock manager + servers, blocks until shutdown,
// and returns the process exit code.
func run(cfg *config.Config) int {
	log := newLogger(cfg.Debug)
	srv := server.New(lock.NewLockManager(cfg, log), cfg, log)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return runAll(ctx, srv, cfg, log, cancel)
}

func newLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// runAll spawns the TCP server and (optionally) the HTTP server,
// waits for both to exit, and returns 0 on clean shutdown / 1 on
// any runner error.
func runAll(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger, cancel context.CancelFunc) int {
	var wg sync.WaitGroup
	var failed atomic.Bool
	runOne(&wg, "tcp server", &failed, cancel, log, func() error { return srv.Run(ctx) })
	if cfg.HTTPPort > 0 {
		runOne(&wg, "http server", &failed, cancel, log, func() error { return httpapi.Run(ctx, srv, cfg, log) })
	}
	wg.Wait()
	if failed.Load() {
		return 1
	}
	return 0
}

// runOne launches one server goroutine. Real errors are logged and
// cancel the parent ctx so the sibling exits promptly.
func runOne(wg *sync.WaitGroup, name string, failed *atomic.Bool, cancel context.CancelFunc, log *slog.Logger, fn func() error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		recordRunnerErr(fn(), name, failed, cancel, log)
	}()
}

// recordRunnerErr classifies a runner's return value. Context
// cancellation is treated as clean shutdown.
func recordRunnerErr(err error, name string, failed *atomic.Bool, cancel context.CancelFunc, log *slog.Logger) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.Error("server error", "err", fmt.Errorf("%s: %w", name, err))
	failed.Store(true)
	cancel()
}
