package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/httpapi"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

var version = "dev"

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	if cfg.Version {
		fmt.Println(version)
		os.Exit(0)
	}

	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))

	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	errCh := make(chan error, 2)
	runners := 1
	go func() {
		if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("tcp server: %w", err)
			cancel() // cascade shutdown to the HTTP server
			return
		}
		errCh <- nil
	}()

	if cfg.HTTPPort > 0 {
		runners++
		go func() {
			if err := httpapi.Run(ctx, srv, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("http server: %w", err)
				cancel() // cascade shutdown to the TCP server
				return
			}
			errCh <- nil
		}()
	}

	var failed bool
	for i := 0; i < runners; i++ {
		if err := <-errCh; err != nil {
			log.Error("server error", "err", err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}
