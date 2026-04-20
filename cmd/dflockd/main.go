package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
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

	var wg sync.WaitGroup
	var tcpErr, httpErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			tcpErr = err
			cancel() // cascade shutdown to the HTTP server
		}
	}()

	if cfg.HTTPPort > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpapi.Run(ctx, srv, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
				httpErr = err
				cancel() // cascade shutdown to the TCP server
			}
		}()
	}

	wg.Wait()

	if tcpErr != nil {
		log.Error("tcp server error", "err", tcpErr)
	}
	if httpErr != nil {
		log.Error("http server error", "err", httpErr)
	}
	if tcpErr != nil || httpErr != nil {
		os.Exit(1)
	}
}
