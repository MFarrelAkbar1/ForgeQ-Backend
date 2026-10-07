// Command worker claims and runs ForgeQ jobs.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ/internal/backoff"
	"github.com/MFarrelAkbar1/ForgeQ/internal/config"
	"github.com/MFarrelAkbar1/ForgeQ/internal/handlers"
	"github.com/MFarrelAkbar1/ForgeQ/internal/logging"
	"github.com/MFarrelAkbar1/ForgeQ/internal/metrics"
	"github.com/MFarrelAkbar1/ForgeQ/internal/ops"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := logging.New(os.Stdout, cfg.LogLevel, cfg.LogFormat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := store.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The notifier outlives w.Run (own context) so wake-ups for jobs released
	// during shutdown are still sent to the other workers.
	notifier := store.NewNotifier(pool, 50*time.Millisecond, logger)
	notifyCtx, stopNotifier := context.WithCancel(context.Background())
	notifierDone := make(chan struct{})
	go func() {
		notifier.Run(notifyCtx)
		close(notifierDone)
	}()
	s := store.New(pool, store.WithNotifier(notifier))
	reg := metrics.NewRegistry()
	observer := metrics.NewWorker(reg)
	registry := worker.NewRegistry()
	handlers.Register(registry, handlers.Options{FailRate: cfg.Worker.DemoFailRate, Logger: logger})

	w := worker.New(worker.Config{
		ID:                cfg.Worker.ID,
		Queues:            cfg.Worker.Queues,
		PollInterval:      cfg.Worker.PollInterval,
		LeaseDuration:     cfg.Worker.LeaseDuration,
		HeartbeatInterval: cfg.Worker.HeartbeatInterval,
		JobTimeout:        cfg.Worker.JobTimeout,
		ShutdownTimeout:   cfg.Worker.ShutdownTimeout,
		Observer:          observer,
		RetryDelay: backoff.Policy{
			Base:   cfg.Worker.BackoffBase,
			Max:    cfg.Worker.BackoffMax,
			Jitter: cfg.Worker.BackoffJitter,
		}.Delay,
	}, s, registry, logger)

	reaper := worker.NewReaper(worker.ReaperConfig{
		Interval:  cfg.Worker.ReaperInterval,
		Retention: cfg.Worker.JobRetention,
		Observer:  observer,
	}, s, logger)

	opsSrv := &http.Server{
		Addr:              cfg.Worker.OpsAddr,
		Handler:           ops.NewMux(pool.Ping, metrics.Handler(reg)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("ops endpoints listening", "addr", cfg.Worker.OpsAddr)
		if err := opsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("ops server failed", "err", err)
		}
	}()

	go store.Listen(ctx, cfg.DatabaseURL, logger, w.Notify)
	go reaper.Run(ctx)

	err = w.Run(ctx)
	stopNotifier()
	<-notifierDone

	// Keep the ops endpoints up until the jobs have drained, then close them.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = opsSrv.Shutdown(shutdownCtx)
	return err
}
