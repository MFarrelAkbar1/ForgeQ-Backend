// Command api serves the ForgeQ HTTP API.
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

	"github.com/MFarrelAkbar1/ForgeQ/internal/api"
	"github.com/MFarrelAkbar1/ForgeQ/internal/config"
	"github.com/MFarrelAkbar1/ForgeQ/internal/dashboard"
	"github.com/MFarrelAkbar1/ForgeQ/internal/logging"
	"github.com/MFarrelAkbar1/ForgeQ/internal/metrics"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
)

const (
	shutdownTimeout = 15 * time.Second
	// notifyWindow bounds wake-up notifications to one per queue per window.
	notifyWindow = 50 * time.Millisecond
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
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

	if cfg.APIKey == "" {
		logger.Warn("API_KEY is not set: the API and dashboard are unauthenticated (development only)")
	}

	notifier := store.NewNotifier(pool, notifyWindow, logger)
	go notifier.Run(ctx)
	s := store.New(pool, store.WithNotifier(notifier))
	reg := metrics.NewRegistry()
	metrics.RegisterQueueDepth(reg, s.Stats)

	dash, err := dashboard.New(dashboard.Options{Store: s, Logger: logger, APIKey: cfg.APIKey})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: api.NewRouter(api.Options{
			Store:     s,
			Logger:    logger,
			Ready:     pool.Ping,
			Metrics:   metrics.Handler(reg),
			Observer:  metrics.NewAPI(reg),
			APIKey:    cfg.APIKey,
			Dashboard: dash,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}
