// Command gateway runs the Ocean live-event buffering gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/cache"
	"github.com/port-labs/ocean-gateway/internal/config"
	"github.com/port-labs/ocean-gateway/internal/port"
	"github.com/port-labs/ocean-gateway/internal/queue"
	"github.com/port-labs/ocean-gateway/internal/redisstream"
	"github.com/port-labs/ocean-gateway/internal/server"
	"github.com/port-labs/ocean-gateway/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load failed", "err", err)
		os.Exit(1)
	}
	log.Info("config loaded",
		"listenAddr", cfg.ListenAddr,
		"queueMaxBytes", cfg.QueueMaxBytes,
		"workers", cfg.Workers,
		"batchSize", cfg.BatchSize,
		"cacheTTL", cfg.CacheTTL.String(),
		"streamMaxLen", cfg.StreamMaxLen,
	)

	// Redis client + connectivity check.
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		log.Error("redis ping failed", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	cancelPing()

	// Dependencies.
	c := cache.New()
	c.StartJanitor(time.Minute)
	defer c.Close()

	q := queue.New(cfg.QueueMaxBytes)

	portClient := port.New(cfg.PortAPIBaseURL, &http.Client{Timeout: cfg.PortAPITimeout})
	streamWriter := redisstream.NewWriter(rdb, cfg.StreamMaxLen)

	pool := worker.New(q, streamWriter, log, cfg.Workers, cfg.BatchSize, cfg.ForwardMaxRetries, cfg.ForwardBackoff)

	h := server.NewHandler(c, portClient, q, log, cfg.CacheTTL)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.New(h, log),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start worker pool.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	workersDone := make(chan struct{})
	go func() {
		pool.Run(workerCtx)
		close(workersDone)
	}()

	// Start HTTP server.
	go func() {
		log.Info("gateway listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	// Wait for a shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info("shutdown initiated")

	// 1. Stop accepting new HTTP requests.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}

	// 2. Close the queue so workers drain remaining events, then exit.
	q.Close()
	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		log.Warn("drain timed out; forcing worker stop", "dropped", pool.Dropped())
	}
	cancelWorkers()

	_ = rdb.Close()
	log.Info("shutdown complete", "dropped", pool.Dropped())
}
