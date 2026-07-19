// Command gateway runs the Ocean live-event gateway. It writes each incoming
// webhook straight to a Redis stream and holds no buffer of its own, so it is
// stateless and horizontally scalable.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/config"
	"github.com/port-labs/ocean-gateway/internal/metrics"
	"github.com/port-labs/ocean-gateway/internal/redisstream"
	"github.com/port-labs/ocean-gateway/internal/server"
)

// Build-time metadata — injected via -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load failed", "err", err)
		os.Exit(1)
	}
	log.Info("starting ocean-gateway",
		"version", version,
		"commit", commit,
		"date", date,
		"goVersion", runtime.Version(),
		"listenAddr", cfg.ListenAddr,
		"redisURL", cfg.RedisURL,
		"redisPoolSize", cfg.RedisPoolSize,
		"streamMaxLen", cfg.StreamMaxLen,
		"eventTTL", cfg.EventTTL.String(),
		"streamTTL", cfg.StreamTTL.String(),
		"writeMaxRetries", cfg.WriteMaxRetries,
		"writeBackoff", cfg.WriteBackoff.String(),
	)

	// Register build info and start collecting Redis pool stats.
	metrics.RegisterBuildInfo(version, commit, date)

	// Redis client + connectivity check.
	redisOpts := &goredis.Options{
		Addr:     cfg.RedisURL,
		Username: cfg.RedisUsername,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize, // 0 => go-redis default (10 * GOMAXPROCS)
	}
	if cfg.RedisTLS {
		redisOpts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	rdb := goredis.NewClient(redisOpts)
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		log.Error("redis ping failed", "url", cfg.RedisURL, "err", err)
		os.Exit(1)
	}
	cancelPing()
	log.Info("redis connected", "url", cfg.RedisURL)

	metrics.RegisterRedisPool(func() metrics.PoolStats {
		s := rdb.PoolStats()
		return metrics.PoolStats{
			Hits:       s.Hits,
			Misses:     s.Misses,
			Timeouts:   s.Timeouts,
			TotalConns: s.TotalConns,
			IdleConns:  s.IdleConns,
			StaleConns: s.StaleConns,
		}
	})

	streamWriter := redisstream.NewWriter(rdb, cfg.StreamMaxLen, cfg.EventTTL, cfg.StreamTTL)
	h := server.NewHandler(streamWriter, log, cfg.WriteMaxRetries, cfg.WriteBackoff)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.New(h, func(ctx context.Context) error { return rdb.Ping(ctx).Err() }, version, commit, log),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("gateway listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	// Wait for a shutdown signal, then stop accepting requests and let in-flight
	// writes finish before closing Redis.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("shutdown initiated", "signal", sig.String())

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}

	_ = rdb.Close()
	log.Info("shutdown complete")
}
