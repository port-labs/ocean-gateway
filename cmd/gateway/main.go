// Command gateway runs the Ocean live-event gateway. It writes each incoming
// webhook straight to a Redis stream and holds no buffer of its own, so it is
// stateless and horizontally scalable.
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

	"github.com/port-labs/ocean-gateway/internal/config"
	"github.com/port-labs/ocean-gateway/internal/redisstream"
	"github.com/port-labs/ocean-gateway/internal/server"
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
		"redisPoolSize", cfg.RedisPoolSize,
		"streamMaxLen", cfg.StreamMaxLen,
		"eventTTL", cfg.EventTTL.String(),
		"streamTTL", cfg.StreamTTL.String(),
		"writeMaxRetries", cfg.WriteMaxRetries,
	)

	// Redis client + connectivity check.
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize, // 0 => go-redis default
	})
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		log.Error("redis ping failed", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	cancelPing()

	streamWriter := redisstream.NewWriter(rdb, cfg.StreamMaxLen, cfg.EventTTL, cfg.StreamTTL)
	h := server.NewHandler(streamWriter, log, cfg.WriteMaxRetries, cfg.WriteBackoff)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.New(h, log),
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
	<-sigCh
	log.Info("shutdown initiated")

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}

	_ = rdb.Close()
	log.Info("shutdown complete")
}
