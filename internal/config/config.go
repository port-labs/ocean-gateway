// Package config loads gateway configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the gateway.
type Config struct {
	ListenAddr string

	PortAPIBaseURL string
	PortAPITimeout time.Duration

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	QueueMaxBytes int64
	Workers       int
	BatchSize     int
	CacheTTL      time.Duration

	StreamMaxLen int64 // 0 = uncapped

	ForwardMaxRetries int
	ForwardBackoff    time.Duration
}

// Load builds a Config from environment variables, applying defaults for any
// unset value. It returns an error only when a set value fails to parse.
func Load() (Config, error) {
	c := Config{
		ListenAddr:     getStr("LISTEN_ADDR", ":8080"),
		PortAPIBaseURL: getStr("PORT_API_BASE_URL", "https://api.getport.io"),
		RedisAddr:      getStr("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  getStr("REDIS_PASSWORD", ""),
	}

	var err error
	if c.PortAPITimeout, err = getDur("PORT_API_TIMEOUT", 10*time.Second); err != nil {
		return c, err
	}
	if c.RedisDB, err = getInt("REDIS_DB", 0); err != nil {
		return c, err
	}
	if c.QueueMaxBytes, err = getInt64("QUEUE_MAX_BYTES", 1<<30); err != nil { // 1 GiB
		return c, err
	}
	if c.Workers, err = getInt("WORKER_CONCURRENCY", 8); err != nil {
		return c, err
	}
	if c.BatchSize, err = getInt("QUEUE_BATCH_SIZE", 500); err != nil {
		return c, err
	}
	if c.CacheTTL, err = getDur("CACHE_TTL", time.Hour); err != nil {
		return c, err
	}
	if c.StreamMaxLen, err = getInt64("REDIS_STREAM_MAXLEN", 0); err != nil {
		return c, err
	}
	if c.ForwardMaxRetries, err = getInt("FORWARD_MAX_RETRIES", 3); err != nil {
		return c, err
	}
	if c.ForwardBackoff, err = getDur("FORWARD_BACKOFF_BASE", 100*time.Millisecond); err != nil {
		return c, err
	}
	return c, nil
}

func getStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func getInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func getInt64(key string, def int64) (int64, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func getDur(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
