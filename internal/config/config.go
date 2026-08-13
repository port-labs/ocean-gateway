// Package config loads gateway configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the gateway.
type Config struct {
	ListenAddr        string
	MetricsListenAddr string

	RedisURL      string
	RedisUsername string
	RedisPassword string
	RedisTLS      bool
	RedisDB       int
	RedisPoolSize int // 0 = go-redis default (10 * GOMAXPROCS)

	StreamMaxLen int64 // 0 = uncapped
	EventTTL     time.Duration
	StreamTTL    time.Duration

	// Per-request synchronous write retry policy.
	WriteMaxRetries int
	WriteBackoff    time.Duration
}

// Load builds a Config from environment variables, applying defaults for any
// unset value. It returns an error only when a set value fails to parse.
func Load() (Config, error) {
	c := Config{
		ListenAddr:        getStr("LISTEN_ADDR", ":8080"),
		MetricsListenAddr: getStr("METRICS_LISTEN_ADDR", ":9100"),
		RedisURL:          trimRedisAddr(getStr("REDIS_LIVE_EVENTS_URL", "localhost:6379")),
		RedisUsername:     getStr("REDIS_LIVE_EVENTS_USERNAME", ""),
		RedisPassword:     getStr("REDIS_LIVE_EVENTS_PASSWORD", ""),
		RedisTLS:          strings.EqualFold(getStr("REDIS_LIVE_EVENTS_ENABLE_TLS", "false"), "true"),
	}

	var err error
	if c.RedisDB, err = getInt("REDIS_DB", 0); err != nil {
		return c, err
	}
	if c.RedisPoolSize, err = getInt("REDIS_POOL_SIZE", 0); err != nil {
		return c, err
	}
	if c.StreamMaxLen, err = getInt64("REDIS_STREAM_MAXLEN", 0); err != nil {
		return c, err
	}
	if c.EventTTL, err = getDur("EVENT_TTL", 0); err != nil {
		return c, err
	}
	if c.StreamTTL, err = getDur("STREAM_TTL", 30*24*time.Hour); err != nil {
		return c, err
	}
	if c.WriteMaxRetries, err = getInt("WRITE_MAX_RETRIES", 2); err != nil {
		return c, err
	}
	if c.WriteBackoff, err = getDur("WRITE_BACKOFF_BASE", 50*time.Millisecond); err != nil {
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

// trimRedisAddr strips a redis:// or rediss:// scheme so the value can be used
// as go-redis Options.Addr (host:port). Auth and path components are not parsed.
func trimRedisAddr(addr string) string {
	lower := strings.ToLower(addr)
	// Check rediss:// before redis:// — the latter is a prefix of the former.
	for _, prefix := range []string{"rediss://", "redis://"} {
		if strings.HasPrefix(lower, prefix) {
			return addr[len(prefix):]
		}
	}
	return addr
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
