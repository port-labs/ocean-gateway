// Package redisclient builds a go-redis client for standalone or cluster Redis.
//
// Connection mode is auto-detected by probing cluster_enabled via INFO cluster,
// matching port_ocean.consumers.redis_client.create_redis_client in Ocean.
package redisclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/config"
)

// Mode is the detected Redis deployment type.
type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeCluster    Mode = "cluster"
)

// Client wraps a go-redis UniversalClient with the resolved connection mode.
type Client struct {
	goredis.UniversalClient
	Mode Mode
}

// New connects to Redis using cfg and returns a client suited to the deployment.
func New(ctx context.Context, cfg config.Config) (*Client, error) {
	opts := optionsFromConfig(cfg)

	probe := goredis.NewClient(opts)
	clusterEnabled, err := isRedisClusterEnabled(ctx, probe)
	_ = probe.Close()
	if err != nil {
		return nil, fmt.Errorf("cluster probe: %w", err)
	}

	var (
		rdb  goredis.UniversalClient
		mode Mode
	)
	if clusterEnabled {
		mode = ModeCluster
		uniOpts := universalOptsFromOptions(opts)
		uniOpts.IsClusterMode = true
		rdb = goredis.NewUniversalClient(uniOpts)
	} else {
		mode = ModeStandalone
		rdb = goredis.NewClient(opts)
	}

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{UniversalClient: rdb, Mode: mode}, nil
}

func optionsFromConfig(cfg config.Config) *goredis.Options {
	opts := &goredis.Options{
		Addr:     cfg.RedisURL,
		Username: cfg.RedisUsername,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize,
	}
	if cfg.RedisTLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return opts
}

func universalOptsFromOptions(opts *goredis.Options) *goredis.UniversalOptions {
	return &goredis.UniversalOptions{
		Addrs:     []string{opts.Addr},
		Username:  opts.Username,
		Password:  opts.Password,
		DB:        opts.DB,
		PoolSize:  opts.PoolSize,
		TLSConfig: opts.TLSConfig,
	}
}

func isRedisClusterEnabled(ctx context.Context, c *goredis.Client) (bool, error) {
	info, err := c.Info(ctx, "cluster").Result()
	if err != nil {
		if clusterInfoUnsupported(err) {
			return false, nil
		}
		return false, err
	}
	return strings.Contains(info, "cluster_enabled:1"), nil
}

func clusterInfoUnsupported(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "not supported") || strings.Contains(msg, "unknown command")
}
