package redisclient

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/config"
)

func TestNewStandaloneAutoDetect(t *testing.T) {
	mr := miniredis.RunT(t)

	cfg := config.Config{
		RedisURL: mr.Addr(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if c.Mode != ModeStandalone {
		t.Fatalf("mode = %q, want %q", c.Mode, ModeStandalone)
	}
}

func TestIsRedisClusterEnabledStandalone(t *testing.T) {
	mr := miniredis.RunT(t)

	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer client.Close()

	ctx := context.Background()
	enabled, err := isRedisClusterEnabled(ctx, client)
	if err != nil {
		t.Fatalf("isRedisClusterEnabled: %v", err)
	}
	if enabled {
		t.Fatal("expected cluster_enabled=0 for miniredis standalone")
	}
}
