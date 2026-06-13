// Package cache provides a small, dependency-free TTL cache mapping a
// logIngestId to its resolved integration identity.
package cache

import (
	"sync"
	"time"
)

// Value is the cached resolution of a logIngestId.
type Value struct {
	LiveEventsUUID string
	OrgID          string
}

type entry struct {
	val       Value
	expiresAt time.Time
}

// Cache is a goroutine-safe TTL cache. Entries expire lazily on Get and are
// also swept by an optional background janitor.
type Cache struct {
	mu    sync.RWMutex
	items map[string]entry
	now   func() time.Time // injectable clock for tests
	stop  chan struct{}
}

// New creates an empty cache. Call StartJanitor to enable background eviction.
func New() *Cache {
	return &Cache{
		items: make(map[string]entry),
		now:   time.Now,
		stop:  make(chan struct{}),
	}
}

// Get returns the cached value for key if present and unexpired.
func (c *Cache) Get(key string) (Value, bool) {
	c.mu.RLock()
	e, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || c.now().After(e.expiresAt) {
		return Value{}, false
	}
	return e.val, true
}

// Set stores val under key with the given time-to-live.
func (c *Cache) Set(key string, val Value, ttl time.Duration) {
	c.mu.Lock()
	c.items[key] = entry{val: val, expiresAt: c.now().Add(ttl)}
	c.mu.Unlock()
}

// StartJanitor runs a background sweep every interval until Close is called.
func (c *Cache) StartJanitor(interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-t.C:
				c.evictExpired()
			}
		}
	}()
}

func (c *Cache) evictExpired() {
	now := c.now()
	c.mu.Lock()
	for k, e := range c.items {
		if now.After(e.expiresAt) {
			delete(c.items, k)
		}
	}
	c.mu.Unlock()
}

// Close stops the background janitor (if running).
func (c *Cache) Close() {
	select {
	case <-c.stop:
		// already closed
	default:
		close(c.stop)
	}
}
