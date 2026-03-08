// Package cache provides a generic, thread-safe LRU cache with optional per-entry TTL.
//
// The cache evicts the least-recently-used entry when it reaches capacity.
// TTL expiry is checked lazily on access, and a background goroutine can be
// started for proactive cleanup.
//
// Example:
//
//	c := cache.New[string, User](1000, cache.WithTTL[string, User](5*time.Minute))
//	c.Set("alice", alice)
//	if user, ok := c.Get("alice"); ok {
//	    fmt.Println(user.Name)
//	}
package cache

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// Cache is a generic, thread-safe LRU cache.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration

	list  *list.List
	items map[K]*list.Element

	hits   uint64
	misses uint64
	evicts uint64
}

// entry is the value stored in each list element.
type entry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time // zero means no TTL
}

// Option configures a Cache.
type Option[K comparable, V any] func(*Cache[K, V])

// WithTTL sets a time-to-live for each cache entry.
// Entries are expired lazily on access; use StartJanitor for proactive cleanup.
func WithTTL[K comparable, V any](ttl time.Duration) Option[K, V] {
	return func(c *Cache[K, V]) { c.ttl = ttl }
}

// New creates a new Cache with the given capacity.
// Panics if capacity < 1.
func New[K comparable, V any](capacity int, opts ...Option[K, V]) *Cache[K, V] {
	if capacity < 1 {
		panic("cache: capacity must be >= 1")
	}
	c := &Cache[K, V]{
		capacity: capacity,
		list:     list.New(),
		items:    make(map[K]*list.Element, capacity),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Get retrieves the value associated with key.
// The second return value is false if the key is absent or expired.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		c.misses++
		var zero V
		return zero, false
	}

	e := el.Value.(*entry[K, V])

	if c.isExpired(e) {
		c.removeElement(el)
		c.misses++
		var zero V
		return zero, false
	}

	c.list.MoveToFront(el)
	c.hits++
	return e.value, true
}

// Set inserts or updates the entry for key.
// If the cache is at capacity, the least-recently-used entry is evicted first.
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.list.MoveToFront(el)
		e := el.Value.(*entry[K, V])
		e.value = value
		e.expiresAt = c.newExpiry()
		return
	}

	if c.list.Len() >= c.capacity {
		c.evictOldest()
	}

	e := &entry[K, V]{key: key, value: value, expiresAt: c.newExpiry()}
	el := c.list.PushFront(e)
	c.items[key] = el
}

// Delete removes the entry for key, if it exists.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeElement(el)
	}
}

// Clear removes all entries from the cache.
func (c *Cache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list.Init()
	c.items = make(map[K]*list.Element, c.capacity)
}

// Len returns the number of items currently in the cache.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

// Stats returns hit/miss/eviction counters.
type Stats struct {
	Hits   uint64
	Misses uint64
	Evicts uint64
	Len    int
}

// Stats returns a snapshot of cache statistics.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits:   c.hits,
		Misses: c.misses,
		Evicts: c.evicts,
		Len:    c.list.Len(),
	}
}

// StartJanitor launches a background goroutine that periodically removes
// expired entries. It stops when ctx is cancelled. Only useful when a TTL
// is configured.
func (c *Cache[K, V]) StartJanitor(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.deleteExpired()
			}
		}
	}()
}

// deleteExpired scans the entire cache and removes expired entries.
func (c *Cache[K, V]) deleteExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl == 0 {
		return
	}
	var next *list.Element
	for el := c.list.Back(); el != nil; el = next {
		next = el.Prev()
		if c.isExpired(el.Value.(*entry[K, V])) {
			c.removeElement(el)
		}
	}
}

// ---- internal helpers ----

func (c *Cache[K, V]) newExpiry() time.Time {
	if c.ttl == 0 {
		return time.Time{}
	}
	return time.Now().Add(c.ttl)
}

func (c *Cache[K, V]) isExpired(e *entry[K, V]) bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

func (c *Cache[K, V]) evictOldest() {
	if el := c.list.Back(); el != nil {
		c.removeElement(el)
		c.evicts++
	}
}

func (c *Cache[K, V]) removeElement(el *list.Element) {
	c.list.Remove(el)
	delete(c.items, el.Value.(*entry[K, V]).key)
}
