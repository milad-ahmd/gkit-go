// Package feature provides a runtime feature flag store backed by Redis.
//
// Three flag modes are supported:
//
//   - Global on/off  — IsEnabled(ctx, "new-checkout")
//   - Percentage rollout — enabled for N% of entities (stable hash-based)
//   - Allow-list — enabled for specific entity IDs (users, orgs, etc.)
//
// Flags are stored in Redis as JSON hashes, so they can be modified at runtime
// without restarting the service.
//
// # Usage
//
//	store := feature.NewStore(redisClient, feature.WithNamespace("myapp"))
//
//	// Create / update a flag (admin panel, CLI, etc.)
//	_ = store.Set(ctx, "dark-mode", feature.Flag{
//	    Enabled:    true,
//	    Percentage: 20,                          // 20% of users
//	    AllowList:  []string{"u1", "u2"},         // always enabled for these
//	})
//
//	// Check in a handler.
//	if store.IsEnabledFor(ctx, "dark-mode", userID) {
//	    renderDarkTheme(w)
//	}
package feature

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Flag defines the configuration of a feature flag.
type Flag struct {
	// Enabled globally turns the flag on or off. If false, all other settings
	// are ignored and the flag is considered disabled.
	Enabled bool `json:"enabled"`

	// Percentage (0–100) enables the flag for a stable percentage of entities.
	// A value of 0 means "use allow-list only"; 100 means "everyone".
	// The selection is deterministic: the same entity ID always maps to the
	// same bucket using a 64-bit hash.
	Percentage int `json:"percentage,omitempty"`

	// AllowList is a set of entity IDs for which the flag is always enabled,
	// regardless of Percentage. Checked before the percentage bucket.
	AllowList []string `json:"allow_list,omitempty"`

	// UpdatedAt is set automatically by Store.Set.
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// --------------------------------------------------------------------------
// Store

// Store manages feature flags in Redis.
type Store struct {
	client    *redis.Client
	namespace string
	log       *slog.Logger
}

// Option configures a Store.
type Option func(*Store)

// WithNamespace prefixes all Redis keys with "namespace:flags:" to isolate
// flags from other data in a shared Redis instance.
func WithNamespace(ns string) Option {
	return func(s *Store) { s.namespace = ns }
}

// WithLogger sets the logger used for internal events.
func WithLogger(l *slog.Logger) Option {
	return func(s *Store) { s.log = l }
}

// NewStore creates a Store backed by the given Redis client.
func NewStore(client *redis.Client, opts ...Option) *Store {
	s := &Store{
		client:    client,
		namespace: "gkit",
		log:       slog.Default(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Set creates or updates a flag.
func (s *Store) Set(ctx context.Context, name string, flag Flag) error {
	flag.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(flag)
	if err != nil {
		return fmt.Errorf("feature: marshal %q: %w", name, err)
	}
	if err := s.client.Set(ctx, s.key(name), raw, 0).Err(); err != nil {
		return fmt.Errorf("feature: set %q: %w", name, err)
	}
	s.log.Info("feature: flag updated", "flag", name, "enabled", flag.Enabled)
	return nil
}

// Get retrieves a flag. Returns the zero Flag and false if not found.
func (s *Store) Get(ctx context.Context, name string) (Flag, bool, error) {
	raw, err := s.client.Get(ctx, s.key(name)).Bytes()
	if err == redis.Nil {
		return Flag{}, false, nil
	}
	if err != nil {
		return Flag{}, false, fmt.Errorf("feature: get %q: %w", name, err)
	}
	var f Flag
	if err := json.Unmarshal(raw, &f); err != nil {
		return Flag{}, false, fmt.Errorf("feature: unmarshal %q: %w", name, err)
	}
	return f, true, nil
}

// Delete removes a flag. No-op if it doesn't exist.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := s.client.Del(ctx, s.key(name)).Err(); err != nil {
		return fmt.Errorf("feature: delete %q: %w", name, err)
	}
	return nil
}

// IsEnabled reports whether the flag is globally enabled (ignores entity).
// Returns false on any error (fail-safe default).
func (s *Store) IsEnabled(ctx context.Context, name string) bool {
	f, ok, err := s.Get(ctx, name)
	if err != nil {
		s.log.Warn("feature: error reading flag", "flag", name, "error", err)
		return false
	}
	return ok && f.Enabled && (f.Percentage == 0 || f.Percentage >= 100) && len(f.AllowList) == 0
}

// IsEnabledFor reports whether the flag is enabled for the given entity ID.
// Evaluation order:
//  1. If flag not found or globally disabled → false
//  2. If entityID is in AllowList → true
//  3. If Percentage > 0 → check hash bucket
//  4. Otherwise → false
//
// Returns false on any error (fail-safe).
func (s *Store) IsEnabledFor(ctx context.Context, name, entityID string) bool {
	f, ok, err := s.Get(ctx, name)
	if err != nil {
		s.log.Warn("feature: error reading flag", "flag", name, "error", err)
		return false
	}
	if !ok || !f.Enabled {
		return false
	}

	// Allow-list check.
	for _, id := range f.AllowList {
		if id == entityID {
			return true
		}
	}

	// Percentage rollout: deterministic hash.
	if f.Percentage > 0 {
		bucket := hashBucket(name+":"+entityID, 100)
		return bucket < f.Percentage
	}

	// Globally enabled with no percentage or allow-list restrictions.
	return true
}

// ListAll returns all flags stored under this namespace.
func (s *Store) ListAll(ctx context.Context) (map[string]Flag, error) {
	pattern := s.key("*")
	var cursor uint64
	flags := make(map[string]Flag)
	prefix := s.key("")

	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("feature: scan: %w", err)
		}
		for _, k := range keys {
			raw, err := s.client.Get(ctx, k).Bytes()
			if err != nil {
				continue
			}
			var f Flag
			if err := json.Unmarshal(raw, &f); err != nil {
				continue
			}
			name := k[len(prefix):]
			flags[name] = f
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return flags, nil
}

// --------------------------------------------------------------------------
// Helpers

func (s *Store) key(name string) string {
	return fmt.Sprintf("%s:flags:%s", s.namespace, name)
}

// hashBucket returns a stable integer in [0, buckets) for the given key.
// Uses FNV-1a for speed and uniform distribution.
func hashBucket(key string, buckets int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(buckets))
}
