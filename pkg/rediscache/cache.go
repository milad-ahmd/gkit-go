// Package rediscache provides a Redis-backed generic cache with JSON
// serialization, OpenTelemetry tracing, and a health.Checker implementation.
//
// It mirrors the API of pkg/cache so the two can be used interchangeably
// behind an interface.
package rediscache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Cache is a Redis-backed cache for values of type V.
// Keys are strings; values are serialised as JSON.
type Cache[V any] struct {
	client *redis.Client
	prefix string
	tracer trace.Tracer
	log    *slog.Logger
}

// New creates a Cache backed by the given Redis client.
//
//	c := rediscache.New[*Product](redisClient,
//	    rediscache.WithKeyPrefix("products:"),
//	)
func New[V any](client *redis.Client, opts ...Option[V]) *Cache[V] {
	o := &options[V]{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Cache[V]{
		client: client,
		prefix: o.prefix,
		tracer: otel.Tracer("github.com/miladhzz/gkit/pkg/rediscache"),
		log:    o.logger,
	}
}

// Get retrieves a value. Returns (zero, false, nil) on cache miss.
func (c *Cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	ctx, span := c.span(ctx, "GET", key)
	defer span.End()

	raw, err := c.client.Get(ctx, c.k(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		span.SetAttributes(attribute.Bool("cache.hit", false))
		var zero V
		return zero, false, nil
	}
	if err != nil {
		recordErr(span, err)
		var zero V
		return zero, false, fmt.Errorf("rediscache: get %q: %w", key, err)
	}

	span.SetAttributes(attribute.Bool("cache.hit", true))

	var v V
	if err := json.Unmarshal(raw, &v); err != nil {
		var zero V
		return zero, false, fmt.Errorf("rediscache: unmarshal %q: %w", key, err)
	}
	return v, true, nil
}

// Set stores a value with the given TTL. Pass 0 for no expiry.
func (c *Cache[V]) Set(ctx context.Context, key string, value V, ttl time.Duration) error {
	ctx, span := c.span(ctx, "SET", key)
	defer span.End()

	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("rediscache: marshal %q: %w", key, err)
	}
	if err := c.client.Set(ctx, c.k(key), raw, ttl).Err(); err != nil {
		recordErr(span, err)
		return fmt.Errorf("rediscache: set %q: %w", key, err)
	}
	return nil
}

// Delete removes a key. It is not an error if the key does not exist.
func (c *Cache[V]) Delete(ctx context.Context, key string) error {
	ctx, span := c.span(ctx, "DEL", key)
	defer span.End()

	if err := c.client.Del(ctx, c.k(key)).Err(); err != nil {
		recordErr(span, err)
		return fmt.Errorf("rediscache: del %q: %w", key, err)
	}
	return nil
}

// Flush removes all keys under the cache prefix. If no prefix is set, it
// flushes the entire Redis DB — use with caution.
func (c *Cache[V]) Flush(ctx context.Context) error {
	ctx, span := c.span(ctx, "FLUSH", "*")
	defer span.End()

	if c.prefix == "" {
		if err := c.client.FlushDB(ctx).Err(); err != nil {
			recordErr(span, err)
			return fmt.Errorf("rediscache: flush db: %w", err)
		}
		return nil
	}

	// SCAN + DEL in batches.
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = c.client.Scan(ctx, cursor, c.prefix+"*", 100).Result()
		if err != nil {
			recordErr(span, err)
			return fmt.Errorf("rediscache: scan: %w", err)
		}
		if len(keys) > 0 {
			if err := c.client.Del(ctx, keys...).Err(); err != nil {
				recordErr(span, err)
				return fmt.Errorf("rediscache: del batch: %w", err)
			}
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

// MGet retrieves multiple keys in a single round-trip.
// Missing keys return a zero value with ok=false in the result map.
func (c *Cache[V]) MGet(ctx context.Context, keys ...string) (map[string]V, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	ctx, span := c.span(ctx, "MGET", fmt.Sprintf("%d keys", len(keys)))
	defer span.End()

	prefixed := make([]string, len(keys))
	for i, k := range keys {
		prefixed[i] = c.k(k)
	}

	vals, err := c.client.MGet(ctx, prefixed...).Result()
	if err != nil {
		recordErr(span, err)
		return nil, fmt.Errorf("rediscache: mget: %w", err)
	}

	result := make(map[string]V, len(keys))
	for i, raw := range vals {
		if raw == nil {
			continue
		}
		var v V
		if err := json.Unmarshal([]byte(raw.(string)), &v); err != nil {
			c.log.Warn("rediscache: unmarshal in mget", "key", keys[i], "error", err)
			continue
		}
		result[keys[i]] = v
	}
	return result, nil
}

// Check implements health.Checker — pings Redis.
func (c *Cache[V]) Check(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("rediscache: ping: %w", err)
	}
	return nil
}

// --------------------------------------------------------------------------
// Options

type options[V any] struct {
	prefix string
	logger *slog.Logger
}

// Option configures a Cache.
type Option[V any] func(*options[V])

// WithKeyPrefix prepends prefix to every Redis key.
// Recommended for multi-tenant use or when sharing a Redis instance.
func WithKeyPrefix[V any](prefix string) Option[V] {
	return func(o *options[V]) { o.prefix = prefix }
}

// WithLogger sets the logger for internal warnings.
func WithLogger[V any](l *slog.Logger) Option[V] {
	return func(o *options[V]) { o.logger = l }
}

// --------------------------------------------------------------------------
// Helpers

func (c *Cache[V]) k(key string) string { return c.prefix + key }

func (c *Cache[V]) span(ctx context.Context, op, key string) (context.Context, trace.Span) {
	return c.tracer.Start(ctx, "redis."+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "redis"),
			attribute.String("db.operation", op),
			attribute.String("cache.key", key),
		),
	)
}

func recordErr(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// --------------------------------------------------------------------------
// NewClient is a convenience constructor for go-redis.

// ClientConfig holds Redis connection settings (loaded from env via pkg/config).
type ClientConfig struct {
	Addr     string        `env:"REDIS_ADDR"     default:"localhost:6379"`
	Password string        `env:"REDIS_PASSWORD" default:""`
	DB       int           `env:"REDIS_DB"       default:"0"`
	PoolSize int           `env:"REDIS_POOL"     default:"10"`
	Timeout  time.Duration `env:"REDIS_TIMEOUT"  default:"5s"`
}

// NewClient creates a go-redis client from a ClientConfig.
func NewClient(cfg ClientConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.Timeout,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
	})
}
