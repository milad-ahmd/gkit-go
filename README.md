# gkit

[![CI](https://github.com/miladhzz/gkit/actions/workflows/ci.yml/badge.svg)](https://github.com/miladhzz/gkit/actions)
[![Go Reference](https://pkg.go.dev/badge/github.com/miladhzz/gkit.svg)](https://pkg.go.dev/github.com/miladhzz/gkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/miladhzz/gkit)](https://goreportcard.com/report/github.com/miladhzz/gkit)
[![Go 1.22+](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)](https://golang.org/)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)

**gkit** is a collection of production-grade Go packages for building reliable, observable microservices.
Each package is independently importable, dependency-minimal, and designed for composability.

> **Also available for other runtimes:**
> - [gkit-java](../gkit-java) — Java 21 + Spring Boot port
> - [gkit-nestjs](../gkit-nestjs) — TypeScript + NestJS port

---

## Packages

| Package | Description |
|---------|-------------|
| [`retry`](#retry) | Generic retry with exponential backoff and jitter |
| [`pool`](#pool) | Generic bounded worker pool with backpressure and live stats |
| [`cache`](#cache) | Generic LRU cache with optional per-entry TTL |
| [`pubsub`](#pubsub) | Typed in-process publish/subscribe event bus |
| [`graceful`](#graceful) | LIFO graceful shutdown coordinator with signal handling |
| [`metrics`](#metrics) | Prometheus integration with pool/cache collectors |
| [`health`](#health) | Health check system with readiness/liveness HTTP handlers |
| [`rpc`](#rpc) | gRPC server/client builder with logging, recovery, and metrics interceptors |
| [`circuitbreaker`](#circuitbreaker) | Circuit breaker with Closed/Open/HalfOpen state machine |
| [`ratelimit`](#ratelimit) | Token bucket rate limiter with per-key variant and TTL eviction |
| [`middleware`](#middleware) | HTTP middleware chain (request ID, logging, recovery, metrics, timeout, rate limit) |
| [`pipeline`](#pipeline) | Generic concurrent pipeline with fan-out, chaining, and composition |
| [`otel`](#otel) | OpenTelemetry tracing with OTLP export and gRPC integration |
| [`sched`](#sched) | Job scheduler for periodic and deferred tasks |
| [`testutil`](#testutil) | Test helpers: Eventually, FreePort, Must, AssertNoError |
| [`config`](#config) | Struct-based env config loader with defaults, required fields, and .env files |
| [`store`](#store) | PostgreSQL layer (pgx pool, OTel tracing, transactions, Prometheus metrics, migrations) |
| [`rediscache`](#rediscache) | Redis-backed generic cache with OTel tracing and health check |
| [`outbox`](#outbox) | Transactional outbox pattern for reliable event publishing |
| [`lock`](#lock) | Distributed lock via Redis SETNX with auto-renewal keepalive |
| [`queue`](#queue) | Postgres-backed durable job queue with retries and dead-letter |
| [`auth`](#auth) | JWT middleware (HS256) + RBAC `Require(roles...)` |
| [`eventstore`](#eventstore) | Append-only event log in Postgres with optimistic concurrency and snapshots |
| [`saga`](#saga) | Saga orchestrator with LIFO compensating transactions |
| [`validation`](#validation) | Struct validation via field tags (required, min, max, email, url, oneof, regex) |
| [`feature`](#feature) | Redis-backed feature flags: global on/off, percentage rollout, allow-list |
| [`async`](#async) | **Goroutine & channel showcase**: Future, Stream, FanOut, FanIn, Tee, Semaphore, Debounce, Throttle, Barrier |

---

## Installation

```bash
go get github.com/miladhzz/gkit
```

Requires **Go 1.22+**.

---

## retry

Generic retry loop with pluggable backoff strategies and context propagation.

```go
import "github.com/miladhzz/gkit/pkg/retry"

user, err := retry.Do(ctx, func(ctx context.Context) (*User, error) {
    return db.FindUser(ctx, id)
},
    retry.WithMaxAttempts(5),
    retry.WithBackoff(retry.WithJitter(retry.ExponentialBackoff{
        Initial:    100 * time.Millisecond,
        Multiplier: 2.0,
        Max:        10 * time.Second,
    })),
    retry.WithOnRetry(func(ctx context.Context, attempt int, err error) {
        slog.WarnContext(ctx, "retrying", "attempt", attempt, "error", err)
    }),
)
```

- `Do[T]` — typed result, zero allocations on the happy path
- `Stop(err)` — immediately abort for non-retryable errors
- `ConstantBackoff`, `ExponentialBackoff`, `WithJitter` — composable strategies

---

## pool

Bounded generic worker pool with configurable queue depth, error callbacks, and atomic stats.

```go
import "github.com/miladhzz/gkit/pkg/pool"

p := pool.New[Job](8, func(ctx context.Context, job Job) error {
    return processJob(ctx, job)
},
    pool.WithQueueSize[Job](512),
    pool.WithOnError[Job](func(ctx context.Context, job Job, err error) {
        slog.Error("job failed", "error", err)
    }),
)
p.Start(ctx)
defer p.Stop()       // drains queue, waits for in-flight jobs

p.Submit(ctx, job)   // blocks until space available
p.TrySubmit(job)     // non-blocking; ErrQueueFull if full
```

---

## cache

Thread-safe generic LRU cache with optional TTL and background janitor.

```go
import "github.com/miladhzz/gkit/pkg/cache"

c := cache.New[string, *Product](10_000,
    cache.WithTTL[string, *Product](5 * time.Minute),
)
c.StartJanitor(ctx, 30*time.Second)

c.Set("p1", product)
if p, ok := c.Get("p1"); ok { ... }

stats := c.Stats() // hits, misses, evictions
```

---

## pubsub

Typed, in-process event bus. Each subscriber runs in its own goroutine.

```go
import "github.com/miladhzz/gkit/pkg/pubsub"

bus := pubsub.NewBus(slog.Default())

unsub := pubsub.Subscribe[OrderPlaced](bus, "orders.placed",
    func(ctx context.Context, e pubsub.Event[OrderPlaced]) error {
        return sendEmail(ctx, e.Payload)
    }, 32) // buffer size
defer unsub()

pubsub.Publish[OrderPlaced](bus, ctx, "orders.placed", OrderPlaced{ID: "123"})
```

---

## graceful

Shutdown coordinator — runs registered hooks in reverse order (LIFO) with a global timeout.

```go
import "github.com/miladhzz/gkit/pkg/graceful"

g := graceful.New(graceful.WithTimeout(15 * time.Second))
g.Register("database",    func(ctx context.Context) error { return db.Close() })
g.Register("http-server", func(ctx context.Context) error { return srv.Shutdown(ctx) })

// Blocks until SIGINT/SIGTERM; shuts down: http-server -> database
if err := g.ListenAndShutdown(ctx); err != nil { log.Fatal(err) }
```

---

## metrics

Prometheus integration with zero-copy snapshot collectors for pool and cache.

```go
import "github.com/miladhzz/gkit/pkg/metrics"

reg := metrics.NewRegistry("myapp") // pre-registers Go + process collectors

// Wire pool into Prometheus.
reg.MustRegister(metrics.NewPoolCollector("order_pool", func() metrics.PoolSnapshot {
    s := orderPool.Stats()
    return metrics.PoolSnapshot{Submitted: s.Submitted, Completed: s.Completed /* ... */}
}))

// Wire cache into Prometheus.
reg.MustRegister(metrics.NewCacheCollector("product_cache", func() metrics.CacheSnapshot {
    s := productCache.Stats()
    return metrics.CacheSnapshot{Hits: s.Hits, Misses: s.Misses /* ... */}
}))

// Custom metrics with namespace prefix.
reqs := reg.NewCounter(metrics.CounterOpts{Name: "http_requests_total", Help: "..."})
lat  := reg.NewHistogram(metrics.HistogramOpts{Name: "http_duration_seconds", Help: "..."})

http.Handle("/metrics", reg.Handler()) // OpenMetrics-compatible
```

---

## health

Health check system suitable for Kubernetes liveness and readiness probes.

```go
import "github.com/miladhzz/gkit/pkg/health"

h := health.New(health.WithTimeout(3 * time.Second))

h.Register("database", health.CheckerFunc(func(ctx context.Context) error {
    return db.PingContext(ctx)
}))
h.Register("order_pool", health.CheckerFunc(func(_ context.Context) error {
    if depth := orderPool.Stats().QueueDepth; depth > 200 {
        return fmt.Errorf("queue depth critical: %d", depth)
    }
    return nil
}))

mux.Handle("/healthz/ready", h.ReadyHandler()) // 200 OK / 503 Service Unavailable
mux.Handle("/healthz/live",  h.LiveHandler())  // always 200 (process is alive)
```

Response body:
```json
{
  "healthy": true,
  "checks": [
    {"name": "database",   "healthy": true},
    {"name": "order_pool", "healthy": true}
  ],
  "duration": "1.2ms"
}
```

---

## rpc

gRPC server and client builder with production-ready interceptor chains.

The `api/product/v1/` package provides a hand-written gRPC service (mirroring
`protoc-gen-go-grpc` output) and a JSON codec so no protobuf toolchain is
required to build or run the project.

```go
import (
    "github.com/miladhzz/gkit/pkg/rpc"
    "github.com/miladhzz/gkit/pkg/rpc/codec"
    "github.com/miladhzz/gkit/pkg/rpc/interceptors"
)

func init() { codec.Register() } // JSON-over-gRPC, no protoc required

rpcMetrics := interceptors.NewRPCMetrics(promReg)

srv := rpc.NewServer(
    rpc.WithReflection(), // enables grpcurl / grpcui
    rpc.WithUnaryInterceptors(
        interceptors.Recovery(logger),     // panic -> gRPC Internal
        interceptors.Logging(logger),      // structured slog per-RPC
        rpcMetrics.UnaryInterceptor(),     // Prometheus counters + histograms
    ),
    rpc.WithStreamInterceptors(
        interceptors.StreamRecovery(logger),
        interceptors.StreamLogging(logger),
        rpcMetrics.StreamInterceptor(),
    ),
)
productv1.RegisterProductServiceServer(srv.Server(), &myService{})
go srv.Serve(ctx, ":50051")
defer srv.GracefulStop()

// Client
conn, _ := rpc.Dial("localhost:50051")
client := productv1.NewProductServiceClient(conn)
```

Prometheus metrics exported:

| Metric | Type | Labels |
|--------|------|--------|
| `grpc_server_requests_total` | Counter | `method`, `code` |
| `grpc_server_request_duration_seconds` | Histogram | `method` |
| `grpc_server_requests_in_flight` | Gauge | `method` |

---

## circuitbreaker

Generic circuit breaker with Closed → Open → HalfOpen state machine.

```go
import "github.com/miladhzz/gkit/pkg/circuitbreaker"

cb := circuitbreaker.New("db",
    circuitbreaker.WithFailureThreshold(5),           // open after 5 consecutive failures
    circuitbreaker.WithSuccessThreshold(2),           // close after 2 successes in HalfOpen
    circuitbreaker.WithOpenTimeout(10 * time.Second), // HalfOpen probe interval
    circuitbreaker.WithOnStateChange(func(name string, from, to circuitbreaker.State) {
        slog.Warn("circuit state changed", "breaker", name, "from", from, "to", to)
    }),
)

product, err := circuitbreaker.Execute(ctx, cb, func(ctx context.Context) (*Product, error) {
    return db.GetProduct(ctx, id)
})
if errors.Is(err, circuitbreaker.ErrOpen) {
    // serve from cache or return degraded response
}
```

States:

| State | Behavior |
|-------|----------|
| `StateClosed` | All requests pass through; failures counted |
| `StateOpen` | All requests rejected immediately with `ErrOpen` |
| `StateHalfOpen` | One probe request allowed; success → Closed, failure → Open |

---

## ratelimit

Token bucket rate limiter with an optional per-key variant for per-user/per-IP limiting.

```go
import "github.com/miladhzz/gkit/pkg/ratelimit"

// Global limiter: 1000 req/s, burst of 100
lim := ratelimit.New(1000, 100)

if !lim.Allow() {
    http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
    return
}

// Wait for a token (context-aware)
if err := lim.Wait(ctx); err != nil { ... }

// Per-key limiter: separate bucket per IP address
keyed := ratelimit.NewKeyed[string](100, 10,
    ratelimit.WithKeyTTL[string](5 * time.Minute),
)
if !keyed.Allow(r.RemoteAddr) {
    http.Error(w, "rate limit exceeded", 429)
    return
}
keyed.Evict() // call periodically to reclaim memory for expired keys
```

---

## middleware

HTTP middleware chain for `net/http` servers. Middlewares are applied in declaration order (first declared = outermost).

```go
import "github.com/miladhzz/gkit/pkg/middleware"

httpMetrics := middleware.NewHTTPMetrics(promReg)
keyed       := ratelimit.NewKeyed[string](100, 10)

chain := middleware.Chain(
    middleware.RequestID(),             // injects X-Request-Id
    middleware.Logging(logger),         // structured access log
    middleware.Recovery(logger),        // panic → 500
    httpMetrics.Middleware(),           // Prometheus HTTP metrics
    middleware.Timeout(5*time.Second),  // per-request deadline
    middleware.RateLimit(func(r *http.Request) bool {
        return keyed.Allow(r.RemoteAddr)
    }),
)

mux := http.NewServeMux()
mux.HandleFunc("/products/{id}", getProduct)

srv := &http.Server{Handler: chain(mux)}
```

Prometheus metrics exported:

| Metric | Type | Labels |
|--------|------|--------|
| `http_requests_total` | Counter | `method`, `path`, `status` |
| `http_request_duration_seconds` | Histogram | `method`, `path` |
| `http_requests_in_flight` | Gauge | — |
| `http_response_bytes_total` | Counter | `method`, `path` |

Request ID is available via:
```go
id := middleware.RequestIDFromContext(r.Context())
```

---

## pipeline

Generic concurrent pipeline with fan-out worker pools and functional composition.

```go
import "github.com/miladhzz/gkit/pkg/pipeline"

// Fan-out: process N items concurrently with 4 workers
results, err := pipeline.Process(ctx, items, 4,
    func(ctx context.Context, item RawEvent) (ProcessedEvent, error) {
        return transform(ctx, item)
    },
)

// Chain: same type, multiple sequential stages
enriched, err := pipeline.Chain(ctx, events, 4,
    normalize,
    enrich,
    deduplicate,
)

// Compose: A → B → C with different types at each stage
final, err := pipeline.Compose(ctx, rawInputs, 4,
    parseJSON,    // []byte  → ParsedRecord
    validateFn,   // ParsedRecord → ValidRecord
)

// Compose3: A → B → C → D
out, err := pipeline.Compose3(ctx, inputs, workers, stage1, stage2, stage3)
```

All functions are fully generic, context-aware, and propagate errors correctly.

---

## otel

OpenTelemetry tracing setup with OTLP gRPC export and gRPC stats handler integration.

```go
import gkitotel "github.com/miladhzz/gkit/pkg/otel"

// Init from environment (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME, etc.)
tp, err := gkitotel.NewTracerProvider(ctx,
    gkitotel.WithServiceName("my-service"),
    gkitotel.WithServiceVersion("v1.2.0"),
    // OTLPEndpoint auto-read from OTEL_EXPORTER_OTLP_ENDPOINT env var,
    // or override:
    gkitotel.WithOTLPEndpoint("localhost:4317"),
)
if err != nil { log.Fatal(err) }
defer tp.Shutdown(ctx)

// Instrument gRPC server
srv := rpc.NewServer(
    gkitotel.NewServerHandler(), // stats.Handler for tracing + baggage
)

// Instrument gRPC client
conn, _ := rpc.Dial("localhost:50051",
    rpc.WithClientOptions(gkitotel.NewClientHandler()),
)

// Manual spans
ctx, span := gkitotel.StartSpan(ctx, "db.query")
defer span.End()
if err := db.Query(ctx); err != nil {
    gkitotel.RecordError(span, err)
}
```

Traces are exported to [Grafana Tempo](https://grafana.com/oss/tempo/) via OTLP gRPC.

---

## sched

Job scheduler for periodic and one-shot deferred tasks with bounded concurrency.

```go
import "github.com/miladhzz/gkit/pkg/sched"

s := sched.New(
    sched.WithMaxConcurrency(4),
    sched.WithOnError(func(name string, err error) {
        slog.Error("scheduled job failed", "job", name, "error", err)
    }),
    sched.WithLogger(slog.Default()),
)

// Periodic job
s.Every(30*time.Second, "cache-warmup", func(ctx context.Context) error {
    return warmCache(ctx)
})

// One-shot deferred job
s.After(5*time.Minute, "send-welcome-email", func(ctx context.Context) error {
    return mailer.Send(ctx, welcomeEmail)
})

s.Start(ctx)
defer s.Stop()
```

---

## testutil

Test helpers to reduce boilerplate in unit and integration tests.

```go
import "github.com/miladhzz/gkit/pkg/testutil"

// Assert condition is true within timeout (polls every 10ms)
testutil.Eventually(t, 2*time.Second, func() bool {
    return cache.Len() == 0
}, "cache should be empty after eviction")

// Fatal version
testutil.RequireEventually(t, time.Second, func() bool {
    return server.IsReady()
}, "server should become ready")

// Get a free TCP port for test servers
port := testutil.FreePort(t) // e.g. 54321

// Unwrap values / errors in one line
conn := testutil.MustVal(t, net.Listen("tcp", ":0"))
testutil.Must(t, db.Ping())

// Zero-noise error assertion
testutil.AssertNoError(t, srv.Shutdown(ctx))
```

---

## config

Zero-dependency struct-based configuration loader. Reads from environment
variables (and optionally a `.env` file) using struct field tags.

```go
import "github.com/miladhzz/gkit/pkg/config"

type Config struct {
    HTTP struct {
        Addr    string        `env:"HTTP_ADDR"    default:":8080"`
        Timeout time.Duration `env:"HTTP_TIMEOUT" default:"30s"`
    }
    DB struct {
        DSN      string `env:"DB_DSN"      required:"true"`
        MaxConns int32  `env:"DB_MAX_CONS" default:"20"`
    }
    Redis struct {
        Addr     string `env:"REDIS_ADDR"     default:"localhost:6379"`
        Password string `env:"REDIS_PASSWORD" default:""`
    }
    Debug bool     `env:"DEBUG" default:"false"`
    Tags  []string `env:"TAGS"  default:"api,web"` // comma-separated
}

var cfg Config
config.MustLoad(&cfg,
    config.WithEnvFile(".env"), // optional; process env wins
)
```

Supported types: `string`, `bool`, `int*`, `uint*`, `float*`,
`time.Duration`, `[]string` (comma-separated), `url.URL`, `net.IP`,
and pointer variants of all the above.

---

## store

Production PostgreSQL layer built on `pgx/v5`:
- Automatic OTel span per query (SQL statement as span attribute)
- Prometheus pool metrics (total/idle/used connections, acquire count)
- `WithTx` — nested transaction support via context propagation
- `MigrateUp/Down/Steps` — schema migrations via `golang-migrate`
- `Check(ctx)` implements `health.Checker`

```go
import "github.com/miladhzz/gkit/pkg/store"

db, err := store.Open(ctx, cfg.DB.DSN,
    store.WithMaxConns(20),
    store.WithPrometheus(promRegistry),
    store.WithLogger(slog.Default()),
)
defer db.Close()

// Run pending migrations from ./migrations directory.
if err := store.MigrateUp(cfg.DB.DSN, "file://migrations"); err != nil {
    log.Fatal(err)
}

// Plain query.
rows, err := db.Query(ctx, "SELECT id, name FROM products WHERE active = $1", true)

// Transaction — auto-commits or rolls back, supports nesting via context.
err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
    if _, err := tx.Exec(ctx,
        "INSERT INTO orders (product_id, qty) VALUES ($1, $2)", productID, qty); err != nil {
        return err
    }
    // Store an outbox event in the same transaction.
    return outbox.Store(ctx, tx, "orders.placed", OrderPlaced{ID: orderID})
})

// Health check (register with pkg/health).
h.Register("postgres", health.CheckerFunc(db.Check))
```

Migration files follow `golang-migrate` naming convention:

```
migrations/
  000001_outbox.up.sql
  000001_outbox.down.sql
  000002_products.up.sql
  000002_products.down.sql
```

---

## rediscache

Generic Redis-backed cache with JSON serialization, OTel tracing, and
`health.Checker`. Mirrors the `pkg/cache` API so the two are interchangeable.

```go
import "github.com/miladhzz/gkit/pkg/rediscache"

client := rediscache.NewClient(rediscache.ClientConfig{
    Addr:    cfg.Redis.Addr,
    Timeout: 5 * time.Second,
})

c := rediscache.New[*Product](client,
    rediscache.WithKeyPrefix[*Product]("products:"),
)

// Get / Set / Delete
if err := c.Set(ctx, "p1", product, 5*time.Minute); err != nil { ... }

p, ok, err := c.Get(ctx, "p1")  // (value, hit, error)

// Batch get in one round-trip
products, err := c.MGet(ctx, "p1", "p2", "p3")

// Flush all keys under the prefix
_ = c.Flush(ctx)

// Health check
h.Register("redis", health.CheckerFunc(c.Check))
```

---

## outbox

Transactional outbox pattern — guarantees at-least-once event delivery even
when the service crashes between the DB write and the broker publish.

**How it works:**

1. `outbox.Store` writes the event into `outbox_events` in the **same
   database transaction** as your business data. If the transaction rolls
   back, the event is also rolled back — atomicity guaranteed.
2. `Relay` polls `outbox_events` for unpublished rows, delivers each to a
   `Publisher`, and marks them published — all in a single transaction using
   `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent operation.

```go
import "github.com/miladhzz/gkit/pkg/outbox"

// 1. Write event in the same tx as your domain data.
err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
    if err := orderRepo.Save(ctx, tx, order); err != nil {
        return err
    }
    return outbox.Store(ctx, tx, "orders.placed", order)
})

// 2. Start the relay (wire to any publisher: pubsub.Bus, Redis Streams, etc.)
type busPublisher struct{ bus *pubsub.Bus }
func (b *busPublisher) Publish(ctx context.Context, topic string, raw []byte) error {
    var order Order
    _ = json.Unmarshal(raw, &order)
    return pubsub.Publish[Order](b.bus, ctx, topic, order)
}

relay := outbox.NewRelay(db, &busPublisher{bus},
    outbox.WithRelayInterval(2 * time.Second),
    outbox.WithBatchSize(50),
    outbox.WithOnError(func(err error) { slog.Error("outbox", "err", err) }),
)
relay.Start(ctx)
defer relay.Stop()
```

Required schema (included in `migrations/000001_outbox.up.sql`):

```sql
CREATE TABLE outbox_events (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_events_unpublished ON outbox_events (created_at)
    WHERE published_at IS NULL;
```

---

## lock

Distributed lock backed by Redis. Uses `SET key token PX ttl NX` for atomic
acquisition and a Lua `CAS-DEL` script for safe release. A background goroutine
auto-renews the TTL at `ttl/3` intervals so the lock never expires under a
live holder.

```go
import "github.com/miladhzz/gkit/pkg/lock"

locker := lock.New(redisClient,
    lock.WithRetry(10, 100*time.Millisecond), // retry up to 10× if held
)

// One-liner: acquire → run → release.
err := locker.WithLock(ctx, "billing:invoice:123", 30*time.Second, func(ctx context.Context) error {
    return processInvoice(ctx)
})
if errors.Is(err, lock.ErrNotAcquired) { /* someone else holds it */ }

// Manual lifecycle.
lk, err := locker.Acquire(ctx, "report:monthly", 30*time.Second)
defer lk.Release(ctx)
```

---

## queue

Postgres-backed durable job queue. Uses `SELECT FOR UPDATE SKIP LOCKED` so
multiple worker processes can poll safely without double-delivery. Failed jobs
are retried with exponential backoff; jobs exceeding `MaxAttempts` go to a
`dead` state for inspection.

```go
import "github.com/miladhzz/gkit/pkg/queue"

q := queue.New(db, queue.WithPollInterval(2*time.Second))

// Register handlers.
q.Register("send-email", func(ctx context.Context, p queue.Payload) error {
    var job EmailJob
    if err := p.Decode(&job); err != nil { return err }
    return mailer.Send(ctx, job)
})

// Enqueue atomically alongside domain writes.
err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
    if err := orderRepo.Save(ctx, tx, order); err != nil { return err }
    return q.Enqueue(ctx, tx, "send-email", EmailJob{To: order.Email},
        queue.WithMaxAttempts(5),
        queue.WithDelay(5*time.Second),
    )
})

q.Start(ctx, 4) // 4 concurrent workers
defer q.Stop()
```

Job status lifecycle: `pending → running → done` (or `failed` → retry → `dead`).

---

## auth

JWT authentication middleware and RBAC for `net/http`. Tokens are HS256-signed.
Claims are injected into the request context for downstream handlers.

```go
import "github.com/miladhzz/gkit/pkg/auth"

secret := []byte(os.Getenv("JWT_SECRET"))

// Issue a token (login endpoint).
token, err := auth.IssueToken(auth.Claims{
    UserID: user.ID,
    Roles:  []string{"admin", "user"},
}, secret, 24*time.Hour)

// Protect routes.
mux.Handle("/api/", auth.JWT(secret)(apiHandler))
mux.Handle("/admin/", auth.JWT(secret)(auth.Require("admin")(adminHandler)))

// Read claims in a handler.
func handler(w http.ResponseWriter, r *http.Request) {
    claims, ok := auth.ClaimsFromContext(r.Context())
    if !ok { http.Error(w, "unauthorized", 401); return }
    fmt.Fprintf(w, "hello %s, roles: %v", claims.UserID, claims.Roles)
}
```

Also supports `access_token` cookie as fallback for browser clients.

---

## eventstore

Append-only event log in Postgres for event sourcing. Optimistic concurrency
control prevents split-brain writes. Snapshots allow fast aggregate
reconstruction without replaying the full history.

```go
import "github.com/miladhzz/gkit/pkg/eventstore"

es := eventstore.New(db)

// Append (ExpectedVersionNew = stream must not exist yet).
err = es.Append(ctx, "order-"+id, []eventstore.EventData{
    {Type: "OrderPlaced",  Data: OrderPlaced{ID: id, Total: 99.5}},
    {Type: "StockReserved", Data: StockReserved{SKU: "ABC"}},
}, eventstore.ExpectedVersionNew)

if errors.Is(err, eventstore.ErrVersionConflict) {
    // Concurrent write — retry with reload.
}

// Load all events from the beginning.
events, err := es.Load(ctx, "order-"+id, 0)
for _, e := range events {
    var payload OrderPlaced
    _ = e.Decode(&payload)
}

// Snapshot + incremental load.
snap, _ := es.LoadSnapshot(ctx, "order-"+id)
fromVersion := int64(0)
if snap != nil { fromVersion = snap.Version + 1 }
tail, _ := es.Load(ctx, "order-"+id, fromVersion)

// Save a snapshot every 50 events.
if len(events) % 50 == 0 {
    _ = es.SaveSnapshot(ctx, "order-"+id, latestVersion, aggregateState)
}
```

---

## saga

Saga orchestrator for distributed transactions. Steps execute in order; on
failure, previously completed steps are compensated in **reverse order** (LIFO).

```go
import "github.com/miladhzz/gkit/pkg/saga"

s := saga.New("place-order",
    saga.Step{
        Name:       "reserve-inventory",
        Execute:    func(ctx context.Context) error { return inventory.Reserve(ctx, item) },
        Compensate: func(ctx context.Context) error { return inventory.Release(ctx, item) },
    },
    saga.Step{
        Name:       "charge-payment",
        Execute:    func(ctx context.Context) error { return payments.Charge(ctx, amount) },
        Compensate: func(ctx context.Context) error { return payments.Refund(ctx, amount) },
    },
    saga.Step{
        Name:    "send-confirmation",
        Execute: func(ctx context.Context) error { return email.Send(ctx, order) },
        // No Compensate — best-effort step.
    },
)

if err := s.Run(ctx); err != nil {
    var se *saga.Error
    if errors.As(err, &se) {
        log.Printf("failed at %q: %v", se.FailedStep, se.Cause)
        log.Printf("compensation errors: %v", se.CompensationErrors)
    }
}
```

---

## validation

Struct validation via `validate` field tags. Returns a `*validation.Error` with
a `Fields` map of field name → rule failures, suitable for JSON API error responses.

```go
import "github.com/miladhzz/gkit/pkg/validation"

type CreateOrderRequest struct {
    ProductID string  `json:"product_id" validate:"required"`
    Quantity  int     `json:"quantity"   validate:"required,min=1,max=1000"`
    Email     string  `json:"email"      validate:"required,email"`
    Status    string  `json:"status"     validate:"oneof=pending|active|cancelled"`
    Code      string  `json:"code"       validate:"regex=^[A-Z]{3}[0-9]{3}$"`
}

v := validation.New()
if err := v.Validate(&req); err != nil {
    var ve *validation.Error
    if errors.As(err, &ve) {
        // {"fields":{"email":["must be a valid email address"],...}}
        respondJSON(w, 422, ve)
        return
    }
}
```

Supported rules: `required`, `min=N`, `max=N`, `email`, `url`, `oneof=a|b|c`, `regex=pattern`.

---

## feature

Redis-backed runtime feature flags. Three modes: global on/off, percentage
rollout (stable hash-based), and explicit allow-list. Fails safe (returns false)
on any Redis error.

```go
import "github.com/miladhzz/gkit/pkg/feature"

store := feature.NewStore(redisClient, feature.WithNamespace("myapp"))

// Create / update a flag (admin panel, migration, CLI tool, etc.).
_ = store.Set(ctx, "dark-mode", feature.Flag{
    Enabled:    true,
    Percentage: 20,                         // 20% of users
    AllowList:  []string{"u1", "u2"},        // always on for these users
})

// Check in a handler — fail-safe (false on error).
if store.IsEnabledFor(ctx, "dark-mode", userID) {
    renderDarkTheme(w)
}

// Global flag (no entity).
if store.IsEnabled(ctx, "maintenance-mode") {
    http.Error(w, "maintenance", 503)
    return
}

// List all flags.
flags, _ := store.ListAll(ctx)
```

---

## async

**Expert-level goroutine and channel patterns.** Every primitive is built
directly on goroutines and channels — no sync.Mutex where a channel suffices.

### Future[T] — asynchronous value

```go
import "github.com/miladhzz/gkit/pkg/async"

// Spawn N concurrent operations.
userFuture    := async.Async(ctx, func(ctx context.Context) (*User,    error) { return db.GetUser(ctx, id) })
profileFuture := async.Async(ctx, func(ctx context.Context) (*Profile, error) { return db.GetProfile(ctx, id) })
ordersFuture  := async.Async(ctx, func(ctx context.Context) ([]*Order, error) { return db.ListOrders(ctx, id) })

// Await all results concurrently — first error cancels the rest.
results, err := async.All(ctx, userFuture, profileFuture) // typed per-future

// Race — first response wins, others are cancelled.
fastest, err := async.Race(ctx, primary, replica1, replica2)
```

### Stream[T] — lazy push-based pipeline

```go
// Build a typed pipeline of goroutines connected by channels.
stream := async.Generate(ctx, func(_ context.Context, send func(Event) bool) {
    for event := range kafkaConsumer.Messages() {
        if !send(event) { return }
    }
})

processed := async.Map(stream, normalize)           // goroutine + channel
filtered  := async.Filter(processed, isRelevant)    // goroutine + channel
batched   := async.Batch(filtered, 100, time.Second) // goroutine + channel

batched.ForEach(ctx, func(batch []Event) error {
    return db.BulkInsert(ctx, batch)
})
```

### Channel combinators

```go
// FanOut — broadcast one channel to N independent receivers.
// Pattern: one goroutine writes to N buffered channels.
outs := async.FanOut(ctx, eventCh, 3) // 3 consumers, each gets every event

// FanIn — merge N channels into one.
// Pattern: one goroutine per input, WaitGroup closes output.
merged := async.FanIn(ctx, workerA, workerB, workerC)

// Tee — duplicate a stream without blocking either side.
a, b := async.Tee(ctx, resultCh)

// OrDone — range over a channel with context cancellation.
for event := range async.OrDone(ctx, events) {
    process(event)
}
```

### Semaphore — channel as permit pool

```go
// The channel buffer IS the semaphore — no mutex needed.
sem := async.NewSemaphore(10) // 10 concurrent DB connections

for _, item := range items {
    item := item
    go func() {
        _ = sem.Acquire(ctx) // blocks when 10 goroutines are inside
        defer sem.Release()
        processItem(ctx, item)
    }()
}
```

### Debounce & Throttle

```go
// Debounce — group rapid calls, fire once after quiet period.
// Pattern: goroutine resets a timer on every call.
save := async.Debounce(500*time.Millisecond, func() { db.Save(state) })
// User types: save() called 50× in 2s → db.Save called once.

// Throttle — at most one call per window.
// Pattern: time.Ticker replenishes a buffered permit channel.
flush := async.Throttle(time.Second, func() { sink.Flush() })
```

### Barrier — goroutine rendezvous

```go
// All N goroutines block at Wait until the last one arrives.
b := async.NewBarrier(workers)
for i := range workers {
    go func() {
        prepareWork(i)
        _ = b.Wait(ctx) // blocks until all workers are ready
        startWork(i)     // all start simultaneously
    }()
}
```

---

## Example: Full Service

[`examples/server/`](examples/server/main.go) integrates **all packages** into a single production-ready service:

- **HTTP** `:8080` — product lookup (cached + circuit-broken), order placement, `/metrics`, `/healthz/*`
- **gRPC** `:50051` — `ProductService` with OTel tracing, metrics, logging, and recovery interceptors
- **Circuit breaker** — wraps DB calls; falls back to cache when open
- **Per-IP rate limiting** — token bucket via `ratelimit.NewKeyed[string]`
- **Middleware chain** — RequestID → Logging → Recovery → Prometheus → Timeout → RateLimit
- **Worker pool** — 4-worker async order processor with Prometheus instrumentation
- **LRU cache** — 512-slot product cache with 5-minute TTL
- **Event bus** — decoupled order event delivery via pubsub
- **Scheduler** — cache warmup every 30s, stats logging every 60s
- **OTel tracing** — OTLP to Grafana Tempo (configured via `OTEL_EXPORTER_OTLP_ENDPOINT`)
- **Graceful shutdown** — SIGINT/SIGTERM → drain pool → stop gRPC → stop HTTP

```bash
go run ./examples/server

# Products
curl http://localhost:8080/products/p1

# Orders
curl -XPOST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id":"p1","quantity":2}'

# Prometheus metrics
curl http://localhost:8080/metrics | grep gkit

# Health probes
curl http://localhost:8080/healthz/ready
curl http://localhost:8080/healthz/live

# gRPC (requires grpcurl: brew install grpcurl)
grpcurl -plaintext -d '{"id":"p1"}' \
  localhost:50051 product.v1.ProductService/GetProduct
```

---

## Observability Stack (Docker)

Run the full stack — app, Prometheus, Grafana, and Grafana Tempo — with one command:

```bash
docker compose up --build
```

| Service | URL | Credentials |
|---------|-----|-------------|
| App metrics | http://localhost:8080/metrics | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Tempo (OTLP gRPC) | localhost:4317 | — |
| PostgreSQL | localhost:5432 | gkit / gkit / gkit |
| Redis | localhost:6379 | — |

Integration tests against the live stack:

```bash
TEST_POSTGRES_DSN="postgres://gkit:gkit@localhost:5432/gkit?sslmode=disable" \
TEST_REDIS_ADDR="localhost:6379" \
  go test -race -count=1 -timeout=60s ./pkg/store/... ./pkg/rediscache/... ./pkg/outbox/...
```

The Grafana instance comes pre-provisioned with:
- **Prometheus** and **Tempo** datasources wired up
- A **gkit dashboard** showing HTTP/gRPC rates, latency percentiles, pool throughput, cache hit rate, and Go runtime metrics
- **Trace correlation** — click any latency spike in Grafana to jump to the trace in Tempo

---

## Development

```bash
make test      # all tests with race detector
make bench     # benchmarks with memory allocation report
make cover     # HTML coverage report
make vet       # go vet
make lint      # golangci-lint (install: https://golangci-lint.run)
make proto     # regenerate from .proto (requires protoc + plugins)
```

### Benchmark results (Apple M4 Pro)

| Package | Benchmark | ns/op | B/op | allocs/op |
|---------|-----------|-------|------|-----------|
| cache | SetGet | 22 | 0 | 0 |
| pool | Throughput | 113 | 0 | 0 |
| retry | NoRetry | 13 | 48 | 1 |

---

## Design Principles

1. **Generics over `interface{}`** — type safety without reflection overhead
2. **Context-first** — every blocking operation accepts `context.Context`
3. **Options pattern** — sensible defaults, extensible without breaking changes
4. **Composability** — packages don't depend on each other
5. **Observable by default** — Prometheus metrics and structured slog built in
6. **Testability** — interfaces everywhere; no global state
7. **Graceful degradation** — circuit breaker + fallback patterns throughout

---

## License

[MIT](LICENSE)
