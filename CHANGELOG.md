# Changelog

All notable changes to **gkit** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Integration tests with Testcontainers for `store`, `rediscache`, `lock`, `queue`, `outbox`, `eventstore`
- Benchmarks for `cache`, `ratelimit`, `retry`, `pipeline`, `circuitbreaker`
- Community health files: `CONTRIBUTING.md`, `SECURITY.md`, `CODEOWNERS`
- GitHub issue and PR templates

---

## [1.0.0] — 2024-03-19

### Added

**Core concurrency**
- `pkg/retry` — generic retry with fixed, exponential, and jitter backoff strategies
- `pkg/pool` — bounded goroutine worker pool with backpressure and drain
- `pkg/cache` — generic in-memory LRU cache with optional per-entry TTL
- `pkg/async` — `Future[T]`, `Semaphore`, and lazy `Stream[T]` primitives
- `pkg/pipeline` — concurrent fan-out pipeline with stage chaining and composition
- `pkg/pubsub` — typed in-process publish/subscribe event bus

**Reliability**
- `pkg/circuitbreaker` — Closed/Open/HalfOpen state machine with configurable thresholds
- `pkg/ratelimit` — token-bucket rate limiter with per-key variant
- `pkg/graceful` — LIFO ordered shutdown coordinator with per-hook timeout
- `pkg/health` — concurrent health-check group with per-checker timeout
- `pkg/saga` — saga orchestrator with LIFO compensation

**Infrastructure**
- `pkg/store` — PostgreSQL layer via pgx/v5 with OTel tracing and Prometheus metrics
- `pkg/rediscache` — Redis-backed cache with JSON serialisation and `GetOrSet`
- `pkg/lock` — Redis distributed lock with Lua-based release and renewal
- `pkg/queue` — Postgres-backed job queue with `SKIP LOCKED` and dead-letter
- `pkg/outbox` — transactional outbox pattern with background relay
- `pkg/eventstore` — append-only Postgres event store with version-checked appends
- `pkg/rpc` — gRPC server and client builders with interceptors and TLS

**Cross-cutting**
- `pkg/auth` — JWT issuance and validation with RBAC helpers
- `pkg/metrics` — Prometheus registry with typed counter, gauge, histogram, summary
- `pkg/middleware` — HTTP middleware chain (request ID, logging, recovery, timeout)
- `pkg/otel` — OpenTelemetry SDK setup with OTLP export
- `pkg/config` — environment-variable config loader with type coercion
- `pkg/feature` — feature flags with percentage rollout, allowlist, and env-var loading
- `pkg/sched` — cron-style job scheduler with fixed-rate and one-shot execution
- `pkg/validation` — fluent field-level validator with built-in rules
- `pkg/testutil` — test helpers

[Unreleased]: https://github.com/miladhzz/gkit/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/miladhzz/gkit/releases/tag/v1.0.0
