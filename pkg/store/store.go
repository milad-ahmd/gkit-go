// Package store provides a PostgreSQL database layer built on pgx/v5.
//
// Features:
//   - pgxpool connection pool with configurable limits
//   - OpenTelemetry query tracing (every SQL statement gets a span)
//   - Prometheus pool metrics (acquire count, idle conns, total conns, etc.)
//   - Clean transaction helper: WithTx(ctx, fn)
//   - Schema migrations via golang-migrate
//   - health.Checker implementation
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// DB wraps a pgxpool.Pool with tracing, metrics, and helpers.
type DB struct {
	pool   *pgxpool.Pool
	tracer trace.Tracer
	log    *slog.Logger
}

// Open creates a new DB, validates the connection, and returns it.
func Open(ctx context.Context, dsn string, opts ...Option) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse DSN: %w", err)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if o.maxConns > 0 {
		cfg.MaxConns = o.maxConns
	}
	if o.minConns > 0 {
		cfg.MinConns = o.minConns
	}

	db := &DB{
		tracer: otel.Tracer("github.com/miladhzz/gkit/pkg/store"),
		log:    o.logger,
	}

	// Attach OTel query tracer.
	cfg.ConnConfig.Tracer = &otelQueryTracer{tracer: db.tracer}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}

	db.pool = pool

	if o.promReg != nil {
		if err := o.promReg.Register(newPoolCollector(pool)); err != nil {
			pool.Close()
			return nil, fmt.Errorf("store: register metrics: %w", err)
		}
	}

	db.log.Info("store: connected to postgres", "dsn_masked", maskDSN(dsn))
	return db, nil
}

// Close shuts down the connection pool.
func (db *DB) Close() { db.pool.Close() }

// Pool returns the underlying pgxpool.Pool for advanced use.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Ping verifies the database is reachable (implements health.Checker).
func (db *DB) Ping(ctx context.Context) error {
	return db.pool.Ping(ctx)
}

// Check implements health.Checker.
func (db *DB) Check(ctx context.Context) error {
	return db.Ping(ctx)
}

// --------------------------------------------------------------------------
// Query helpers

// Exec runs a query that returns no rows.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) error {
	ctx, span := db.startSpan(ctx, "exec", sql)
	defer span.End()
	_, err := db.pool.Exec(ctx, sql, args...)
	recordSpanError(span, err)
	return err
}

// QueryRow runs a query that returns exactly one row.
func (db *DB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx, span := db.startSpan(ctx, "query_row", sql)
	// Span ends when the row is scanned; best-effort close.
	defer span.End()
	return db.pool.QueryRow(ctx, sql, args...)
}

// Query runs a query that returns multiple rows.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx, span := db.startSpan(ctx, "query", sql)
	defer span.End()
	rows, err := db.pool.Query(ctx, sql, args...)
	recordSpanError(span, err)
	return rows, err
}

// --------------------------------------------------------------------------
// Transactions

// Tx is a thin wrapper around pgx.Tx.
type Tx struct {
	pgx.Tx
	tracer trace.Tracer
}

// WithTx runs fn inside a transaction. It commits on nil return, rolls back
// otherwise. Nested calls detect an existing tx via context and re-use it.
func (db *DB) WithTx(ctx context.Context, fn func(context.Context, *Tx) error) error {
	// Re-use existing tx if one is already active.
	if tx, ok := txFromContext(ctx); ok {
		return fn(ctx, tx)
	}

	ctx, span := db.tracer.Start(ctx, "db.transaction",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	pgxTx, err := db.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		recordSpanError(span, err)
		return fmt.Errorf("store: begin tx: %w", err)
	}

	tx := &Tx{Tx: pgxTx, tracer: db.tracer}
	ctx = contextWithTx(ctx, tx)

	defer func() {
		if p := recover(); p != nil {
			_ = pgxTx.Rollback(ctx)
			span.RecordError(fmt.Errorf("panic: %v", p))
			span.SetStatus(codes.Error, "panic in transaction")
			panic(p) // re-throw
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := pgxTx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			db.log.Error("store: rollback failed", "error", rbErr)
		}
		recordSpanError(span, err)
		return err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		recordSpanError(span, err)
		return fmt.Errorf("store: commit: %w", err)
	}

	return nil
}

// --------------------------------------------------------------------------
// OTel tracer for pgx

type spanKey struct{}

type otelQueryTracer struct {
	tracer trace.Tracer
}

func (t *otelQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx, span := t.tracer.Start(ctx, "db.query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemPostgreSQL,
			attribute.String("db.statement", data.SQL),
		),
	)
	return context.WithValue(ctx, spanKey{}, span)
}

func (t *otelQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(spanKey{}).(trace.Span)
	if !ok {
		return
	}
	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
	}
	span.End()
}

// --------------------------------------------------------------------------
// Context tx helpers

type txCtxKey struct{}

func contextWithTx(ctx context.Context, tx *Tx) context.Context {
	return context.WithValue(ctx, txCtxKey{}, tx)
}

func txFromContext(ctx context.Context) (*Tx, bool) {
	tx, ok := ctx.Value(txCtxKey{}).(*Tx)
	return tx, ok
}

// --------------------------------------------------------------------------
// Helpers

func (db *DB) startSpan(ctx context.Context, op, sql string) (context.Context, trace.Span) {
	return db.tracer.Start(ctx, "db."+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemPostgreSQL,
			attribute.String("db.statement", sql),
		),
	)
}

func recordSpanError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func maskDSN(dsn string) string {
	// Hide the password portion for logging.
	u, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return "<unparseable>"
	}
	host := u.ConnConfig.Host
	db := u.ConnConfig.Database
	user := u.ConnConfig.User
	return fmt.Sprintf("postgres://%s@%s/%s", user, host, db)
}
