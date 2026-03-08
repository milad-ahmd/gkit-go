// Package otel provides OpenTelemetry setup helpers and gRPC instrumentation
// for gkit services.
//
// It wires together the OTel SDK, OTLP exporter (for Jaeger, Tempo, etc.),
// and a stdout exporter for local development — with a single call:
//
//	tp, err := otel.NewTracerProvider(ctx,
//	    otel.WithServiceName("my-service"),
//	    otel.WithOTLPEndpoint("localhost:4317"),
//	)
//	if err != nil { log.Fatal(err) }
//	defer tp.Shutdown(ctx)
//
// For gRPC, attach the stats handler to the server/client:
//
//	srv := grpc.NewServer(otel.NewServerHandler())
//	conn, _ := grpc.NewClient(addr, otel.NewClientHandler())
package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// TracerProvider wraps the OTel SDK TracerProvider with a Shutdown method
// that flushes pending spans.
type TracerProvider struct {
	tp *sdktrace.TracerProvider
}

// Shutdown flushes and shuts down the underlying TracerProvider.
// Always defer this after creating a TracerProvider.
func (p *TracerProvider) Shutdown(ctx context.Context) error {
	return p.tp.Shutdown(ctx)
}

// Tracer returns a named tracer backed by this provider.
func (p *TracerProvider) Tracer(name string) oteltrace.Tracer {
	return p.tp.Tracer(name)
}

// config holds setup options.
type config struct {
	serviceName    string
	serviceVersion string
	environment    string
	otlpEndpoint   string // empty → stdout exporter only
	sampler        sdktrace.Sampler
	extraAttrs     []attribute.KeyValue
}

// ProviderOption configures a TracerProvider.
type ProviderOption func(*config)

// WithServiceName sets the service.name resource attribute.
func WithServiceName(name string) ProviderOption {
	return func(c *config) { c.serviceName = name }
}

// WithServiceVersion sets the service.version resource attribute.
func WithServiceVersion(v string) ProviderOption {
	return func(c *config) { c.serviceVersion = v }
}

// WithEnvironment sets the deployment.environment attribute (e.g. "production").
func WithEnvironment(env string) ProviderOption {
	return func(c *config) { c.environment = env }
}

// WithOTLPEndpoint configures the gRPC OTLP exporter endpoint (e.g. "localhost:4317").
// When set, spans are exported to this endpoint in addition to stdout (dev mode).
// When empty (default), only stdout export is used.
func WithOTLPEndpoint(addr string) ProviderOption {
	return func(c *config) { c.otlpEndpoint = addr }
}

// WithSampler overrides the default AlwaysSample sampler.
func WithSampler(s sdktrace.Sampler) ProviderOption {
	return func(c *config) { c.sampler = s }
}

// WithAttributes adds extra resource attributes.
func WithAttributes(attrs ...attribute.KeyValue) ProviderOption {
	return func(c *config) { c.extraAttrs = append(c.extraAttrs, attrs...) }
}

// NewTracerProvider initialises the global OTel TracerProvider and propagator.
// Call Shutdown when the process exits to flush pending spans.
func NewTracerProvider(ctx context.Context, opts ...ProviderOption) (*TracerProvider, error) {
	cfg := &config{
		serviceName: "gkit-service",
		sampler:     sdktrace.AlwaysSample(),
	}
	for _, o := range opts {
		o(cfg)
	}

	// Build resource attributes.
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.serviceName),
	}
	if cfg.serviceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.serviceVersion))
	}
	if cfg.environment != "" {
		attrs = append(attrs, attribute.String("deployment.environment", cfg.environment))
	}
	attrs = append(attrs, cfg.extraAttrs...)

	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithProcess(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: build resource: %w", err)
	}

	// Always add stdout exporter (great for dev/debug).
	stdExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("otel: stdout exporter: %w", err)
	}

	spanProcessors := []sdktrace.SpanProcessor{
		sdktrace.NewBatchSpanProcessor(stdExporter),
	}

	// Optionally add OTLP exporter for production (Jaeger / Grafana Tempo).
	if cfg.otlpEndpoint != "" {
		otlpExporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.otlpEndpoint),
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithTimeout(5*time.Second),
		)
		if err != nil {
			return nil, fmt.Errorf("otel: OTLP exporter: %w", err)
		}
		spanProcessors = append(spanProcessors, sdktrace.NewBatchSpanProcessor(otlpExporter))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(cfg.sampler),
		sdktrace.WithResource(res),
	)
	for _, sp := range spanProcessors {
		tp.RegisterSpanProcessor(sp)
	}

	// Set as global provider + propagator (W3C TraceContext + Baggage).
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &TracerProvider{tp: tp}, nil
}

// ---- Span helpers -------------------------------------------------------

// StartSpan starts a new span using the global tracer and returns the child context.
func StartSpan(ctx context.Context, name string, opts ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	return otel.Tracer("gkit").Start(ctx, name, opts...)
}

// RecordError marks span as errored and records err as an event.
func RecordError(span oteltrace.Span, err error) {
	if err != nil {
		span.RecordError(err)
	}
}

// ---- gRPC instrumentation -----------------------------------------------

// NewServerHandler returns a gRPC stats.Handler that traces all server RPCs.
// Pass it as grpc.StatsHandler(otel.NewServerHandler()) when building a server.
//
//	srv := grpc.NewServer(grpc.StatsHandler(otel.NewServerHandler()))
func NewServerHandler(opts ...otelgrpc.Option) grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler(opts...))
}

// NewClientHandler returns a gRPC stats.Handler that traces all client RPCs.
// Pass it as grpc.WithStatsHandler(otel.NewClientHandler()) when dialling.
//
//	conn, _ := grpc.NewClient(addr, grpc.WithStatsHandler(otel.NewClientHandler()))
func NewClientHandler(opts ...otelgrpc.Option) grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler(opts...))
}
