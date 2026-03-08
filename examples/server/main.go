// Package main demonstrates how gkit packages compose into a production-ready service:
//
//   - HTTP API with middleware chain (request ID, logging, recovery, metrics, rate limiting)
//   - gRPC API with interceptors (logging, recovery, Prometheus metrics, OTel tracing)
//   - Circuit breaker protecting the database fallback
//   - Per-IP rate limiting on HTTP endpoints
//   - LRU cache with TTL for hot product data
//   - Worker pool for async order processing
//   - Pub/sub for decoupled event delivery
//   - Job scheduler for background maintenance
//   - Prometheus metrics: HTTP, gRPC, pool, cache
//   - OpenTelemetry traces exported to Grafana Tempo
//   - Health checks: readiness + liveness probes
//   - Graceful shutdown in LIFO order
//
// Run locally:
//
//	go run ./examples/server
//
// With full observability stack (Prometheus + Grafana + Tempo):
//
//	docker compose up
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	productv1 "github.com/miladhzz/gkit/api/product/v1"
	"github.com/miladhzz/gkit/pkg/cache"
	"github.com/miladhzz/gkit/pkg/circuitbreaker"
	"github.com/miladhzz/gkit/pkg/graceful"
	"github.com/miladhzz/gkit/pkg/health"
	"github.com/miladhzz/gkit/pkg/metrics"
	"github.com/miladhzz/gkit/pkg/middleware"
	gkitotel "github.com/miladhzz/gkit/pkg/otel"
	"github.com/miladhzz/gkit/pkg/pool"
	"github.com/miladhzz/gkit/pkg/pubsub"
	"github.com/miladhzz/gkit/pkg/ratelimit"
	"github.com/miladhzz/gkit/pkg/retry"
	"github.com/miladhzz/gkit/pkg/rpc"
	"github.com/miladhzz/gkit/pkg/rpc/codec"
	"github.com/miladhzz/gkit/pkg/rpc/interceptors"
	"github.com/miladhzz/gkit/pkg/sched"
)

func init() { codec.Register() }

// ---- domain types -------------------------------------------------------

type OrderEvent struct {
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	At        time.Time `json:"at"`
}

var ErrNotFound = errors.New("not found")

// ---- main ---------------------------------------------------------------

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── OpenTelemetry ────────────────────────────────────────────────────
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	tp, err := gkitotel.NewTracerProvider(ctx,
		gkitotel.WithServiceName(envOr("OTEL_SERVICE_NAME", "gkit-server")),
		gkitotel.WithEnvironment("development"),
		gkitotel.WithOTLPEndpoint(otlpEndpoint),
	)
	if err != nil {
		logger.Error("otel init failed", slog.Any("error", err))
	} else {
		defer tp.Shutdown(context.Background()) //nolint:errcheck
	}

	// ── Prometheus ───────────────────────────────────────────────────────
	reg := metrics.NewRegistry("gkit")
	httpMetrics := middleware.NewHTTPMetrics(reg.Registry())
	rpcMetrics := interceptors.NewRPCMetrics(reg.Registry())

	// ── Cache ────────────────────────────────────────────────────────────
	productCache := cache.New[string, productv1.Product](512,
		cache.WithTTL[string, productv1.Product](5*time.Minute),
	)
	productCache.StartJanitor(ctx, 30*time.Second)
	for _, p := range sampleProducts() {
		productCache.Set(p.ID, p)
	}
	reg.MustRegister(metrics.NewCacheCollector("product_cache", func() metrics.CacheSnapshot {
		s := productCache.Stats()
		return metrics.CacheSnapshot{Hits: s.Hits, Misses: s.Misses, Evicts: s.Evicts, Len: s.Len}
	}))

	// ── Circuit Breaker ──────────────────────────────────────────────────
	dbBreaker := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(5),
		circuitbreaker.WithSuccessThreshold(2),
		circuitbreaker.WithOpenTimeout(30*time.Second),
		circuitbreaker.WithOnStateChange(func(from, to circuitbreaker.State) {
			logger.Warn("circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()),
			)
		}),
	)

	// ── Rate Limiter: per-IP ─────────────────────────────────────────────
	ipLimiter := ratelimit.NewKeyed[string](100, 20,
		ratelimit.WithKeyTTL[string](5*time.Minute),
	)

	// ── Event Bus ────────────────────────────────────────────────────────
	bus := pubsub.NewBus(logger)

	// ── Worker Pool ──────────────────────────────────────────────────────
	orderPool := pool.New[OrderEvent](4, func(ctx context.Context, ev OrderEvent) error {
		return processOrder(ctx, ev, logger)
	},
		pool.WithQueueSize[OrderEvent](256),
		pool.WithOnError[OrderEvent](func(ctx context.Context, ev OrderEvent, err error) {
			logger.ErrorContext(ctx, "order processing failed",
				slog.String("product_id", ev.ProductID), slog.Any("error", err))
		}),
	)
	orderPool.Start(ctx)
	reg.MustRegister(metrics.NewPoolCollector("order_pool", func() metrics.PoolSnapshot {
		s := orderPool.Stats()
		return metrics.PoolSnapshot{
			Submitted: s.Submitted, Completed: s.Completed,
			Errors: s.Errors, InFlight: s.InFlight, QueueDepth: s.QueueDepth,
		}
	}))

	unsub := pubsub.Subscribe[OrderEvent](bus, "orders.placed",
		func(ctx context.Context, e pubsub.Event[OrderEvent]) error {
			return orderPool.Submit(ctx, e.Payload)
		}, 64)
	defer unsub()

	// ── Scheduler ────────────────────────────────────────────────────────
	scheduler := sched.New(2,
		sched.WithLogger(logger),
		sched.WithOnError(func(j sched.Job, err error) {
			logger.Error("job failed", slog.String("job", j.Name), slog.Any("error", err))
		}),
	)
	scheduler.Every(30*time.Second, "cache-warmup", func(ctx context.Context) error {
		for _, p := range sampleProducts() {
			productCache.Set(p.ID, p)
		}
		return nil
	})
	scheduler.Every(60*time.Second, "log-stats", func(ctx context.Context) error {
		s := productCache.Stats()
		logger.InfoContext(ctx, "cache stats",
			slog.Uint64("hits", s.Hits), slog.Uint64("misses", s.Misses), slog.Int("len", s.Len))
		return nil
	})
	scheduler.Start(ctx)

	// ── Health ───────────────────────────────────────────────────────────
	healthGroup := health.New(health.WithTimeout(3 * time.Second))
	healthGroup.Register("order_pool", health.CheckerFunc(func(_ context.Context) error {
		if d := orderPool.Stats().QueueDepth; d > 200 {
			return fmt.Errorf("order queue depth critical: %d", d)
		}
		return nil
	}))
	healthGroup.Register("circuit_breaker", health.CheckerFunc(func(_ context.Context) error {
		if dbBreaker.State() == circuitbreaker.StateOpen {
			return errors.New("database circuit is open")
		}
		return nil
	}))

	// ── HTTP Server ──────────────────────────────────────────────────────
	mux := http.NewServeMux()

	mux.HandleFunc("GET /products/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		product, err := retry.Do(r.Context(), func(ctx context.Context) (productv1.Product, error) {
			if p, ok := productCache.Get(id); ok {
				return p, nil
			}
			return circuitbreaker.Execute(ctx, dbBreaker, func(ctx context.Context) (productv1.Product, error) {
				return fetchProductFromDB(ctx, id)
			})
		},
			retry.WithMaxAttempts(3),
			retry.WithBackoff(retry.WithJitter(retry.ExponentialBackoff{
				Initial: 50 * time.Millisecond, Multiplier: 2, Max: 500 * time.Millisecond,
			})),
		)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				http.Error(w, "product not found", http.StatusNotFound)
				return
			}
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(product) //nolint:errcheck
	})

	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var ev OrderEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		ev.At = time.Now()
		pubsub.Publish[OrderEvent](bus, r.Context(), "orders.placed", ev)
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted"}) //nolint:errcheck
	})

	mux.Handle("GET /metrics", reg.Handler())
	mux.Handle("GET /healthz/ready", healthGroup.ReadyHandler())
	mux.Handle("GET /healthz/live", healthGroup.LiveHandler())

	handler := middleware.Apply(mux,
		middleware.RequestID(),
		middleware.Logging(logger),
		middleware.Recovery(logger),
		httpMetrics.Middleware(),
		middleware.Timeout(10*time.Second),
		middleware.RateLimit(func(r *http.Request) bool {
			return ipLimiter.Allow(r.RemoteAddr)
		}),
	)

	httpSrv := &http.Server{
		Addr:        ":8080",
		Handler:     handler,
		ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second,
	}

	// ── gRPC Server ──────────────────────────────────────────────────────
	grpcSrv := rpc.NewServer(
		rpc.WithReflection(),
		rpc.WithUnaryInterceptors(
			interceptors.Recovery(logger),
			interceptors.Logging(logger),
			rpcMetrics.UnaryInterceptor(),
		),
		rpc.WithStreamInterceptors(
			interceptors.StreamRecovery(logger),
			interceptors.StreamLogging(logger),
			rpcMetrics.StreamInterceptor(),
		),
	)
	productv1.RegisterProductServiceServer(grpcSrv.Server(), &productService{
		cache: productCache, breaker: dbBreaker, bus: bus,
	})

	// ── Graceful Shutdown ─────────────────────────────────────────────────
	g := graceful.New(graceful.WithTimeout(15 * time.Second))
	g.Register("scheduler", func(_ context.Context) error {
		logger.Info("stopping scheduler"); cancel(); scheduler.Stop(); return nil
	})
	g.Register("grpc-server", func(_ context.Context) error {
		logger.Info("shutting down gRPC server"); grpcSrv.GracefulStop(); return nil
	})
	g.Register("http-server", func(ctx context.Context) error {
		logger.Info("shutting down HTTP server"); return httpSrv.Shutdown(ctx)
	})
	g.Register("order-pool", func(_ context.Context) error {
		logger.Info("draining order pool"); orderPool.Stop(); return nil
	})

	go func() {
		logger.Info("HTTP listening", slog.String("addr", ":8080"))
		if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error", slog.Any("error", err))
		}
	}()
	go func() {
		logger.Info("gRPC listening", slog.String("addr", ":50051"))
		if err := grpcSrv.Serve(ctx, ":50051"); err != nil {
			logger.Error("gRPC error", slog.Any("error", err))
		}
	}()

	if err := g.ListenAndShutdown(ctx); err != nil {
		logger.Error("shutdown error", slog.Any("error", err)); os.Exit(1)
	}
	logger.Info("server stopped gracefully")
}

// ---- gRPC service -------------------------------------------------------

type productService struct {
	productv1.UnimplementedProductServiceServer
	cache   *cache.Cache[string, productv1.Product]
	breaker *circuitbreaker.Breaker
	bus     *pubsub.Bus
}

func (s *productService) GetProduct(ctx context.Context, req *productv1.GetProductRequest) (*productv1.Product, error) {
	if p, ok := s.cache.Get(req.ID); ok {
		return &p, nil
	}
	p, err := circuitbreaker.Execute(ctx, s.breaker, func(ctx context.Context) (productv1.Product, error) {
		return fetchProductFromDB(ctx, req.ID)
	})
	if err != nil {
		return nil, err
	}
	s.cache.Set(p.ID, p)
	return &p, nil
}

func (s *productService) ListProducts(_ context.Context, req *productv1.ListProductsRequest) (*productv1.ListProductsResponse, error) {
	all := sampleProducts()
	pageSize := int(req.PageSize)
	if pageSize <= 0 || pageSize > len(all) {
		pageSize = len(all)
	}
	result := make([]*productv1.Product, pageSize)
	for i := range pageSize {
		p := all[i]
		result[i] = &p
	}
	return &productv1.ListProductsResponse{Products: result}, nil
}

func (s *productService) PlaceOrder(ctx context.Context, req *productv1.PlaceOrderRequest) (*productv1.PlaceOrderResponse, error) {
	ev := OrderEvent{ProductID: req.ProductID, Quantity: int(req.Quantity), At: time.Now()}
	pubsub.Publish[OrderEvent](s.bus, ctx, "orders.placed", ev)
	return &productv1.PlaceOrderResponse{
		OrderID: fmt.Sprintf("ord-%d", time.Now().UnixNano()),
		Status:  "accepted",
	}, nil
}

// ---- helpers ------------------------------------------------------------

func fetchProductFromDB(_ context.Context, id string) (productv1.Product, error) {
	if rand.Float64() < 0.2 {
		return productv1.Product{}, fmt.Errorf("transient db error: %w", ErrNotFound)
	}
	return productv1.Product{}, fmt.Errorf("%w: product %s", ErrNotFound, id)
}

func processOrder(ctx context.Context, ev OrderEvent, logger *slog.Logger) error {
	logger.InfoContext(ctx, "processing order",
		slog.String("product_id", ev.ProductID), slog.Int("quantity", ev.Quantity))
	time.Sleep(time.Duration(rand.IntN(10)) * time.Millisecond)
	return nil
}

func sampleProducts() []productv1.Product {
	return []productv1.Product{
		{ID: "p1", Name: "Wireless Keyboard", Price: 79.99},
		{ID: "p2", Name: "Mechanical Mouse", Price: 49.99},
		{ID: "p3", Name: "4K Monitor", Price: 399.99},
		{ID: "p4", Name: "USB-C Hub", Price: 34.99},
		{ID: "p5", Name: "Laptop Stand", Price: 24.99},
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
