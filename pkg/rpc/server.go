// Package rpc provides a gRPC server builder and client utilities with
// production-ready defaults: structured logging, panic recovery, and
// Prometheus metrics out of the box.
//
// Server example:
//
//	srv := rpc.NewServer(
//	    rpc.WithUnaryInterceptors(
//	        interceptors.Recovery(logger),
//	        interceptors.Logging(logger),
//	        interceptors.Metrics(reg),
//	    ),
//	)
//	productv1.RegisterProductServiceServer(srv.Server(), &productService{})
//
//	go srv.Serve(ctx, ":50051")
//	defer srv.GracefulStop()
package rpc

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps a *grpc.Server with lifecycle management.
type Server struct {
	srv  *grpc.Server
	opts serverOptions
}

type serverOptions struct {
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	enableReflection   bool
}

// ServerOption configures a Server.
type ServerOption func(*serverOptions)

// WithUnaryInterceptors adds unary server interceptors (applied in order).
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(o *serverOptions) {
		o.unaryInterceptors = append(o.unaryInterceptors, interceptors...)
	}
}

// WithStreamInterceptors adds stream server interceptors (applied in order).
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(o *serverOptions) {
		o.streamInterceptors = append(o.streamInterceptors, interceptors...)
	}
}

// WithReflection enables gRPC server reflection (for grpcurl, grpcui, etc.).
func WithReflection() ServerOption {
	return func(o *serverOptions) { o.enableReflection = true }
}

// NewServer creates a *Server with the configured interceptor chain.
func NewServer(opts ...ServerOption) *Server {
	o := serverOptions{enableReflection: true}
	for _, opt := range opts {
		opt(&o)
	}

	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(o.unaryInterceptors...),
		grpc.ChainStreamInterceptor(o.streamInterceptors...),
	}

	srv := grpc.NewServer(grpcOpts...)

	s := &Server{srv: srv, opts: o}
	if o.enableReflection {
		reflection.Register(srv)
	}
	return s
}

// Server returns the underlying *grpc.Server for service registration.
func (s *Server) Server() *grpc.Server { return s.srv }

// Serve starts the gRPC server on addr. It blocks until the context is
// cancelled, then initiates a graceful stop.
func (s *Server) Serve(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(lis) }()

	select {
	case <-ctx.Done():
		s.srv.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// GracefulStop stops the server after all active RPCs complete.
func (s *Server) GracefulStop() { s.srv.GracefulStop() }
