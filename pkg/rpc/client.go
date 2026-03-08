package rpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientOption configures a gRPC client connection.
type ClientOption func(*clientOptions)

type clientOptions struct {
	unaryInterceptors  []grpc.UnaryClientInterceptor
	streamInterceptors []grpc.StreamClientInterceptor
	dialOpts           []grpc.DialOption
}

// WithClientUnaryInterceptors adds unary client interceptors.
func WithClientUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) ClientOption {
	return func(o *clientOptions) {
		o.unaryInterceptors = append(o.unaryInterceptors, interceptors...)
	}
}

// WithClientStreamInterceptors adds stream client interceptors.
func WithClientStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) ClientOption {
	return func(o *clientOptions) {
		o.streamInterceptors = append(o.streamInterceptors, interceptors...)
	}
}

// WithDialOptions appends raw grpc.DialOptions (e.g. credentials, keepalive).
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(o *clientOptions) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// Dial creates a gRPC client connection to target.
// By default it uses insecure credentials; pass WithDialOptions(grpc.WithTransportCredentials(...))
// to use TLS.
func Dial(target string, opts ...ClientOption) (*grpc.ClientConn, error) {
	o := &clientOptions{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(o.unaryInterceptors...),
		grpc.WithChainStreamInterceptor(o.streamInterceptors...),
	}
	dialOpts = append(dialOpts, o.dialOpts...)

	return grpc.NewClient(target, dialOpts...)
}
