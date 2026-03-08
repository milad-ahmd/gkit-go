package interceptors

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recovery returns a unary interceptor that recovers from panics in handlers,
// logs a stack trace, and returns a gRPC Internal error to the caller.
func Recovery(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				logger.ErrorContext(ctx, "grpc panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(stack)),
				)
				err = status.Errorf(codes.Internal, "internal error: %v", fmt.Sprint(r))
			}
		}()
		return handler(ctx, req)
	}
}

// StreamRecovery returns a stream interceptor that recovers from panics.
func StreamRecovery(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				logger.ErrorContext(ss.Context(), "grpc stream panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(stack)),
				)
				err = status.Errorf(codes.Internal, "internal error: %v", fmt.Sprint(r))
			}
		}()
		return handler(srv, ss)
	}
}
