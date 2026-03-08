// Package interceptors provides production-ready gRPC server interceptors.
package interceptors

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logging returns a unary server interceptor that logs each RPC using slog.
// It records the method, duration, and gRPC status code. Errors are logged
// at the ERROR level; successes and client-error codes at DEBUG.
func Logging(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		attrs := []slog.Attr{
			slog.String("method", info.FullMethod),
			slog.String("code", code.String()),
			slog.Duration("duration", dur),
		}

		level := slog.LevelDebug
		if isServerError(code) {
			level = slog.LevelError
			attrs = append(attrs, slog.Any("error", err))
		}

		logger.LogAttrs(ctx, level, "grpc unary", attrs...)
		return resp, err
	}
}

// StreamLogging returns a stream server interceptor with the same semantics.
func StreamLogging(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)
		dur := time.Since(start)

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		attrs := []slog.Attr{
			slog.String("method", info.FullMethod),
			slog.String("code", code.String()),
			slog.Duration("duration", dur),
			slog.Bool("client_stream", info.IsClientStream),
			slog.Bool("server_stream", info.IsServerStream),
		}

		level := slog.LevelDebug
		if isServerError(code) {
			level = slog.LevelError
			attrs = append(attrs, slog.Any("error", err))
		}

		logger.LogAttrs(ss.Context(), level, "grpc stream", attrs...)
		return err
	}
}

// isServerError returns true for codes that indicate a server-side fault.
func isServerError(c codes.Code) bool {
	switch c {
	case codes.OK,
		codes.Canceled,
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange:
		return false
	default:
		return true
	}
}
