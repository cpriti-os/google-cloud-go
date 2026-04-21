package storage

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func unaryMetricsInterceptor(instruments *metricInstruments) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if instruments == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		start := time.Now()
		host := cc.Target()

		instruments.gcsStorageClientActiveRequests.Add(ctx, 1, metric.WithAttributes(
			attribute.String("gcp.client.service", "storage"),
			attribute.String("rpc.method", method),
			attribute.String("server.address", host),
		))
		defer instruments.gcsStorageClientActiveRequests.Add(ctx, -1, metric.WithAttributes(
			attribute.String("gcp.client.service", "storage"),
			attribute.String("rpc.method", method),
			attribute.String("server.address", host),
		))

		err := invoker(ctx, method, req, reply, cc, opts...)
		duration := time.Since(start).Seconds()

		statusCode := "OK"
		if err != nil {
			if s, ok := status.FromError(err); ok {
				statusCode = s.Code().String()
			} else {
				statusCode = "UNKNOWN"
			}
		}

		instruments.rpcClientCallDuration.Record(ctx, duration, metric.WithAttributes(
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", "storage.v2.Storage"), // approximate
			attribute.String("rpc.method", method),
			attribute.String("rpc.response.status_code", statusCode),
			attribute.String("server.address", host),
		))

		return err
	}
}

func streamMetricsInterceptor(instruments *metricInstruments) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if instruments == nil {
			return streamer(ctx, desc, cc, method, opts...)
		}

		host := cc.Target()
		instruments.gcsStorageClientActiveRequests.Add(ctx, 1, metric.WithAttributes(
			attribute.String("gcp.client.service", "storage"),
			attribute.String("rpc.method", method),
			attribute.String("server.address", host),
		))

		stream, err := streamer(ctx, desc, cc, method, opts...)

		// In Go gRPC, stream active tracking accurately is not possible via interceptors
		// since CloseSend/Recv can drop or naturally end without giving a hook. Thus,
		// relying on wrapper CloseSend is generally risky for decrementing active requests
		// because streams could be aborted or just close naturally via Recv EOF.
		// Instead we decrement here. We only increment it to show stream setups in progress.
		// A full active streams counter would need connection-level stats tracking.
		instruments.gcsStorageClientActiveRequests.Add(ctx, -1, metric.WithAttributes(
			attribute.String("gcp.client.service", "storage"),
			attribute.String("rpc.method", method),
			attribute.String("server.address", host),
		))

		return stream, err
	}
}
