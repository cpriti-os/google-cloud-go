package storage

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/status"
)

func recordWriteBodySize(ctx context.Context, instruments *metricInstruments, operation string, bytes int64) {
	if instruments == nil {
		return
	}
	instruments.gcsStorageClientRequestBodySize.Record(ctx, bytes, metric.WithAttributes(
		attribute.String("gcp.client.service", "storage"),
		attribute.String("rpc.method", operation),
	))
}

func recordReadBodySize(ctx context.Context, instruments *metricInstruments, operation string, bytes int64) {
	if instruments == nil {
		return
	}
	instruments.gcsStorageClientResponseBodySize.Record(ctx, bytes, metric.WithAttributes(
		attribute.String("gcp.client.service", "storage"),
		attribute.String("rpc.method", operation),
	))
}

func recordAttemptMetrics(ctx context.Context, instruments *metricInstruments, operation string, start time.Time, err error) {
	if instruments == nil {
		return
	}

	duration := time.Since(start).Seconds()
	isGRPC, _ := ctx.Value(transportKey).(bool)
	statusCode := "200"
	if isGRPC {
		statusCode = "OK"
	}
	errorType := "OK"

	if err != nil {
		if isGRPC {
			if s, ok := status.FromError(err); ok {
				statusCode = s.Code().String()
			} else {
				statusCode = "UNKNOWN"
			}
		} else {
			if gErr, ok := err.(*googleapi.Error); ok {
				statusCode = strconv.Itoa(gErr.Code)
			} else {
				statusCode = "UNKNOWN"
			}
		}

		errorType = "UNKNOWN"
		if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
			errorType = "TIMEOUT"
		} else if strings.Contains(err.Error(), "connection") {
			errorType = "CONNECTIVITY"
		}
	}

	instruments.gcsStorageClientOperationAttemptDuration.Record(ctx, duration, metric.WithAttributes(
		attribute.String("gcp.client.service", "storage"),
		attribute.String("rpc.method", operation),
		attribute.String("status", statusCode), // Use actual statusCode here
		attribute.String("error.type", errorType),
	))

	instruments.gcsStorageClientAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("gcp.client.service", "storage"),
		attribute.String("rpc.method", operation),
		attribute.String("http.response.status_code", statusCode),
		attribute.String("error.type", errorType),
	))
}

func recordGFEDurationFromHeader(ctx context.Context, instruments *metricInstruments, header http.Header, host string) {
	if instruments == nil {
		return
	}
	serverTiming := header.Get("Server-Timing")
	if serverTiming != "" {
		parts := strings.Split(serverTiming, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "gfet4a; dur=") {
				durStr := strings.TrimPrefix(part, "gfet4a; dur=")
				if durMs, err := strconv.ParseFloat(durStr, 64); err == nil {
					instruments.gcsStorageClientGfeDuration.Record(ctx, durMs/1000.0, metric.WithAttributes(
						attribute.String("gcp.client.service", "storage"),
						attribute.String("server.address", host),
					))
				}
			}
		}
	}
}
