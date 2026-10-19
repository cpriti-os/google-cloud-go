// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/auth"
	internalTrace "cloud.google.com/go/internal/trace"
	"cloud.google.com/go/storage/internal"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type traceContextKey string

const (
	cacheContextKey    traceContextKey = "bucketMetadataCache"
	bucketContextKey   traceContextKey = "bucketName"
	isBucketContextKey traceContextKey = "isBucketOp"
	traceAttributesKey traceContextKey = "traceAttributes"
)

func contextWithTraceAttributes(ctx context.Context, attrs []attribute.KeyValue) context.Context {
	return context.WithValue(ctx, traceAttributesKey, attrs)
}

func traceAttributesFromContext(ctx context.Context) ([]attribute.KeyValue, bool) {
	attrs, ok := ctx.Value(traceAttributesKey).([]attribute.KeyValue)
	return attrs, ok
}

const (
	defaultTracerName = "cloud.google.com/go/storage"
	gcpClientArtifact = "cloud.google.com/go/storage"
	gcpClientService  = "storage"
	storageURIAttrKey = "gcp.storage.uri"

	// Demo-only payload attributes recorded on Object.Reader/Object.Writer
	// spans when they end.
	attrPayloadOffset = "gcp.storage.payload.offset"
	attrPayloadSize   = "gcp.storage.payload.size_bytes"
	attrChecksumType  = "gcp.storage.checksum.type"
)

var (
	rpcSystemHTTP = attribute.String("rpc.system.name", "http")
	rpcSystemGRPC = attribute.String("rpc.system.name", "grpc")
)

// isOTelTracingDevEnabled checks the development flag until experimental feature is launched.
// TODO: Remove development flag upon experimental launch.
func isOTelTracingDevEnabled() bool {
	return os.Getenv(storageOtelTracingDevVar) == "true"
}

func isACOEnabled() bool {
	return os.Getenv(storageBucketMetadataDisabledVar) != "true"
}

func tracer() trace.Tracer {
	return otel.Tracer(defaultTracerName, trace.WithInstrumentationVersion(internal.Version))
}

func withStorageURI(bucket, object string) trace.SpanStartOption {
	if !isOTelTracingDevEnabled() || bucket == "" {
		return trace.WithAttributes()
	}
	return trace.WithAttributes(attribute.String(storageURIAttrKey, "gs://"+bucket+"/"+object))
}

func startSpanWithBucket(ctx context.Context, client *Client, bucket string, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return startSpanWithObject(ctx, client, bucket, "", name, opts...)
}

func startSpanWithObject(ctx context.Context, client *Client, bucket string, object string, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !isOTelTracingDevEnabled() {
		return startSpan(ctx, name, opts...)
	}
	if bucket != "" {
		opts = append(opts, withStorageURI(bucket, object))
	}
	if client != nil && client.rpcSystem != "" {
		opts = append(opts, trace.WithAttributes(attribute.String("rpc.system.name", client.rpcSystem)))
	}
	if client != nil && client.bucketMetadataCache != nil && bucket != "" {
		ctx = context.WithValue(ctx, cacheContextKey, client.bucketMetadataCache)
		ctx = context.WithValue(ctx, bucketContextKey, bucket)
		isBucket := strings.HasPrefix(name, "Bucket.") || strings.HasPrefix(name, "ACL.") || strings.HasPrefix(name, "storage.IAM.")
		ctx = context.WithValue(ctx, isBucketContextKey, isBucket)

		cache := client.bucketMetadataCache
		meta, hit := cache.get(bucket)
		if !hit {
			placeholder := bucketMetadata{
				resource:    fmt.Sprintf("projects/_/buckets/%s", bucket),
				location:    "global",
				placeholder: true,
			}
			cache.put(bucket, placeholder)
			cache.fetchBackground(ctx, bucket)
			meta = placeholder
		}
		attrs := []attribute.KeyValue{
			attribute.String("gcp.resource.destination.id", destinationResourceName(meta.resource)),
			attribute.String("gcp.resource.destination.location", meta.location),
		}
		ctx = contextWithTraceAttributes(ctx, attrs)
	}
	return startSpan(ctx, name, opts...)
}

// storageResourceNamePrefix is prepended to the bucket resource name only when
// it is emitted in the gcp.resource.destination.id span attribute. Cloud
// Trace's App Hub extractor only accepts full resource names of the form
// "//{service}/{path}"; a bare "projects/.../buckets/..." path is rejected as
// malformed and the span is silently dropped from App Hub enrichment. The
// bucket metadata cache and fetchBucketMetadata keep the bare
// "projects/{p}/buckets/{b}" form.
const storageResourceNamePrefix = "//storage.googleapis.com/"

// destinationResourceName converts a bare bucket resource name
// ("projects/{p}/buckets/{b}") into the full resource name
// ("//storage.googleapis.com/projects/{p}/buckets/{b}") required by Cloud
// Trace's App Hub extractor for the gcp.resource.destination.id attribute.
func destinationResourceName(resource string) string {
	return storageResourceNamePrefix + resource
}

// startSpan creates a span and a context.Context containing the newly-created span.
// If the context.Context provided in `ctx` contains a span then the newly-created
// span will be a child of that span, otherwise it will be a root span.
func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx = context.WithValue(ctx, apiMethodKey{}, name)
	name = appendPackageName(name)
	// TODO: Remove internalTrace upon experimental launch.
	if !isOTelTracingDevEnabled() {
		ctx = internalTrace.StartSpan(ctx, name)
		return ctx, nil
	}
	opts = append(opts, getCommonTraceOptions()...)
	if attrs, ok := traceAttributesFromContext(ctx); ok {
		opts = append(opts, trace.WithAttributes(attrs...))
	}
	ctx, span := tracer().Start(ctx, name, opts...)
	return ctx, span
}

func isNotFoundError(err error) bool {
	if errors.Is(err, ErrBucketNotExist) {
		return true
	}
	var e *googleapi.Error
	if s, ok := status.FromError(err); (ok && s.Code() == codes.NotFound) ||
		(errors.As(err, &e) && e.Code == http.StatusNotFound) {
		return true
	}
	return false
}

// errorAttributes returns the error.type, error.message and, when known,
// gcp.errors.domain span attributes for err. It returns no attributes for a nil error or io.EOF.
//
// error.type reuses computeErrorType, the classifier behind the error.type
// metric attribute, so spans and metrics report the same value for a failure.
// gcp.errors.domain is the google.rpc.ErrorInfo domain of the error response.
func errorAttributes(err error) []attribute.KeyValue {
	if err == nil || err == io.EOF {
		return nil
	}
	// Infer the transport from the error, since endSpan does not know it:
	// HTTP errors always carry a *googleapi.Error.
	var httpErr *googleapi.Error
	isHTTP := errors.As(err, &httpErr)
	attrs := []attribute.KeyValue{
		attribute.String("error.type", computeErrorType(err, isHTTP, 0)),
		attribute.String("error.message", extractErrorMessage(err)),
	}
	if domain := errorDomain(err); domain != "" {
		attrs = append(attrs, attribute.String("gcp.errors.domain", domain))
	}
	return attrs
}

// errorDomain returns the google.rpc.ErrorInfo domain of err, or "" if the
// error response carried no ErrorInfo.
func errorDomain(err error) string { return extractErrorDomain(err) }

// extractErrorMessage returns a human-readable error message from err.
// It prefers the service-provided message from an APIError, googleapi.Error,
// or gRPC status. For a googleapi.Error without a message, such as an XML API
// error response, it uses the Message element of the XML body, then the HTTP
// status text, so that the raw response body is never returned. Otherwise it
// falls back to err.Error().
func extractErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	var apiErr *apierror.APIError
	if errors.As(err, &apiErr) && apiErr.Message() != "" {
		return apiErr.Message()
	}
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		if gErr.Message != "" {
			return gErr.Message
		}
		// Error responses from the XML API are not parsed by googleapi, so
		// Message is empty and Error() would embed the raw response body.
		if msg := xmlErrorMessage(gErr.Body); msg != "" {
			return msg
		}
		if msg := http.StatusText(gErr.Code); msg != "" {
			return msg
		}
		if gErr.Code > 0 {
			return fmt.Sprintf("HTTP %d", gErr.Code)
		}
	}
	// status.FromError is not used here: for wrapped errors it replaces the
	// status message with the entire err.Error() text.
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		if st := gs.GRPCStatus(); st != nil && st.Message() != "" {
			return st.Message()
		}
	}
	return err.Error()
}

// xmlErrorMessage returns the Message element of a Cloud Storage XML API
// error body, or "" if body is not such an error.
func xmlErrorMessage(body string) string {
	if body == "" {
		return ""
	}
	var xmlErr struct {
		XMLName xml.Name `xml:"Error"`
		Message string   `xml:"Message"`
	}
	if err := xml.Unmarshal([]byte(body), &xmlErr); err != nil {
		return ""
	}
	return strings.TrimSpace(xmlErr.Message)
}

// extractErrorDomain returns the service domain that originated err, taken
// from the google.rpc.ErrorInfo details of the error response. It returns ""
// if the response carried no ErrorInfo or err did not come from the service.
func extractErrorDomain(err error) string {
	// Check for an existing APIError first: ParseError on a wrapped APIError
	// prefers its mapped gRPC status, which carries no ErrorInfo.
	var apiErr *apierror.APIError
	if !errors.As(err, &apiErr) {
		var ok bool
		if apiErr, ok = apierror.ParseError(err, false); !ok {
			return ""
		}
	}
	return apiErr.Domain()
}

// endSpan retrieves the current span from ctx and completes the span.
// If an error occurs, the error.type, error.message and, when known,
// gcp.errors.domain
// attributes are set, the error is recorded as an exception span event for
// this span, and the span status is set in the form of a code and a
// description.
func endSpan(ctx context.Context, err error) {
	if err != nil && isNotFoundError(err) {
		isBucket, _ := ctx.Value(isBucketContextKey).(bool)
		cache, _ := ctx.Value(cacheContextKey).(*bucketMetadataCache)
		bucket, _ := ctx.Value(bucketContextKey).(string)

		if isBucket && cache != nil && bucket != "" {
			cache.evict(bucket)
		}
	}

	// TODO: Remove internalTrace upon experimental launch.
	if !isOTelTracingDevEnabled() {
		internalTrace.EndSpan(ctx, err)
	} else {
		span := trace.SpanFromContext(ctx)
		if err != nil {
			span.SetAttributes(errorAttributes(err)...)
			span.SetStatus(otelcodes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}
}

// getCommonTraceOptions makes a SpanStartOption with common attributes.
func getCommonTraceOptions() []trace.SpanStartOption {
	opts := []trace.SpanStartOption{
		trace.WithAttributes(getCommonAttributes()...),
	}
	return opts
}

// getCommonAttributes includes the common attributes used for Cloud Trace adoption tracking.
func getCommonAttributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("gcp.client.service", gcpClientService),
		attribute.String("gcp.client.version", internal.Version),
		attribute.String("gcp.client.artifact", gcpClientArtifact),
	}
}

func appendPackageName(spanName string) string {
	return fmt.Sprintf("%s.%s", gcpClientArtifact, spanName)
}

// recordWriterTraceAttributes attaches the upload strategy to the Object.Writer
// span in ctx as gcp.storage.write.mode (resumable, oneshot, appendable or
// parallel). For parallel uploads, the configured part size and concurrency are
// also recorded.
func recordWriterTraceAttributes(ctx context.Context, w *Writer) {
	if !isOTelTracingDevEnabled() || w == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	writeMode := "resumable"
	if w.EnableParallelUpload {
		writeMode = "parallel"
	} else if w.Append {
		writeMode = "appendable"
	} else if w.ChunkSize <= 0 {
		writeMode = "oneshot"
	}

	attrs := []attribute.KeyValue{
		attribute.String("gcp.storage.write.mode", writeMode),
	}
	if w.EnableParallelUpload {
		attrs = append(attrs,
			attribute.Int("gcp.storage.write.parallel.part_size", w.ParallelUploadConfig.PartSize),
			attribute.Int("gcp.storage.write.parallel.concurrency", w.ParallelUploadConfig.MaxConcurrency),
		)
	}
	span.SetAttributes(attrs...)
}

// recordReaderTraceAttributes attaches the read strategy to the Object.Reader or
// Object.MultiRangeDownloader span in ctx as gcp.storage.read.mode (full, range
// or multi_range).
func recordReaderTraceAttributes(ctx context.Context, readMode string) {
	if !isOTelTracingDevEnabled() {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String("gcp.storage.read.mode", readMode))
}

// retryAttemptKey is the context key under which run records the attempt it
// is about to make.
type retryAttemptKey struct{}

// retryAttempt describes an attempt made by run.
type retryAttempt struct {
	// number is the attempt number, matching the gccl-attempt-count value sent
	// to the service. It restarts at 1 when run is given withProgressReset.
	number int
	// parent is the span context of the span in run's ctx, usually the
	// operation span. It lets retryAttemptStatsHandler tell the transport's
	// attempt span apart from it.
	parent trace.SpanContext
}

// withRetryAttempt returns ctx annotated with the given attempt number so that
// retryAttemptStatsHandler can record it on the gRPC attempt span.
func withRetryAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, retryAttemptKey{}, retryAttempt{
		number: attempt,
		parent: trace.SpanContextFromContext(ctx),
	})
}

// retryAttemptStatsHandler is a gRPC stats handler that records
// gcp.client.retry.attempt_number on the span of every gRPC attempt made by
// run. The attempt span is created by the OpenTelemetry stats handler that the
// transport layer installs ahead of the client's own dial options, so by the
// time TagRPC runs here the span in ctx is that attempt span. The handler is
// only registered when dev tracing is enabled.
type retryAttemptStatsHandler struct{}

// TagRPC implements stats.Handler.
func (retryAttemptStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	attempt, ok := ctx.Value(retryAttemptKey{}).(retryAttempt)
	if !ok {
		return ctx
	}
	span := trace.SpanFromContext(ctx)
	// If the transport did not start an attempt span (for example because
	// telemetry is disabled), the span in ctx is still the operation span,
	// which must not be tagged.
	if !span.IsRecording() || span.SpanContext().SpanID() == attempt.parent.SpanID() {
		return ctx
	}
	span.SetAttributes(attribute.Int("gcp.client.retry.attempt_number", attempt.number))
	return ctx
}

// HandleRPC implements stats.Handler.
func (retryAttemptStatsHandler) HandleRPC(context.Context, stats.RPCStats) {}

// TagConn implements stats.Handler.
func (retryAttemptStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn implements stats.Handler.
func (retryAttemptStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

// recordRetryBackoff records the backoff that started at start as a
// RetryBackoff child span of the span in ctx. It is a no-op unless dev tracing
// is enabled and the span in ctx is recording.
//
// TODO: When the dev tracing flag is removed, decide whether backoff spans
// need their own opt-in or sampling, since each retry adds one span.
func recordRetryBackoff(ctx context.Context, start time.Time) {
	if !isOTelTracingDevEnabled() {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	_, backoffSpan := startSpan(ctx, "RetryBackoff", trace.WithTimestamp(start))
	backoffSpan.End()
}

// tracedTokenProvider records an Auth.RefreshAccessToken span for every
// blocking token fetch or refresh. Cache hits, which return the same
// *auth.Token as the previous call, do not emit a span.
type tracedTokenProvider struct {
	base auth.TokenProvider
	last atomic.Pointer[auth.Token] // cache hits return the same *auth.Token
}

func (p *tracedTokenProvider) Token(ctx context.Context) (*auth.Token, error) {
	start := time.Now()
	tok, err := p.base.Token(ctx)
	if err == nil && p.last.Swap(tok) == tok {
		return tok, nil // cache hit, no span
	}
	_, span := tracer().Start(ctx, appendPackageName("Auth.RefreshAccessToken"),
		append(getCommonTraceOptions(), trace.WithTimestamp(start))...)
	if err != nil {
		span.SetStatus(otelcodes.Error, err.Error())
		span.RecordError(err)
	}
	span.End()
	return tok, err
}

// wrapAuthCredentialsForTracing wraps an auth.Credentials object to record
// Auth.RefreshAccessToken internal operation spans on blocking token fetches
// and refreshes (cache hits do not emit spans).
func wrapAuthCredentialsForTracing(c *auth.Credentials) *auth.Credentials {
	if c == nil || c.TokenProvider == nil {
		return c
	}
	if hasTracedTokenProvider(c.TokenProvider) {
		return c
	}
	clone := *c
	clone.TokenProvider = &tracedTokenProvider{base: c.TokenProvider}
	return &clone
}

func hasTracedTokenProvider(tp auth.TokenProvider) bool {
	for tp != nil {
		switch p := tp.(type) {
		case *tracedTokenProvider:
			return true
		case *metricsTokenProvider:
			tp = p.base
		default:
			return false
		}
	}
	return false
}

// setUploadIDAttribute records the resumable upload ID on the span in ctx as
// gcp.storage.upload.id.
func setUploadIDAttribute(ctx context.Context, uploadID string) {
	if !isOTelTracingDevEnabled() || uploadID == "" {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("gcp.storage.upload.id", uploadID))
}

// setSpanAttributes sets attributes on the span in ctx when OpenTelemetry
// tracing is enabled.
func setSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	if ctx == nil || !isOTelTracingDevEnabled() {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attrs...)
}

// attemptSpanEvent is the name of the span event recorded on the operation
// span for every attempt (HTTP request or gRPC call) of the operation, with
// the attempt's rpc.system.name, gcp.storage.operation, server.address,
// network.path, error.type, response status and attempt number.
const attemptSpanEvent = "gcp.storage.attempt"

// addSpanEvent adds an event to the span in ctx when OpenTelemetry tracing is
// enabled.
func addSpanEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	if ctx == nil || !isOTelTracingDevEnabled() {
		return
	}
	trace.SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(attrs...))
}
