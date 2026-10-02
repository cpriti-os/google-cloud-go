// Copyright 2026 Google LLC
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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// accuracyMetrics returns client metrics backed by a manual reader.
func accuracyMetrics(t *testing.T) (*clientMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	mr := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })
	cfg := storageConfig{enableOtelMetrics: true, enableOtelDebugMetrics: true, meterProvider: provider}
	cm, _, err := initMetrics(context.Background(), "project-id", &cfg)
	if err != nil {
		t.Fatalf("initMetrics: %v", err)
	}
	return cm, mr
}

// metricPoints sums the count (histograms), value (counters/gauges) and sum of
// all data points of the named metric whose attributes include want.
func metricPoints(t *testing.T, mr *sdkmetric.ManualReader, name string, want map[string]string) (count int64, sum float64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := mr.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	match := func(s interface{ Value(string) (string, bool) }) bool {
		for k, v := range want {
			if got, ok := s.Value(k); !ok || got != v {
				return false
			}
		}
		return true
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					if match(attrLookup(dp.Attributes.ToSlice())) {
						count += int64(dp.Count)
						sum += dp.Sum
					}
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					if match(attrLookup(dp.Attributes.ToSlice())) {
						count += int64(dp.Count)
						sum += float64(dp.Sum)
					}
				}
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if match(attrLookup(dp.Attributes.ToSlice())) {
						count += dp.Value
						sum += float64(dp.Value)
					}
				}
			}
		}
	}
	return count, sum
}

type attrLookupMap map[string]string

func (a attrLookupMap) Value(k string) (string, bool) { v, ok := a[k]; return v, ok }

func attrLookup(kvs []attribute.KeyValue) attrLookupMap {
	m := make(attrLookupMap, len(kvs))
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value.Emit()
	}
	return m
}

func TestComputeErrorTypeAccuracy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		isHTTP     bool
		statusCode int64
		want       string
	}{
		// Object names must never influence the classification.
		{name: "http 404 with keyword object name", isHTTP: true, err: &googleapi.Error{Code: 404, Message: "No such object: b/api-keys/token-auth-timeout.txt"}, want: "NOT_FOUND"},
		{name: "grpc 404 with keyword object name", err: status.Error(codes.NotFound, "No such object: b/tls-certs/connection-refused-timeout"), want: "NOT_FOUND"},
		{name: "wrapped object not exist", isHTTP: true, err: fmt.Errorf("%w: %w", ErrObjectNotExist, &googleapi.Error{Code: 404}), want: "NOT_FOUND"},
		{name: "http 408", isHTTP: true, err: &googleapi.Error{Code: 408}, want: "DEADLINE_EXCEEDED"},
		{name: "http 412", isHTTP: true, err: &googleapi.Error{Code: 412}, want: "FAILED_PRECONDITION"},
		{name: "http 429", isHTTP: true, err: &googleapi.Error{Code: 429}, want: "RESOURCE_EXHAUSTED"},
		{name: "http 502", isHTTP: true, err: &googleapi.Error{Code: 502}, want: "UNAVAILABLE"},
		{name: "http status without error", isHTTP: true, statusCode: 503, want: "UNAVAILABLE"},
		{name: "http upload checksum rejected", isHTTP: true, err: &googleapi.Error{Code: 400, Message: `Provided CRC32C "AAAAAA==" doesn't match calculated CRC32C "n/7wpQ=="`}, want: "CHECKSUM_MISMATCH"},
		{name: "grpc upload checksum rejected", err: status.Error(codes.InvalidArgument, "Provided CRC32C does not match calculated CRC32C"), want: "CHECKSUM_MISMATCH"},
		{name: "client checksum mismatch", err: errors.New("storage: bad CRC on read: got 1, want 2"), want: "CHECKSUM_MISMATCH"},
		// End of stream vs. broken connections.
		{name: "bare EOF", err: io.EOF, want: "OK"},
		{name: "wrapped EOF", err: fmt.Errorf("read body: %w", io.EOF), want: "CONNECTION_ERROR"},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: "CONNECTION_ERROR"},
		// Context and network errors.
		{name: "wrapped cancel", err: fmt.Errorf("op: %w", context.Canceled), want: "CANCELLED"},
		{name: "deadline", err: context.DeadlineExceeded, want: "TIMEOUT"},
		{name: "dial timeout", err: &net.OpError{Op: "dial", Net: "tcp", Err: timeoutError{}}, want: "CONNECTION_ERROR"},
		{name: "read timeout", err: &net.OpError{Op: "read", Net: "tcp", Err: timeoutError{}}, want: "TIMEOUT"},
		{name: "dns timeout", err: &net.DNSError{Err: "i/o timeout", Name: "storage.googleapis.com", IsTimeout: true}, want: "DNS_FAILURE"},
		{name: "grpc dns failure", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp: lookup storage.googleapis.com: no such host"`), want: "DNS_FAILURE"},
		{name: "grpc dial refused", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 1.2.3.4:443: connect: connection refused"`), want: "CONNECTION_ERROR"},
		{name: "grpc server unavailable", err: status.Error(codes.Unavailable, "service is overloaded"), want: "UNAVAILABLE"},
		// Credentials.
		{name: "auth error", err: &auth.Error{Response: &http.Response{StatusCode: 400}, Body: []byte(`{"error":"invalid_grant"}`)}, want: "AUTHENTICATION_ERROR"},
		{name: "token fetch transport failure", isHTTP: true, err: &credentialError{err: errors.New("metadata: GCE metadata \"instance/service-accounts/default/token\" not defined")}, want: "AUTHENTICATION_ERROR"},
		{name: "grpc per-rpc creds", err: status.Error(codes.Unavailable, "transport: per-RPC creds failed due to error: context deadline exceeded"), want: "AUTHENTICATION_ERROR"},
		{name: "grpc unauthenticated from server", err: status.Error(codes.Unauthenticated, "Request had invalid authentication credentials"), want: "UNAUTHENTICATED"},
		{name: "unknown", err: errors.New("something odd"), want: "UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := computeErrorType(tc.err, tc.isHTTP, tc.statusCode); got != tc.want {
				t.Errorf("computeErrorType(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestRefineCancelledForReadStall(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errReadStallTimeout)
	if got := refineCancelled(ctx, computeErrorType(ctx.Err(), true, 0)); got != errorTypeTimeout {
		t.Errorf("stall-aborted attempt: got %q, want %q", got, errorTypeTimeout)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if got := refineCancelled(ctx2, computeErrorType(ctx2.Err(), true, 0)); got != errorTypeCancelled {
		t.Errorf("application cancel: got %q, want %q", got, errorTypeCancelled)
	}
}

func TestAttemptNumberFromHeaders(t *testing.T) {
	for _, tc := range []struct {
		vals []string
		want int
	}{
		{nil, 0},
		{[]string{"gl-go/1.26 gccl/1.2"}, 0},
		{[]string{"gl-go/1.26 gccl/1.2 gccl-invocation-id/abc gccl-attempt-count/3"}, 3},
		{[]string{"gccl-attempt-count/1", "gccl-attempt-count/2"}, 2},
	} {
		if got := attemptNumberFromHeaders(tc.vals); got != tc.want {
			t.Errorf("attemptNumberFromHeaders(%q) = %d, want %d", tc.vals, got, tc.want)
		}
	}
}

func TestHTTPAttemptsRetriesAndTTFB(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Goog-Gfe-Service-Time", "3")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))
	defer srv.Close()
	rt := &metricsRoundTripper{base: http.DefaultTransport, metrics: cm}

	const appDelay = 200 * time.Millisecond
	for attempt := 1; attempt <= 2; attempt++ {
		req, _ := http.NewRequest("GET", srv.URL+"/storage/v1/b/bucket/o/obj", nil)
		req.Header.Set(xGoogHeaderKey, fmt.Sprintf("gl-go/1 gccl-invocation-id/x gccl-attempt-count/%d", attempt))
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		// The application waits before reading the body. This must not be
		// counted as server latency (time to first byte).
		time.Sleep(appDelay)
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}

	if c, _ := metricPoints(t, mr, "gcp.storage.client.attempts", map[string]string{"error.type": "OK"}); c != 2 {
		t.Errorf("attempts = %d, want 2", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.retries", nil); c != 1 {
		t.Errorf("retries = %d, want 1", c)
	}
	c, sum := metricPoints(t, mr, "gcp.storage.client.operation.ttfb", nil)
	if c != 2 {
		t.Fatalf("ttfb count = %d, want 2", c)
	}
	if avg := sum / float64(c); avg >= appDelay.Seconds() {
		t.Errorf("ttfb average %.3fs includes application read delay", avg)
	}
	if c, sum := metricPoints(t, mr, "http.client.request.duration", nil); c != 2 || sum/float64(c) < appDelay.Seconds() {
		t.Errorf("request duration count=%d avg=%.3fs, want 2 attempts covering the body read", c, sum/float64(c))
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.request.active", nil); c != 0 {
		t.Errorf("request.active = %d, want 0", c)
	}
}

func TestHTTPConnectionFailureRecordsNoTTFB(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	rt := &metricsRoundTripper{base: http.DefaultTransport, metrics: cm}
	req, _ := http.NewRequest("GET", url+"/storage/v1/b/bucket/o/obj", nil)
	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("RoundTrip succeeded against a closed server")
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.operation.ttfb", nil); c != 0 {
		t.Errorf("ttfb recorded %d times without a response", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.attempts", map[string]string{"error.type": "CONNECTION_ERROR"}); c != 1 {
		t.Errorf("attempts{CONNECTION_ERROR} = %d, want 1", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.request.active", nil); c != 0 {
		t.Errorf("request.active = %d, want 0", c)
	}
}

func TestIsDirectPathPeer(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want bool
	}{
		{"34.126.10.20:443", true},
		{"[2001:4860:8040::5]:443", true},
		{"142.250.1.1:443", false},
		{"[2607:f8b0:4004::1]:443", false},
	} {
		addr, err := net.ResolveTCPAddr("tcp", tc.addr)
		if err != nil {
			t.Fatal(err)
		}
		if got := isDirectPathPeer(&peer.Peer{Addr: addr}); got != tc.want {
			t.Errorf("isDirectPathPeer(%s) = %v, want %v", tc.addr, got, tc.want)
		}
	}
	if isDirectPathPeer(nil) {
		t.Error("isDirectPathPeer(nil) = true")
	}
}

func TestGFEHeaderMissingSkipsDirectPath(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	ctx := context.Background()
	dp, _ := net.ResolveTCPAddr("tcp", "34.126.10.20:443")
	cp, _ := net.ResolveTCPAddr("tcp", "142.250.1.1:443")
	cm.recordGFEMetrics(ctx, nil, nil, nil, "ReadObject", "google-c2p:///storage.googleapis.com", &peer.Peer{Addr: dp})
	cm.recordGFEMetrics(ctx, nil, nil, nil, "ReadObject", "dns:///storage.googleapis.com:443", &peer.Peer{Addr: cp})
	cm.recordGFEMetrics(ctx, metadata.Pairs("x-goog-gfe-service-time", "7"), nil, nil, "ReadObject", "dns:///storage.googleapis.com:443", &peer.Peer{Addr: cp})
	if c, _ := metricPoints(t, mr, "gcp.storage.client.gfe.header_missing", nil); c != 1 {
		t.Errorf("header_missing = %d, want 1 (CloudPath only)", c)
	}
	if c, sum := metricPoints(t, mr, "gcp.storage.client.gfe.duration", nil); c != 1 || sum != 0.007 {
		t.Errorf("gfe.duration count=%d sum=%v, want 1, 0.007", c, sum)
	}
}

// startBufconnServer starts a gRPC server that handles every method with
// handler and returns a client connection using the metrics interceptors.
func startBufconnServer(t *testing.T, cm *clientMetrics, handler grpc.StreamHandler) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(handler))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	unary, stream := metricsInterceptors(cm)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(unary),
		grpc.WithStreamInterceptor(stream))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

func TestGRPCStreamRecordedWhenCancelled(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	var started atomic.Bool
	cc := startBufconnServer(t, cm, func(_ any, ss grpc.ServerStream) error {
		started.Store(true)
		ss.SendMsg(&emptypb.Empty{})
		<-ss.Context().Done()
		return ss.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cs, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/google.storage.v2.Storage/ReadObject")
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	cs.CloseSend()
	if err := cs.RecvMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	// The application stops reading and cancels, like closing a Reader early.
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for {
		c, _ := metricPoints(t, mr, "rpc.client.call.duration", map[string]string{"error.type": "CANCELLED"})
		if c == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancelled stream was not recorded (count=%d)", c)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.request.active", nil); c != 0 {
		t.Errorf("request.active = %d, want 0 after cancellation", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.operation.ttfb", nil); c != 1 {
		t.Errorf("ttfb count = %d, want 1", c)
	}
}

func TestGRPCUnaryNotFoundWithKeywordName(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	cc := startBufconnServer(t, cm, func(_ any, ss grpc.ServerStream) error {
		ss.RecvMsg(&emptypb.Empty{})
		return status.Error(codes.NotFound, "No such object: bucket/api-keys/token-auth-timeout.txt")
	})
	err := cc.Invoke(context.Background(), "/google.storage.v2.Storage/GetObject", &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Invoke: %v", err)
	}
	want := map[string]string{"error.type": "NOT_FOUND", "rpc.status_code": "NOT_FOUND", "rpc.method": "google.storage.v2.Storage/GetObject"}
	if c, _ := metricPoints(t, mr, "rpc.client.call.duration", want); c != 1 {
		t.Errorf("rpc.client.call.duration%v = %d, want 1", want, c)
	}
	// The server responded, so time to first byte is recorded.
	if c, _ := metricPoints(t, mr, "gcp.storage.client.operation.ttfb", nil); c != 1 {
		t.Errorf("ttfb count = %d, want 1", c)
	}
}

func TestBodySizeRecordedOncePerOperationIncludingZero(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	ctx := context.Background()
	s := &metricsState{method: "WriteObject", metrics: cm, isHTTP: true}
	s.recordRequestBodySize(ctx, 0)
	s.recordRequestBodySize(ctx, 0) // e.g. Close called twice
	if c, sum := metricPoints(t, mr, "gcp.storage.client.request.body.size", nil); c != 1 || sum != 0 {
		t.Errorf("request.body.size count=%d sum=%v, want 1, 0", c, sum)
	}
}

func TestCredentialRefreshIgnoresCacheHits(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	ctx := context.Background()
	cm.recordCredentialRefreshDuration(ctx, 20*time.Microsecond, nil)
	if c, _ := metricPoints(t, mr, "gcp.storage.client.auth.credential_refresh.duration", nil); c != 0 {
		t.Errorf("cache hit recorded (%d)", c)
	}
	cm.recordCredentialRefreshDuration(ctx, 20*time.Microsecond, errors.New("invalid_grant"))
	cm.recordCredentialRefreshDuration(ctx, 80*time.Millisecond, nil)
	if c, _ := metricPoints(t, mr, "gcp.storage.client.auth.credential_refresh.duration", map[string]string{"error.type": "AUTHENTICATION_ERROR"}); c != 1 {
		t.Errorf("failed refreshes = %d, want 1", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.auth.credential_refresh.duration", map[string]string{"error.type": "OK"}); c != 1 {
		t.Errorf("blocking refreshes = %d, want 1", c)
	}
}

type slowTokenProvider struct{ calls atomic.Int32 }

func (p *slowTokenProvider) Token(context.Context) (*auth.Token, error) {
	p.calls.Add(1)
	time.Sleep(5 * time.Millisecond)
	return &auth.Token{Value: "t"}, nil
}

func TestDeferredMetricsCredentials(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	base := &slowTokenProvider{}
	creds, mtp := deferredMetricsCredentials(auth.NewCredentials(&auth.CredentialsOptions{TokenProvider: base}))
	if mtp == nil {
		t.Fatal("deferredMetricsCredentials returned no provider")
	}
	// Tokens fetched before metrics are attached are not recorded.
	creds.Token(context.Background())
	// newHTTPStorageClient attaches the metrics to the installed provider.
	if got := wrapAuthCredentials(creds, cm); got != creds {
		t.Error("wrapAuthCredentials cloned the deferred credentials instead of attaching metrics")
	}
	creds.Token(context.Background())
	if c, _ := metricPoints(t, mr, "gcp.storage.client.auth.credential_refresh.duration", nil); c != 1 {
		t.Errorf("credential_refresh count = %d, want 1", c)
	}
	if _, err := (&metricsTokenProvider{base: errTokenProvider{}}).Token(context.Background()); computeErrorType(err, true, 0) != errorTypeAuthenticationError {
		t.Errorf("token error classified as %q", computeErrorType(err, true, 0))
	}
}

type errTokenProvider struct{}

func (errTokenProvider) Token(context.Context) (*auth.Token, error) {
	return nil, errors.New("dial tcp 169.254.169.254:80: connect: connection refused")
}

func TestDebugMetricsAloneEnableMetrics(t *testing.T) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	defer mp.Shutdown(context.Background())
	sc, err := newHTTPStorageClient(context.Background(), withClientOptions(
		withOtelDebugMetrics(), withMeterProvider(mp),
		option.WithoutAuthentication(), option.WithEndpoint("http://localhost:1/storage/v1/")))
	if err != nil {
		t.Fatalf("newHTTPStorageClient: %v", err)
	}
	defer sc.Close()
	hc := sc.(*httpStorageClient)
	if hc.metrics == nil {
		t.Fatal("metrics are not initialized when only debug metrics are enabled")
	}
	if hc.metrics.dnsLookupDuration == nil {
		t.Error("debug instruments are not initialized")
	}
}

func TestCompositeOperationRecordedOnce(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	ctx := cm.startCompositeOperation(context.Background(), "WriteObject", false)
	parent := metricsStateFromContext(ctx)
	for _, m := range []string{"WriteObject", "WriteObject", "ComposeObject"} {
		cctx, record := cm.startOperation(ctx, m, false)
		child := metricsStateFromContext(cctx)
		child.setTarget("dns:///storage.googleapis.com:443")
		child.recordRequestBodySize(cctx, 8<<20)
		record(nil)
	}
	parent.recordRequestBodySize(ctx, 24<<20)
	parent.record(nil)
	want := map[string]string{"rpc.method": "WriteObject", "server.address": "dns:///storage.googleapis.com"}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.operations", nil); c != 1 {
		t.Errorf("operations = %d, want 1", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.operations", want); c != 1 {
		t.Errorf("operations%v = %d, want 1", want, c)
	}
	if c, sum := metricPoints(t, mr, "gcp.storage.client.request.body.size", nil); c != 1 || sum != 24<<20 {
		t.Errorf("request.body.size count=%d sum=%v, want 1, %d", c, sum, 24<<20)
	}
}

func TestStripPort(t *testing.T) {
	for in, want := range map[string]string{
		"storage.googleapis.com:443":               "storage.googleapis.com",
		"storage.googleapis.com":                   "storage.googleapis.com",
		"dns:///storage.googleapis.com:443":        "dns:///storage.googleapis.com",
		"google-c2p:///storage.googleapis.com":     "google-c2p:///storage.googleapis.com",
		"dns://8.8.8.8/storage.googleapis.com:443": "dns://8.8.8.8/storage.googleapis.com",
		"[2001:db8::1]:443":                        "2001:db8::1",
		"localhost:8888":                           "localhost",
	} {
		if got := stripPort(in); got != want {
			t.Errorf("stripPort(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBucketAttributeOnOperationAndAttemptMetrics(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer srv.Close()
	rt := &metricsRoundTripper{base: http.DefaultTransport, metrics: cm}

	mock := &mockStorageClient{getObjectFn: func(ctx context.Context, params *getObjectParams, opts ...storageOption) (*ObjectAttrs, error) {
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/storage/v1/b/my-bucket/o/obj", nil)
		resp, err := rt.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
		return &ObjectAttrs{}, nil
	}}
	mc := &metricsStorageClient{storageClient: mock, metrics: cm, isHTTP: true}
	if _, err := mc.GetObject(context.Background(), &getObjectParams{bucket: "my-bucket", object: "obj"}); err != nil {
		t.Fatalf("GetObject: %v", err)
	}

	want := map[string]string{bucketAttrKey: "my-bucket"}
	for _, name := range []string{
		"gcp.client.request.duration",
		"gcp.storage.client.operations",
		"gcp.storage.client.attempts",
		"gcp.storage.client.operation.ttfb",
	} {
		if c, _ := metricPoints(t, mr, name, want); c != 1 {
			t.Errorf("%s with bucket attribute: count = %d, want 1", name, c)
		}
	}
}

func TestContextWithMetricsBucketKeepsOuterBucket(t *testing.T) {
	ctx := contextWithMetricsBucket(context.Background(), "outer")
	ctx = contextWithMetricsBucket(ctx, "inner")
	ctx = contextWithMetricsBucket(ctx, "")
	got := attrLookup(injectAPIMethod(ctx, nil))
	if got[bucketAttrKey] != "outer" {
		t.Errorf("bucket = %q, want %q", got[bucketAttrKey], "outer")
	}
	if _, ok := attrLookup(injectAPIMethod(context.Background(), nil))[bucketAttrKey]; ok {
		t.Error("bucket attribute set without a bucket in context")
	}
}
