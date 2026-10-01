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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/api/option"
)

const throughputMetric = "gcp.storage.client.operation.throughput"

func TestThroughputSizeClass(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{1 << 20, "1MiB-16MiB"},
		{16<<20 - 1, "1MiB-16MiB"},
		{16 << 20, "16MiB-256MiB"},
		{256<<20 - 1, "16MiB-256MiB"},
		{256 << 20, "256MiB-4GiB"},
		{4<<30 - 1, "256MiB-4GiB"},
		{4 << 30, "4GiB+"},
		{1 << 40, "4GiB+"},
	} {
		if got := throughputSizeClass(tc.n); got != tc.want {
			t.Errorf("throughputSizeClass(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestThroughputHistogramBoundaries(t *testing.T) {
	b := throughputHistogramBoundaries()
	if len(b) != 20 {
		t.Fatalf("got %d boundaries, want 20", len(b))
	}
	if b[0] != 64<<10 || b[len(b)-1] != 32<<30 {
		t.Errorf("boundaries span [%v, %v], want [64KiB/s, 32GiB/s]", b[0], b[len(b)-1])
	}
	for i := 1; i < len(b); i++ {
		if b[i] != 2*b[i-1] {
			t.Fatalf("boundary %d = %v, want %v", i, b[i], 2*b[i-1])
		}
	}
}

// startTestOperation starts an operation whose start time is d in the past.
func startTestOperation(cm *clientMetrics, method string, d time.Duration) (context.Context, *metricsState) {
	ctx, _ := cm.startOperation(context.Background(), method, true)
	s := metricsStateFromContext(ctx)
	s.startTime = time.Now().Add(-d)
	s.setTarget("dns:///storage.googleapis.com:443")
	return ctx, s
}

func TestThroughputRecordedForSuccessfulTransfer(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	const size = 8 << 20
	const elapsed = 2 * time.Second
	ctx, s := startTestOperation(cm, "ReadObject", elapsed)
	s.recordResponseBodySize(ctx, size)
	s.record(nil)

	want := map[string]string{
		"rpc.method":           "ReadObject",
		"rpc.system.name":      "http",
		"server.address":       "dns:///storage.googleapis.com",
		throughputSizeClassKey: "1MiB-16MiB",
	}
	c, sum := metricPoints(t, mr, throughputMetric, want)
	if c != 1 {
		t.Fatalf("throughput%v count = %d, want 1", want, c)
	}
	// The operation took slightly longer than elapsed, so throughput is at
	// most size/elapsed.
	if max, min := float64(size)/elapsed.Seconds(), float64(size)/(elapsed.Seconds()+1); sum > max || sum < min {
		t.Errorf("throughput = %.0f B/s, want in [%.0f, %.0f]", sum, min, max)
	}
	if c, _ := metricPoints(t, mr, "gcp.client.request.duration", nil); c != 1 {
		t.Errorf("duration count = %d, want 1", c)
	}
}

func TestThroughputNotRecorded(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(cm *clientMetrics)
	}{
		{"below minimum size", func(cm *clientMetrics) {
			ctx, s := startTestOperation(cm, "ReadObject", time.Second)
			s.recordResponseBodySize(ctx, minThroughputBytes-1)
			s.record(nil)
		}},
		{"failed operation", func(cm *clientMetrics) {
			ctx, s := startTestOperation(cm, "WriteObject", time.Second)
			s.recordRequestBodySize(ctx, 8<<20)
			s.record(errors.New("connection reset by peer"))
		}},
		{"multi-range downloader", func(cm *clientMetrics) {
			ctx, s := startTestOperation(cm, "ReadObject", time.Second)
			s.noThroughput = true
			s.recordResponseBodySize(ctx, 8<<20)
			s.record(nil)
		}},
		{"operation without a body", func(cm *clientMetrics) {
			_, s := startTestOperation(cm, "GetObject", time.Second)
			s.record(nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm, mr := accuracyMetrics(t)
			tc.run(cm)
			if c, _ := metricPoints(t, mr, throughputMetric, nil); c != 0 {
				t.Errorf("throughput count = %d, want 0", c)
			}
			// The operation itself is still recorded.
			if c, _ := metricPoints(t, mr, "gcp.client.request.duration", nil); c != 1 {
				t.Errorf("duration count = %d, want 1", c)
			}
		})
	}
}

// mrdCaptureClient records the context passed to NewMultiRangeDownloader.
type mrdCaptureClient struct {
	storageClient
	ctx context.Context
}

func (c *mrdCaptureClient) NewMultiRangeDownloader(ctx context.Context, _ *newMultiRangeDownloaderParams, _ ...storageOption) (*MultiRangeDownloader, error) {
	c.ctx = ctx
	return nil, nil
}

func TestMultiRangeDownloaderExcludedFromThroughput(t *testing.T) {
	cm, _ := accuracyMetrics(t)
	inner := &mrdCaptureClient{}
	mc := &metricsStorageClient{storageClient: inner, metrics: cm, isHTTP: false}
	mc.NewMultiRangeDownloader(context.Background(), &newMultiRangeDownloaderParams{})
	s := metricsStateFromContext(inner.ctx)
	if s == nil {
		t.Fatal("NewMultiRangeDownloader did not start an operation")
	}
	if !s.noThroughput {
		t.Error("MultiRangeDownloader operation is not excluded from throughput")
	}
}

func TestThroughputCompositeRecordedOnce(t *testing.T) {
	cm, mr := accuracyMetrics(t)
	ctx := cm.startCompositeOperation(context.Background(), "WriteObject", false)
	parent := metricsStateFromContext(ctx)
	parent.setTarget("dns:///storage.googleapis.com:443")
	for range 3 {
		cctx, record := cm.startOperation(ctx, "WriteObject", false)
		metricsStateFromContext(cctx).recordRequestBodySize(cctx, 8<<20)
		record(nil)
	}
	parent.recordRequestBodySize(ctx, 24<<20)
	parent.record(nil)
	if c, _ := metricPoints(t, mr, throughputMetric, nil); c != 1 {
		t.Errorf("throughput count = %d, want 1 (the composite operation only)", c)
	}
	if c, _ := metricPoints(t, mr, throughputMetric, map[string]string{throughputSizeClassKey: "16MiB-256MiB"}); c != 1 {
		t.Errorf("composite throughput not recorded with the total size class")
	}
}

// TestThroughputEndToEndHTTP checks that a Reader and a Writer of a real HTTP
// client record throughput when they are closed.
func TestThroughputEndToEndHTTP(t *testing.T) {
	const size = 2 << 20
	payload := bytes.Repeat([]byte("x"), size)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/obj"):
			w.Header().Set("Content-Length", fmt.Sprint(size))
			w.Header().Set("X-Goog-Generation", "1")
			w.Write(payload)
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/upload/"):
			io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"bucket":"bucket","name":"obj","size":"%d","generation":"1"}`, size)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	mr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	defer mp.Shutdown(context.Background())
	ctx := context.Background()
	client, err := NewClient(ctx, option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication(),
		withOtelMetrics(), withMeterProvider(mp))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	obj := client.Bucket("bucket").Object("obj")

	r, err := obj.NewReader(ctx)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if n, err := io.Copy(io.Discard, r); err != nil || n != size {
		t.Fatalf("read %d bytes, err %v; want %d", n, err, size)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}

	w := obj.NewWriter(ctx)
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := mr.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != throughputMetric {
				continue
			}
			if m.Unit != "By/s" {
				t.Errorf("unit = %q, want By/s", m.Unit)
			}
			for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				a := attrLookup(dp.Attributes.ToSlice())
				if a[throughputSizeClassKey] != "1MiB-16MiB" || dp.Count != 1 {
					t.Errorf("unexpected data point %v count=%d", a, dp.Count)
				}
				if len(dp.Bounds) != len(throughputHistogramBoundaries()) {
					t.Errorf("histogram has %d bounds, want the throughput boundaries", len(dp.Bounds))
				}
				got[a["rpc.method"]] = dp.Sum
			}
		}
	}
	for _, method := range []string{"ReadObject", "WriteObject"} {
		if got[method] <= 0 {
			t.Errorf("no throughput recorded for %s (got %v)", method, got)
		}
	}
}
