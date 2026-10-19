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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/iam"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/internal/testutil"
	"cloud.google.com/go/storage/internal"
	"cloud.google.com/go/storage/internal/apiv2/storagepb"
	"github.com/google/go-cmp/cmp"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.opentelemetry.io/otel/attribute"
	otcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func TestStorageTraceStartEndSpan(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})

	// TODO: Remove setting development env var upon launch.
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	spanName := "storage.TestTrace.TestStartEndSpan"
	ctx, span := startSpan(ctx, spanName)
	newAttrs := attribute.Int("fakeKey", 800)
	span.SetAttributes(newAttrs)
	endSpan(ctx, nil)

	spans := te.Spans()
	gotSpan := spans[0]
	if len(spans) != 1 {
		t.Errorf("expected one span, got %d", len(spans))
	}
	if got, want := gotSpan.Name, appendPackageName(spanName); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}

	wantSpan := createWantSpanStub(spanName, getCommonAttributes())
	wantSpan.Attributes = append(wantSpan.Attributes, newAttrs)
	opts := []cmp.Option{
		cmp.Comparer(spanAttributesComparer),
	}
	if diff := testutil.Diff(gotSpan, wantSpan, opts...); diff != "" {
		t.Errorf("diff: -got, +want:\n%s\n", diff)
	}
}
func TestStorageTraceStartSpanOption(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})

	// TODO: Remove setting development env var upon launch.
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	spanName := "storage.TestTrace.TestStartSpanOption"
	attrMap := make(map[string]interface{})
	attrMap["my_string"] = "my string"
	attrMap["my_bool"] = true
	attrMap["my_int"] = 123
	attrMap["my_int64"] = int64(456)
	attrMap["my_float"] = 0.9
	spanStartOpts := makeSpanStartOptAttrs(attrMap)

	ctx, _ = startSpan(ctx, spanName, spanStartOpts...)
	endSpan(ctx, nil)

	spans := te.Spans()
	gotSpan := spans[0]
	if len(spans) != 1 {
		t.Errorf("expected one span, got %d", len(spans))
	}
	if got, want := gotSpan.Name, appendPackageName(spanName); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}

	wantSpan := createWantSpanStub(spanName, getCommonAttributes())
	wantSpan.Attributes = append(wantSpan.Attributes, otAttrs(attrMap)...)
	opts := []cmp.Option{
		cmp.Comparer(spanAttributesComparer),
	}
	if diff := testutil.Diff(gotSpan, wantSpan, opts...); diff != "" {
		t.Errorf("diff: -got, +want:\n%s\n", diff)
	}
}

func TestStorageTraceEndSpanRecordError(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})

	// TODO: Remove setting development env var upon launch.
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	spanName := "storage.TestTrace.TestRecordError"
	ctx, _ = startSpan(ctx, spanName)
	err := &googleapi.Error{Code: http.StatusBadRequest, Message: "INVALID ARGUMENT"}
	endSpan(ctx, err)

	spans := te.Spans()
	gotSpan := spans[0]
	if len(spans) != 1 {
		t.Errorf("expected one span, got %d", len(spans))
	}
	if got, want := gotSpan.Name, appendPackageName(spanName); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	if want := otcodes.Error; gotSpan.Status.Code != want {
		t.Errorf("got %v, want %v", gotSpan.Status.Code, want)
	}
}

func createWantSpanStub(spanName string, attrs []attribute.KeyValue) tracetest.SpanStub {
	return tracetest.SpanStub{
		Name:       appendPackageName(spanName),
		Attributes: attrs,
		InstrumentationScope: instrumentation.Scope{
			Name:    "cloud.google.com/go/storage",
			Version: internal.Version,
		},
	}
}

func spanAttributesComparer(a, b tracetest.SpanStub) bool {
	if a.Name != b.Name {
		return false
	}
	if len(a.Attributes) != len(b.Attributes) {
		return false
	}
	if a.InstrumentationScope != b.InstrumentationScope {
		return false
	}
	return true
}

// makeSpanStartOptAttrs makes a SpanStartOption and converts a generic map to OpenTelemetry attributes.
func makeSpanStartOptAttrs(attrMap map[string]interface{}) []trace.SpanStartOption {
	attrs := otAttrs(attrMap)
	return []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
	}
}

// otAttrs converts a generic map to OpenTelemetry attributes.
func otAttrs(attrMap map[string]interface{}) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for k, v := range attrMap {
		var a attribute.KeyValue
		switch v := v.(type) {
		case string:
			a = attribute.Key(k).String(v)
		case bool:
			a = attribute.Key(k).Bool(v)
		case int:
			a = attribute.Key(k).Int(v)
		case int64:
			a = attribute.Key(k).Int64(v)
		default:
			a = attribute.Key(k).String(fmt.Sprintf("%#v", v))
		}
		attrs = append(attrs, a)
	}
	return attrs
}

func TestStartSpanWithBucket(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})

	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	fetcher := &mockMetadataFetcher{
		fetchFunc: func(ctx context.Context, bucket string) (resource string, location string, err error) {
			return "projects/p1/buckets/" + bucket, "us-west1", nil
		},
	}

	tests := []struct {
		name         string
		bucket       string
		setupCache   func(*bucketMetadataCache)
		wantResource string
		wantLocation string
		verifyCache  bool
	}{
		{
			name:   "Cache Miss (Placeholder)",
			bucket: "bucket-miss",
			setupCache: func(c *bucketMetadataCache) {
				// empty cache
			},
			wantResource: storageResourceNamePrefix + "projects/_/buckets/bucket-miss",
			wantLocation: "global",
			verifyCache:  true,
		},
		{
			name:   "Cache Hit (Resolved)",
			bucket: "bucket-hit",
			setupCache: func(c *bucketMetadataCache) {
				c.put("bucket-hit", bucketMetadata{resource: "projects/p1/buckets/bucket-hit", location: "us-west1"})
			},
			wantResource: storageResourceNamePrefix + "projects/p1/buckets/bucket-hit",
			wantLocation: "us-west1",
			verifyCache:  false,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := newBucketMetadataCache(10, fetcher)
			tc.setupCache(cache)
			doneChan := make(chan struct{}, 1)
			if tc.verifyCache {
				cache.fetchDone = doneChan
			}
			client := &Client{bucketMetadataCache: cache}

			ctx1, _ := startSpanWithBucket(ctx, client, tc.bucket, "TestSpan")
			endSpan(ctx1, nil)

			spans := te.Spans()
			if len(spans) != i+1 {
				t.Fatalf("expected %d spans, got %d", i+1, len(spans))
			}
			gotSpan := spans[i]

			verifySpanAttributes(t, gotSpan, tc.wantResource, tc.wantLocation)

			if tc.verifyCache {
				// Wait for background fetch to complete and populate cache.
				select {
				case <-doneChan:
				case <-time.After(fetchBackgroundTimeout):
					t.Fatalf("timeout waiting for fetchBackground completion")
				}
				_, found := cache.get(tc.bucket)
				if !found {
					t.Fatalf("expected entry to be populated in cache")
				}
			}
		})
	}
}

func verifySpanAttributes(t *testing.T, span tracetest.SpanStub, wantResource, wantLocation string) {
	t.Helper()
	var gotResource, gotLocation string
	for _, attr := range span.Attributes {
		if attr.Key == "gcp.resource.destination.id" {
			gotResource = attr.Value.AsString()
		}
		if attr.Key == "gcp.resource.destination.location" {
			gotLocation = attr.Value.AsString()
		}
	}

	if gotResource != wantResource {
		t.Errorf("got resource %q, want %q", gotResource, wantResource)
	}

	if gotLocation != wantLocation {
		t.Errorf("got location %q, want %q", gotLocation, wantLocation)
	}
}

func TestEndSpanEviction(t *testing.T) {
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	bucketName := "evict-bucket"
	tests := []struct {
		name      string
		spanName  string
		err       error
		wantEvict bool
	}{
		{
			name:      "Evict on ErrBucketNotExist",
			spanName:  "Bucket.Attrs",
			err:       ErrBucketNotExist,
			wantEvict: true,
		},
		{
			name:      "Evict on googleapi.Error 404",
			spanName:  "Bucket.Attrs",
			err:       &googleapi.Error{Code: http.StatusNotFound},
			wantEvict: true,
		},
		{
			name:      "No Evict on 500",
			spanName:  "Bucket.Attrs",
			err:       &googleapi.Error{Code: http.StatusInternalServerError},
			wantEvict: false,
		},
		{
			name:      "No Evict on Object 404",
			spanName:  "Object.Attrs",
			err:       ErrObjectNotExist,
			wantEvict: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &mockMetadataFetcher{}
			cache := newBucketMetadataCache(10, fetcher)
			client := &Client{bucketMetadataCache: cache}

			// Populate cache.
			cache.put(bucketName, bucketMetadata{resource: "res", location: "loc"})

			ctx, _ := startSpanWithBucket(context.Background(), client, bucketName, tc.spanName)
			endSpan(ctx, tc.err)

			_, found := cache.get(bucketName)
			if tc.wantEvict && found {
				t.Errorf("expected bucket to be evicted")
			}
			if !tc.wantEvict && !found {
				t.Errorf("expected bucket to remain in cache")
			}
		})
	}
}

// storageURIAttr returns the gcp.storage.uri value on the span named spanName,
// and whether it was set.
func storageURIAttr(t *testing.T, spans tracetest.SpanStubs, spanName string) (string, bool) {
	t.Helper()
	for _, s := range spans {
		if s.Name != appendPackageName(spanName) {
			continue
		}
		for _, a := range s.Attributes {
			if a.Key == storageURIAttrKey {
				return a.Value.AsString(), true
			}
		}
		return "", false
	}
	t.Fatalf("span %q not found", spanName)
	return "", false
}

func TestStorageURIAttribute(t *testing.T) {
	const (
		bucket    = "my-bucket"
		object    = "dir/my object.txt"
		dstBucket = "dst-bucket"
		dstObject = "dst/object.txt"
	)
	wantObjURI := "gs://" + bucket + "/" + object
	wantDstURI := "gs://" + dstBucket + "/" + dstObject
	wantBucketURI := "gs://" + bucket + "/"

	tests := []struct {
		name     string // Test name; defaults to spanName.
		spanName string
		op       func(ctx context.Context, c *Client)
		wantURI  string
	}{
		// Object-scoped operations.
		{
			spanName: "Object.Attrs",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Object(object).Attrs(ctx) },
			wantURI:  wantObjURI,
		},
		{
			spanName: "Object.Update",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).Object(object).Update(ctx, ObjectAttrsToUpdate{ContentType: "text/plain"})
			},
			wantURI: wantObjURI,
		},
		{
			spanName: "Object.Delete",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Object(object).Delete(ctx) },
			wantURI:  wantObjURI,
		},
		{
			spanName: "Object.Reader",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Object(object).NewReader(ctx) },
			wantURI:  wantObjURI,
		},
		{
			spanName: "Object.Writer",
			op: func(ctx context.Context, c *Client) {
				w := c.Bucket(bucket).Object(object).NewWriter(ctx)
				w.Write([]byte("data"))
				w.Close()
			},
			wantURI: wantObjURI,
		},
		{
			spanName: "Object.MultiRangeDownloader",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Object(object).NewMultiRangeDownloader(ctx) },
			wantURI:  wantObjURI,
		},
		{
			spanName: "Object.WriterFromAppendableObject",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).Object(object).Generation(1).NewWriterFromAppendableObject(ctx, nil)
			},
			wantURI: wantObjURI,
		},
		{
			name:     "Copier.Run uses destination",
			spanName: "Copier.Run",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(dstBucket).Object(dstObject).CopierFrom(c.Bucket(bucket).Object(object)).Run(ctx)
			},
			wantURI: wantDstURI,
		},
		{
			name:     "Composer.Run uses destination",
			spanName: "Composer.Run",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(dstBucket).Object(dstObject).ComposerFrom(c.Bucket(dstBucket).Object("src1")).Run(ctx)
			},
			wantURI: wantDstURI,
		},
		{
			name:     "object ACL.List",
			spanName: "ACL.List",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Object(object).ACL().List(ctx) },
			wantURI:  wantObjURI,
		},
		{
			name:     "object ACL.Set",
			spanName: "ACL.Set",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).Object(object).ACL().Set(ctx, AllUsers, RoleReader)
			},
			wantURI: wantObjURI,
		},
		{
			name:     "object ACL.Delete",
			spanName: "ACL.Delete",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Object(object).ACL().Delete(ctx, AllUsers) },
			wantURI:  wantObjURI,
		},
		// Bucket-scoped operations.
		{
			name:     "bucket ACL.List",
			spanName: "ACL.List",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).ACL().List(ctx) },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "Bucket.Create",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Create(ctx, "my-project", nil) },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "Bucket.Delete",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Delete(ctx) },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "Bucket.Attrs",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Attrs(ctx) },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "Bucket.Update",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).Update(ctx, BucketAttrsToUpdate{StorageClass: "STANDARD"})
			},
			wantURI: wantBucketURI,
		},
		{
			spanName: "Bucket.AddNotification",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).AddNotification(ctx, &Notification{TopicProjectID: "p", TopicID: "t"})
			},
			wantURI: wantBucketURI,
		},
		{
			spanName: "Bucket.Notifications",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Notifications(ctx) },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "Bucket.DeleteNotification",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).DeleteNotification(ctx, "n") },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "storage.IAM.Get",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).IAM().Policy(ctx) },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "storage.IAM.Set",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).IAM().SetPolicy(ctx, &iam.Policy{InternalProto: &iampb.Policy{}})
			},
			wantURI: wantBucketURI,
		},
		{
			spanName: "storage.IAM.Test",
			op: func(ctx context.Context, c *Client) {
				c.Bucket(bucket).IAM().TestPermissions(ctx, []string{"storage.buckets.get"})
			},
			wantURI: wantBucketURI,
		},
		{
			spanName: "httpStorageClient.ObjectsListCall",
			op:       func(ctx context.Context, c *Client) { c.Bucket(bucket).Objects(ctx, nil).Next() },
			wantURI:  wantBucketURI,
		},
		{
			spanName: "grpcStorageClient.ObjectsListCall",
			op: func(ctx context.Context, _ *Client) {
				gc, err := NewGRPCClient(ctx,
					option.WithEndpoint("localhost:1"),
					option.WithoutAuthentication(),
					option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
				)
				if err != nil {
					t.Fatalf("NewGRPCClient: %v", err)
				}
				defer gc.Close()
				gc.Bucket(bucket).Retryer(WithPolicy(RetryNever)).Objects(ctx, nil).Next()
			},
			wantURI: wantBucketURI,
		},
		{
			name:     "empty bucket omits attribute",
			spanName: "Bucket.Attrs",
			op:       func(ctx context.Context, c *Client) { c.Bucket("").Attrs(ctx) },
			wantURI:  "",
		},
	}

	for _, tc := range tests {
		name := tc.name
		if name == "" {
			name = tc.spanName
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv(storageOtelTracingDevVar, "true")
			// Disable the bucket metadata cache so no background fetch runs.
			t.Setenv(storageBucketMetadataDisabledVar, "true")
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() { te.Unregister(ctx) })

			tc.op(ctx, mockClient(t, &mockTransport{}))

			got, ok := storageURIAttr(t, te.Spans(), tc.spanName)
			if tc.wantURI == "" {
				if ok {
					t.Errorf("%s = %q, want it absent when bucket is empty", storageURIAttrKey, got)
				}
				return
			}
			if !ok {
				t.Fatalf("%s not found on span %q", storageURIAttrKey, tc.spanName)
			}
			if got != tc.wantURI {
				t.Errorf("%s = %q, want %q", storageURIAttrKey, got, tc.wantURI)
			}
		})
	}
}

func TestStorageURIAttributeDevTracingDisabled(t *testing.T) {
	t.Setenv(storageOtelTracingDevVar, "false")
	t.Setenv(storageBucketMetadataDisabledVar, "true")
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() { te.Unregister(ctx) })

	mockClient(t, &mockTransport{}).Bucket("b").Object("o").Attrs(ctx)

	if got, ok := storageURIAttr(t, te.Spans(), "Object.Attrs"); ok {
		t.Errorf("%s = %q, want it absent when dev tracing is disabled", storageURIAttrKey, got)
	}
}

func TestRecordWriterTraceAttributes(t *testing.T) {
	testCases := []struct {
		name      string
		writer    *Writer
		wantAttrs []attribute.KeyValue
	}{
		{
			name:   "resumable",
			writer: &Writer{ChunkSize: 256 * 1024},
			wantAttrs: []attribute.KeyValue{
				attribute.String("gcp.storage.write.mode", "resumable"),
			},
		},
		{
			name:   "oneshot",
			writer: &Writer{ChunkSize: 0},
			wantAttrs: []attribute.KeyValue{
				attribute.String("gcp.storage.write.mode", "oneshot"),
			},
		},
		{
			name:   "appendable",
			writer: &Writer{ChunkSize: 256 * 1024, Append: true},
			wantAttrs: []attribute.KeyValue{
				attribute.String("gcp.storage.write.mode", "appendable"),
			},
		},
		{
			name: "parallel",
			writer: &Writer{
				ChunkSize:            256 * 1024,
				EnableParallelUpload: true,
				ParallelUploadConfig: ParallelUploadConfig{
					PartSize:       16 * 1024 * 1024,
					MaxConcurrency: 4,
				},
			},
			wantAttrs: []attribute.KeyValue{
				attribute.String("gcp.storage.write.mode", "parallel"),
				attribute.Int("gcp.storage.write.parallel.part_size", 16*1024*1024),
				attribute.Int("gcp.storage.write.parallel.concurrency", 4),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})
			// TODO: Remove setting development env var upon launch.
			t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

			ctx, _ = startSpan(ctx, "Object.Writer")
			recordWriterTraceAttributes(ctx, tc.writer)
			endSpan(ctx, nil)

			verifySingleSpanAttributes(t, te.Spans(), tc.wantAttrs)
		})
	}
}

// TestReaderTraceReadMode verifies the read mode recorded by NewRangeReader and
// NewMultiRangeDownloader. The ObjectHandle has no object name, so each call
// fails validation after the span has been started and the attribute recorded,
// before any RPC is made.
func TestReaderTraceReadMode(t *testing.T) {
	testCases := []struct {
		name     string
		read     func(context.Context, *ObjectHandle) error
		wantMode string
	}{
		{
			name: "full",
			read: func(ctx context.Context, o *ObjectHandle) error {
				_, err := o.NewReader(ctx)
				return err
			},
			wantMode: "full",
		},
		{
			name: "range",
			read: func(ctx context.Context, o *ObjectHandle) error {
				_, err := o.NewRangeReader(ctx, 100, 500)
				return err
			},
			wantMode: "range",
		},
		{
			name: "range_to_end",
			read: func(ctx context.Context, o *ObjectHandle) error {
				_, err := o.NewRangeReader(ctx, 100, -1)
				return err
			},
			wantMode: "range",
		},
		{
			name: "range_suffix",
			read: func(ctx context.Context, o *ObjectHandle) error {
				_, err := o.NewRangeReader(ctx, -100, -1)
				return err
			},
			wantMode: "range",
		},
		{
			name: "multi_range",
			read: func(ctx context.Context, o *ObjectHandle) error {
				_, err := o.NewMultiRangeDownloader(ctx)
				return err
			},
			wantMode: "multi_range",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})
			// TODO: Remove setting development env var upon launch.
			t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

			o := &ObjectHandle{c: &Client{}, bucket: "bucket"}
			if err := tc.read(ctx, o); err == nil {
				t.Fatal("read succeeded, want validation error")
			}

			verifySingleSpanAttributes(t, te.Spans(), []attribute.KeyValue{
				attribute.String("gcp.storage.read.mode", tc.wantMode),
			})
		})
	}
}

func TestRecordTraceAttributes_DevTracingDisabled(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "false")

	wCtx, _ := startSpan(ctx, "Object.Writer")
	recordWriterTraceAttributes(wCtx, &Writer{EnableParallelUpload: true})
	endSpan(wCtx, nil)

	rCtx, _ := startSpan(ctx, "Object.Reader")
	recordReaderTraceAttributes(rCtx, "full")
	endSpan(rCtx, nil)

	for _, s := range te.Spans() {
		for _, a := range s.Attributes {
			switch a.Key {
			case "gcp.storage.write.mode",
				"gcp.storage.write.parallel.part_size",
				"gcp.storage.write.parallel.concurrency",
				"gcp.storage.read.mode":
				t.Errorf("span %s has unexpected attribute %s when dev tracing is disabled", s.Name, a.Key)
			}
		}
	}
}

// verifySingleSpanAttributes checks that exactly one span was exported and that
// its attributes are exactly the common attributes plus wantAttrs.
func verifySingleSpanAttributes(t *testing.T, spans tracetest.SpanStubs, wantAttrs []attribute.KeyValue) {
	t.Helper()
	if len(spans) != 1 {
		t.Fatalf("expected one span, got %d", len(spans))
	}
	// Attributes set by other features (gcp.storage.uri, rpc.system.name,
	// payload attributes) are ignored: only the common attributes and
	// wantAttrs are compared.
	ignored := map[attribute.Key]bool{
		storageURIAttrKey: true, "rpc.system.name": true,
		attrPayloadOffset: true, attrPayloadSize: true, attrChecksumType: true,
		"error.type": true, "gcp.errors.domain": true, "error.message": true,
	}
	var filtered []attribute.KeyValue
	for _, a := range spans[0].Attributes {
		if !ignored[a.Key] {
			filtered = append(filtered, a)
		}
	}
	got := attribute.NewSet(filtered...)
	want := attribute.NewSet(append(getCommonAttributes(), wantAttrs...)...)
	if !got.Equals(&want) {
		t.Errorf("span %s attributes mismatch:\n got: %s\nwant: %s", spans[0].Name,
			got.Encoded(attribute.DefaultEncoder()), want.Encoded(attribute.DefaultEncoder()))
	}
}

// retryAttemptNumber returns the value of gcp.client.retry.attempt_number in
// attrs and whether it is present.
func retryAttemptNumber(attrs []attribute.KeyValue) (int64, bool) {
	for _, a := range attrs {
		if a.Key == "gcp.client.retry.attempt_number" {
			return a.Value.AsInt64(), true
		}
	}
	return 0, false
}

func TestRetryAttemptStatsHandlerTagRPC(t *testing.T) {
	for _, tc := range []struct {
		name string
		// start returns the ctx handed to TagRPC, mimicking the ctx the
		// transport's stats handler produces, and ends any spans it started.
		start func(ctx context.Context) (tagCtx context.Context, end func())
		// wantAttempts maps span names to the attempt number they must carry.
		// Spans not listed must not carry one.
		wantAttempts map[string]int64
	}{
		{
			name: "attempt span under operation span",
			start: func(ctx context.Context) (context.Context, func()) {
				opCtx, opSpan := tracer().Start(ctx, "op")
				attemptCtx, attemptSpan := tracer().Start(withRetryAttempt(opCtx, 2), "attempt")
				return attemptCtx, func() { attemptSpan.End(); opSpan.End() }
			},
			wantAttempts: map[string]int64{"attempt": 2},
		},
		{
			name: "attempt span without operation span",
			start: func(ctx context.Context) (context.Context, func()) {
				attemptCtx, attemptSpan := tracer().Start(withRetryAttempt(ctx, 1), "attempt")
				return attemptCtx, func() { attemptSpan.End() }
			},
			wantAttempts: map[string]int64{"attempt": 1},
		},
		{
			name: "no attempt in context",
			start: func(ctx context.Context) (context.Context, func()) {
				opCtx, opSpan := tracer().Start(ctx, "op")
				attemptCtx, attemptSpan := tracer().Start(opCtx, "attempt")
				return attemptCtx, func() { attemptSpan.End(); opSpan.End() }
			},
		},
		{
			// With no transport span the span in ctx is the operation span,
			// which must be left alone.
			name: "no attempt span",
			start: func(ctx context.Context) (context.Context, func()) {
				opCtx, opSpan := tracer().Start(ctx, "op")
				return withRetryAttempt(opCtx, 2), func() { opSpan.End() }
			},
		},
		{
			name: "no span at all",
			start: func(ctx context.Context) (context.Context, func()) {
				return withRetryAttempt(ctx, 2), func() {}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			tagCtx, end := tc.start(ctx)
			retryAttemptStatsHandler{}.TagRPC(tagCtx, &stats.RPCTagInfo{})
			end()

			// Left nil when no span carries the attribute so that it compares
			// equal to an unset wantAttempts.
			var gotAttempts map[string]int64
			for _, s := range te.Spans() {
				if n, ok := retryAttemptNumber(s.Attributes); ok {
					if gotAttempts == nil {
						gotAttempts = map[string]int64{}
					}
					gotAttempts[s.Name] = n
				}
			}
			if diff := cmp.Diff(tc.wantAttempts, gotAttempts); diff != "" {
				t.Errorf("attempt numbers mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// retryAttemptTestServer is a fake storage server whose GetBucket fails with
// UNAVAILABLE for the first failures calls and succeeds afterwards.
type retryAttemptTestServer struct {
	storagepb.UnimplementedStorageServer
	mu       sync.Mutex
	failures int
	calls    int
}

func (s *retryAttemptTestServer) GetBucket(_ context.Context, req *storagepb.GetBucketRequest) (*storagepb.Bucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failures {
		return nil, status.Error(codes.Unavailable, "retry me")
	}
	return &storagepb.Bucket{Name: req.GetName()}, nil
}

func TestGRPCRetryAttemptNumberTracing(t *testing.T) {
	const getBucketSpan = "google.storage.v2.Storage/GetBucket"

	for _, tc := range []struct {
		name       string
		devTracing bool
		failures   int
		// wantAttempts are the attempt numbers expected on the GetBucket
		// attempt spans, in order. nil means the spans carry none.
		wantAttempts []int64
	}{
		{
			name:         "single attempt",
			devTracing:   true,
			wantAttempts: []int64{1},
		},
		{
			name:         "retried",
			devTracing:   true,
			failures:     2,
			wantAttempts: []int64{1, 2, 3},
		},
		{
			name:     "dev tracing disabled",
			failures: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})
			t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", strconv.FormatBool(tc.devTracing))
			// The background bucket metadata fetch would issue an extra,
			// untraced GetBucket that also consumes the server's failures.
			t.Setenv("GO_OTEL_BUCKETMETADATA_DISABLED", "true")

			lis, err := net.Listen("tcp", "localhost:0")
			if err != nil {
				t.Fatalf("net.Listen: %v", err)
			}
			grpcSrv := grpc.NewServer()
			storagepb.RegisterStorageServer(grpcSrv, &retryAttemptTestServer{failures: tc.failures})
			go grpcSrv.Serve(lis)
			t.Cleanup(grpcSrv.Stop)
			t.Setenv("STORAGE_EMULATOR_HOST_GRPC", lis.Addr().String())

			client, err := NewGRPCClient(ctx)
			if err != nil {
				t.Fatalf("NewGRPCClient: %v", err)
			}
			t.Cleanup(func() { client.Close() })
			client.SetRetry(WithBackoff(gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond}))

			if _, err := client.Bucket("bucket").Attrs(ctx); err != nil {
				t.Fatalf("Bucket.Attrs: %v", err)
			}

			spans := te.Spans()
			var opSpan *tracetest.SpanStub
			for i := range spans {
				if spans[i].Name == appendPackageName("Bucket.Attrs") {
					if opSpan != nil {
						t.Fatalf("got more than one Bucket.Attrs span")
					}
					opSpan = &spans[i]
				}
			}
			if opSpan == nil {
				t.Fatalf("Bucket.Attrs span not found")
			}

			var gotAttempts []int64
			attemptSpans := 0
			for _, s := range spans {
				if s.Name != getBucketSpan {
					continue
				}
				attemptSpans++
				if got, want := s.Parent.SpanID(), opSpan.SpanContext.SpanID(); got != want {
					t.Errorf("%s span parent ID = %v, want %v", getBucketSpan, got, want)
				}
				if n, ok := retryAttemptNumber(s.Attributes); ok {
					gotAttempts = append(gotAttempts, n)
				}
			}
			if want := tc.failures + 1; attemptSpans != want {
				t.Errorf("got %d %s spans, want %d", attemptSpans, getBucketSpan, want)
			}
			if diff := cmp.Diff(tc.wantAttempts, gotAttempts); diff != "" {
				t.Errorf("attempt numbers on %s spans mismatch (-want +got):\n%s", getBucketSpan, diff)
			}
		})
	}
}

func TestErrorAttributes(t *testing.T) {
	grpcWithErrorInfo, err := status.New(codes.Unimplemented, "Appendable objects are not supported for this request.").WithDetails(&errdetails.ErrorInfo{
		Reason: "GRPC_APPENDABLE_OBJECT_NOT_SUPPORTED",
		Domain: "storage.googleapis.com",
	})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	httpWithErrorInfo := &googleapi.Error{
		Code:    http.StatusForbidden,
		Message: "Permission denied.",
		Body:    `{"error":{"code":403,"message":"Permission denied.","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"SERVICE_DISABLED","domain":"googleapis.com"}]}}`,
	}

	// The generated JSON client returns a googleapi.Error wrapping an APIError.
	generatedHTTPErr := &googleapi.Error{
		Code:    http.StatusForbidden,
		Message: "Permission denied.",
		Body:    `{"error":{"code":403,"message":"Permission denied.","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"SERVICE_DISABLED","domain":"storage.googleapis.com"}]}}`,
	}
	apiErr, ok := apierror.ParseError(generatedHTTPErr, false)
	if !ok {
		t.Fatal("apierror.ParseError failed")
	}
	generatedHTTPErr.Wrap(apiErr)

	tests := []struct {
		name       string
		err        error
		wantType   string // "" means the attribute must not be set.
		wantDomain string // "" means the attribute must not be set.
	}{
		{
			name:     "HTTP not found",
			err:      fmt.Errorf("%w: %w", ErrBucketNotExist, &googleapi.Error{Code: http.StatusNotFound, Message: "The specified bucket does not exist."}),
			wantType: "NOT_FOUND",
		},
		{
			name:     "HTTP precondition failed",
			err:      &googleapi.Error{Code: http.StatusPreconditionFailed, Message: "At least one of the pre-conditions you specified did not hold."},
			wantType: "FAILED_PRECONDITION",
		},
		{
			name: "HTTP XML error",
			err: &googleapi.Error{
				Code: http.StatusRequestedRangeNotSatisfiable,
				Body: "<?xml version='1.0' encoding='UTF-8'?><Error><Code>InvalidRange</Code><Message>The requested range cannot be satisfied.</Message></Error>",
			},
			wantType: "OUT_OF_RANGE",
		},
		{
			name:       "HTTP error with ErrorInfo",
			err:        httpWithErrorInfo,
			wantType:   "PERMISSION_DENIED",
			wantDomain: "googleapis.com",
		},
		{
			name:     "gRPC not found",
			err:      fmt.Errorf("%w: %w", ErrObjectNotExist, status.Error(codes.NotFound, "No such object: bucket/obj")),
			wantType: "NOT_FOUND",
		},
		{
			name:       "gRPC error with ErrorInfo",
			err:        grpcWithErrorInfo.Err(),
			wantType:   "UNIMPLEMENTED",
			wantDomain: "storage.googleapis.com",
		},
		{
			name:       "wrapped gRPC error with ErrorInfo",
			err:        fmt.Errorf("storage: write failed: %w", grpcWithErrorInfo.Err()),
			wantType:   "UNIMPLEMENTED",
			wantDomain: "storage.googleapis.com",
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			wantType: "CANCELLED",
		},
		{
			name:     "deadline exceeded",
			err:      context.DeadlineExceeded,
			wantType: "TIMEOUT",
		},
		{
			name:     "client-side error",
			err:      errors.New("storage: bucket name is empty"),
			wantType: "UNKNOWN",
		},
		{
			name:       "HTTP error with ErrorInfo from the generated client",
			err:        generatedHTTPErr,
			wantType:   "PERMISSION_DENIED",
			wantDomain: "storage.googleapis.com",
		},
		{
			name:       "wrapped HTTP error with ErrorInfo from the generated client",
			err:        fmt.Errorf("%w: %w", ErrBucketNotExist, generatedHTTPErr),
			wantType:   "NOT_FOUND",
			wantDomain: "storage.googleapis.com",
		},
		{
			name:     "bare ErrObjectNotExist",
			err:      ErrObjectNotExist,
			wantType: "NOT_FOUND",
		},
		{
			name:     "bare ErrBucketNotExist",
			err:      ErrBucketNotExist,
			wantType: "NOT_FOUND",
		},
		{
			name:     "gRPC not found with key in object name",
			err:      fmt.Errorf("%w: %w", ErrObjectNotExist, status.Error(codes.NotFound, "No such object: bucket/my-key")),
			wantType: "NOT_FOUND",
		},
		{
			name:     "gRPC not found with timeout in object name",
			err:      status.Error(codes.NotFound, "No such object: bucket/timeout.log"),
			wantType: "NOT_FOUND",
		},
		{
			name:     "HTTP not found with eof in object name",
			err:      &googleapi.Error{Code: http.StatusNotFound, Message: "No such object: bucket/geofence.json"},
			wantType: "NOT_FOUND",
		},
		{
			name:     "gRPC invalid page token",
			err:      status.Error(codes.InvalidArgument, "Invalid page token."),
			wantType: "INVALID_ARGUMENT",
		},
		{
			name:     "gRPC unavailable with network error message",
			err:      status.Error(codes.Unavailable, "connection error: dial tcp: connection refused"),
			wantType: "CONNECTION_ERROR",
		},
		{
			name:     "gRPC unavailable from the service",
			err:      status.Error(codes.Unavailable, "The service is currently unavailable."),
			wantType: "UNAVAILABLE",
		},
		{
			name:     "nil error sets no attributes",
			err:      nil,
			wantType: "",
		},
		{
			name:     "io.EOF does not set error.type",
			err:      io.EOF,
			wantType: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := map[attribute.Key]string{}
			for _, kv := range errorAttributes(tc.err) {
				if kv.Key == "error.message" {
					continue // covered by TestExtractErrorMessage
				}
				got[kv.Key] = kv.Value.AsString()
			}
			want := map[attribute.Key]string{}
			if tc.wantType != "" {
				want["error.type"] = tc.wantType
			}
			if tc.wantDomain != "" {
				want["gcp.errors.domain"] = tc.wantDomain
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("errorAttributes(%v) mismatch (-want +got):\n%s", tc.err, diff)
			}
		})
	}
}

func TestStorageTraceEndSpanErrorAttributes(t *testing.T) {
	ctx := context.Background()
	grpcWithErrorInfo, err := status.New(codes.PermissionDenied, "Permission denied.").WithDetails(&errdetails.ErrorInfo{
		Reason: "SERVICE_DISABLED",
		Domain: "storage.googleapis.com",
	})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}

	tests := []struct {
		name      string
		otelOn    bool
		err       error
		wantAttrs map[attribute.Key]string
	}{
		{
			name:      "error sets error.type and gcp.errors.domain",
			otelOn:    true,
			err:       grpcWithErrorInfo.Err(),
			wantAttrs: map[attribute.Key]string{"error.type": "PERMISSION_DENIED", "gcp.errors.domain": "storage.googleapis.com"},
		},
		{
			name:      "gRPC not found error sets error.type",
			otelOn:    true,
			err:       status.Error(codes.NotFound, "No such object: bucket/obj"),
			wantAttrs: map[attribute.Key]string{"error.type": "NOT_FOUND"},
		},
		{
			name:      "HTTP not found error sets error.type",
			otelOn:    true,
			err:       fmt.Errorf("%w: %w", ErrBucketNotExist, &googleapi.Error{Code: http.StatusNotFound, Message: "The specified bucket does not exist."}),
			wantAttrs: map[attribute.Key]string{"error.type": "NOT_FOUND"},
		},
		{
			name:      "nil error sets no error attributes",
			otelOn:    true,
			err:       nil,
			wantAttrs: map[attribute.Key]string{},
		},
		{
			// The legacy internalTrace path still ends a span through the
			// global provider; it must not get the new attributes.
			name:      "legacy internalTrace path unchanged",
			otelOn:    false,
			err:       grpcWithErrorInfo.Err(),
			wantAttrs: map[attribute.Key]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.otelOn {
				t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")
			} else {
				t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "false")
			}
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			spanCtx, _ := startSpan(ctx, "Object.Attrs")
			endSpan(spanCtx, tc.err)

			spans := te.Spans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}
			got := map[attribute.Key]string{}
			for _, kv := range spans[0].Attributes {
				if kv.Key == "error.type" || kv.Key == "gcp.errors.domain" {
					got[kv.Key] = kv.Value.AsString()
				}
			}
			if diff := cmp.Diff(tc.wantAttrs, got); diff != "" {
				t.Errorf("error attributes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// spanAttr returns the string value of the attribute key on s.
func spanAttr(s tracetest.SpanStub, key attribute.Key) (string, bool) {
	for _, kv := range s.Attributes {
		if kv.Key == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

func TestStorageTraceErrorMessage(t *testing.T) {
	ctx := context.Background()
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	apiErrGRPC, ok := apierror.FromError(status.Error(codes.FailedPrecondition, "This bucket requires appendable objects."))
	if !ok {
		t.Fatal("apierror.FromError failed for gRPC status")
	}
	apiErrHTTP, ok := apierror.FromError(&googleapi.Error{Code: http.StatusPreconditionFailed, Message: "Precondition Failed"})
	if !ok {
		t.Fatal("apierror.FromError failed for googleapi.Error")
	}

	tests := []struct {
		name    string
		err     error
		wantMsg string // "" means the attribute must not be set.
	}{
		{
			name:    "nil error does not set error.message",
			err:     nil,
			wantMsg: "",
		},
		{
			name:    "googleapi.Error extracts Message",
			err:     &googleapi.Error{Code: http.StatusBadRequest, Message: "This bucket requires appendable objects."},
			wantMsg: "This bucket requires appendable objects.",
		},
		{
			name:    "gRPC status.Error extracts Message",
			err:     status.Error(codes.FailedPrecondition, "This bucket requires appendable objects."),
			wantMsg: "This bucket requires appendable objects.",
		},
		{
			name:    "apierror.APIError wrapping gRPC status",
			err:     apiErrGRPC,
			wantMsg: "This bucket requires appendable objects.",
		},
		{
			name:    "apierror.APIError wrapping googleapi.Error",
			err:     apiErrHTTP,
			wantMsg: "Precondition Failed",
		},
		{
			name:    "wrapped gRPC status.Error preserves inner message",
			err:     fmt.Errorf("%w: %w", ErrObjectNotExist, status.Error(codes.NotFound, "No such object: bucket/obj")),
			wantMsg: "No such object: bucket/obj",
		},
		{
			name:    "sentinel error falls back to Error()",
			err:     ErrBucketNotExist,
			wantMsg: ErrBucketNotExist.Error(),
		},
		{
			name:    "plain error falls back to Error()",
			err:     errors.New("storage: bucket name is empty"),
			wantMsg: "storage: bucket name is empty",
		},
		{
			name: "XML API error body extracts Message element",
			err: &googleapi.Error{
				Code: http.StatusRequestedRangeNotSatisfiable,
				Body: "<?xml version='1.0' encoding='UTF-8'?><Error><Code>InvalidRange</Code><Message>The requested range cannot be satisfied.</Message><Details>bytes=104857600-104857727</Details></Error>",
			},
			wantMsg: "The requested range cannot be satisfied.",
		},
		{
			name: "wrapped XML API error body extracts Message element",
			err: fmt.Errorf("storage: read failed: %w", &googleapi.Error{
				Code: http.StatusPreconditionFailed,
				Body: "<?xml version='1.0' encoding='UTF-8'?><Error><Code>ConditionNotMet</Code><Message>At least one of the pre-conditions you specified did not hold.</Message></Error>",
			}),
			wantMsg: "At least one of the pre-conditions you specified did not hold.",
		},
		{
			name:    "googleapi.Error with non-XML body falls back to status text",
			err:     &googleapi.Error{Code: http.StatusServiceUnavailable, Body: "upstream connect error"},
			wantMsg: "Service Unavailable",
		},
		{
			name:    "googleapi.Error with empty body falls back to status text",
			err:     &googleapi.Error{Code: http.StatusNotFound},
			wantMsg: "Not Found",
		},
		{
			name:    "googleapi.Error with unknown status code does not embed body",
			err:     &googleapi.Error{Code: 499, Body: "client closed request"},
			wantMsg: "HTTP 499",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			spanCtx, _ := startSpan(ctx, "Object.Attrs")
			endSpan(spanCtx, tc.err)

			spans := te.Spans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}
			gotMsg, hasAttr := spanAttr(spans[0], "error.message")
			if wantHasAttr := tc.wantMsg != ""; hasAttr != wantHasAttr {
				t.Fatalf("got hasAttr=%v, want %v (attrs=%v)", hasAttr, wantHasAttr, spans[0].Attributes)
			}
			if gotMsg != tc.wantMsg {
				t.Errorf("got error.message %q, want %q", gotMsg, tc.wantMsg)
			}
		})
	}
}

func TestStorageTraceErrorDomain(t *testing.T) {
	ctx := context.Background()
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	grpcWithErrorInfo, err := status.New(codes.PermissionDenied, "Permission denied.").WithDetails(&errdetails.ErrorInfo{
		Reason: "SERVICE_DISABLED",
		Domain: "storage.googleapis.com",
	})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	apiErrGRPC, ok := apierror.FromError(grpcWithErrorInfo.Err())
	if !ok {
		t.Fatal("apierror.FromError failed for gRPC status")
	}

	tests := []struct {
		name       string
		err        error
		wantDomain string // "" means the attribute must not be set.
	}{
		{
			name:       "nil error",
			err:        nil,
			wantDomain: "",
		},
		{
			name:       "gRPC status with ErrorInfo",
			err:        grpcWithErrorInfo.Err(),
			wantDomain: "storage.googleapis.com",
		},
		{
			name:       "wrapped gRPC status with ErrorInfo",
			err:        fmt.Errorf("storage: retry failed: %w", grpcWithErrorInfo.Err()),
			wantDomain: "storage.googleapis.com",
		},
		{
			name:       "apierror.APIError with ErrorInfo",
			err:        apiErrGRPC,
			wantDomain: "storage.googleapis.com",
		},
		{
			name: "HTTP JSON error with ErrorInfo details",
			err: &googleapi.Error{
				Code:    http.StatusForbidden,
				Message: "Permission denied.",
				Body:    `{"error":{"code":403,"message":"Permission denied.","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"SERVICE_DISABLED","domain":"storage.googleapis.com"}]}}`,
			},
			wantDomain: "storage.googleapis.com",
		},
		{
			name: "HTTP JSON error with legacy errors[].domain is not used",
			err: &googleapi.Error{
				Code:    http.StatusNotFound,
				Message: "The specified bucket does not exist.",
				Body:    `{"error":{"code":404,"message":"The specified bucket does not exist.","errors":[{"message":"The specified bucket does not exist.","domain":"global","reason":"notFound"}]}}`,
			},
			wantDomain: "",
		},
		{
			name:       "gRPC status without ErrorInfo",
			err:        status.Error(codes.NotFound, "No such object: bucket/obj"),
			wantDomain: "",
		},
		{
			name: "HTTP XML error",
			err: &googleapi.Error{
				Code: http.StatusRequestedRangeNotSatisfiable,
				Body: "<?xml version='1.0' encoding='UTF-8'?><Error><Code>InvalidRange</Code><Message>The requested range cannot be satisfied.</Message></Error>",
			},
			wantDomain: "",
		},
		{
			name:       "client-side context error",
			err:        context.Canceled,
			wantDomain: "",
		},
		{
			name:       "sentinel error",
			err:        ErrObjectNotExist,
			wantDomain: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			spanCtx, _ := startSpan(ctx, "Object.Attrs")
			endSpan(spanCtx, tc.err)

			spans := te.Spans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}
			gotDomain, hasAttr := spanAttr(spans[0], "gcp.errors.domain")
			if wantHasAttr := tc.wantDomain != ""; hasAttr != wantHasAttr {
				t.Fatalf("got hasAttr=%v, want %v (attrs=%v)", hasAttr, wantHasAttr, spans[0].Attributes)
			}
			if gotDomain != tc.wantDomain {
				t.Errorf("got gcp.errors.domain %q, want %q", gotDomain, tc.wantDomain)
			}
		})
	}
}

func TestStorageTraceRPCSystemNameAttribute(t *testing.T) {
	ctx := context.Background()
	t.Setenv(storageOtelTracingDevVar, "true")

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	rangeParams := &newRangeReaderParams{
		bucket: "my-bucket",
		object: "my-object",
	}

	tests := []struct {
		name string
		emit func()
		want string
	}{
		{
			name: "HTTP operation span",
			emit: func() {
				c := &Client{rpcSystem: "http"}
				sCtx, _ := startSpanWithBucket(ctx, c, "my-bucket", "Bucket.Attrs")
				endSpan(sCtx, nil)
			},
			want: "http",
		},
		{
			name: "gRPC operation span",
			emit: func() {
				c := &Client{rpcSystem: "grpc"}
				sCtx, _ := startSpanWithBucket(ctx, c, "my-bucket", "Bucket.Attrs")
				endSpan(sCtx, nil)
			},
			want: "grpc",
		},
		{
			name: "httpStorageClient.NewRangeReader",
			emit: func() {
				hc := &httpStorageClient{
					settings: &settings{},
					config:   &storageConfig{},
				}
				_, _ = hc.NewRangeReader(canceledCtx, rangeParams)
			},
			want: "http",
		},
		{
			name: "grpcStorageClient.NewRangeReader",
			emit: func() {
				gc := &grpcStorageClient{
					settings: &settings{},
					config:   &storageConfig{grpcBidiReads: true},
				}
				_, _ = gc.NewRangeReader(canceledCtx, rangeParams)
			},
			want: "grpc",
		},
		{
			name: "grpcStorageClient.NewRangeReaderReadObject",
			emit: func() {
				gc := &grpcStorageClient{
					settings: &settings{},
					config:   &storageConfig{grpcBidiReads: false},
				}
				_, _ = gc.NewRangeReader(canceledCtx, rangeParams)
			},
			want: "grpc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			tc.emit()

			spans := te.Spans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}

			if got := spanStringAttribute(spans[0], "rpc.system.name"); got != tc.want {
				t.Errorf("rpc.system.name = %q, want %q", got, tc.want)
			}
		})
	}
}

func spanStringAttribute(span tracetest.SpanStub, key attribute.Key) string {
	for _, attr := range span.Attributes {
		if attr.Key == key {
			return attr.Value.AsString()
		}
	}
	return ""
}

// findSpans returns the spans in spans named with the package-qualified form
// of name.
func findSpans(spans tracetest.SpanStubs, name string) tracetest.SpanStubs {
	want := appendPackageName(name)
	var found tracetest.SpanStubs
	for _, s := range spans {
		if s.Name == want {
			found = append(found, s)
		}
	}
	return found
}

// checkRetryBackoffs checks that the single Bucket.Attrs span in spans has
// wantBackoffs RetryBackoff child spans, each lying within the parent span's
// time range.
func checkRetryBackoffs(t *testing.T, spans tracetest.SpanStubs, wantBackoffs int) {
	t.Helper()
	parents := findSpans(spans, "Bucket.Attrs")
	if len(parents) != 1 {
		t.Fatalf("got %d Bucket.Attrs spans, want 1", len(parents))
	}
	parent := parents[0]
	backoffs := findSpans(spans, "RetryBackoff")
	if len(backoffs) != wantBackoffs {
		t.Errorf("got %d RetryBackoff spans, want %d", len(backoffs), wantBackoffs)
	}
	for _, s := range backoffs {
		if got, want := s.Parent.SpanID(), parent.SpanContext.SpanID(); got != want {
			t.Errorf("RetryBackoff span parent ID = %v, want %v", got, want)
		}
		if s.StartTime.After(s.EndTime) {
			t.Errorf("RetryBackoff span start time %v is after end time %v", s.StartTime, s.EndTime)
		}
		if s.StartTime.Before(parent.StartTime) || s.EndTime.After(parent.EndTime) {
			t.Errorf("RetryBackoff span [%v, %v] is outside parent span [%v, %v]", s.StartTime, s.EndTime, parent.StartTime, parent.EndTime)
		}
	}
}

func TestRecordRetryBackoffNoParentSpan(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	recordRetryBackoff(ctx, time.Now())

	if got := len(te.Spans()); got != 0 {
		t.Errorf("recordRetryBackoff() without a parent span exported %d spans, want 0", got)
	}
}

func TestRunRetryBackoffTracing(t *testing.T) {
	fastBackoff := &gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond}
	maxAttempts := 2
	retryableErr := &googleapi.Error{Code: http.StatusServiceUnavailable}
	nonRetryableErr := &googleapi.Error{Code: http.StatusBadRequest}

	for _, tc := range []struct {
		name  string
		retry *retryConfig
		// errs[i] is returned by attempt i+1. Later attempts succeed.
		errs []error
		// timeout, if set, is applied to the context passed to run.
		timeout time.Duration
		wantErr bool
		// wantBackoffs is the number of RetryBackoff spans expected.
		wantBackoffs int
	}{
		{
			name:  "success on first attempt",
			retry: &retryConfig{backoff: fastBackoff},
		},
		{
			name:         "one retry",
			retry:        &retryConfig{backoff: fastBackoff},
			errs:         []error{retryableErr},
			wantBackoffs: 1,
		},
		{
			name:         "two retries",
			retry:        &retryConfig{backoff: fastBackoff},
			errs:         []error{retryableErr, retryableErr},
			wantBackoffs: 2,
		},
		{
			name:    "non-retryable error",
			retry:   &retryConfig{backoff: fastBackoff},
			errs:    []error{nonRetryableErr},
			wantErr: true,
		},
		{
			name:         "max attempts reached",
			retry:        &retryConfig{backoff: fastBackoff, maxAttempts: &maxAttempts},
			errs:         []error{retryableErr, retryableErr},
			wantErr:      true,
			wantBackoffs: 1,
		},
		{
			name: "context done during backoff",
			// gax picks a random pause in [1ns, Initial], so a huge Initial makes
			// a pause shorter than the timeout practically impossible.
			retry:        &retryConfig{backoff: &gax.Backoff{Initial: 1000 * time.Hour, Max: 1000 * time.Hour}},
			errs:         []error{retryableErr},
			timeout:      50 * time.Millisecond,
			wantErr:      true,
			wantBackoffs: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})
			t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

			spanCtx, _ := startSpan(ctx, "Bucket.Attrs")
			runCtx := spanCtx
			if tc.timeout > 0 {
				var cancel context.CancelFunc
				runCtx, cancel = context.WithTimeout(spanCtx, tc.timeout)
				defer cancel()
			}
			attempt := 0
			err := run(runCtx, func(context.Context) error {
				attempt++
				if attempt <= len(tc.errs) {
					return tc.errs[attempt-1]
				}
				return nil
			}, tc.retry, true)
			endSpan(spanCtx, err)

			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("run() = %v, want error: %t", err, tc.wantErr)
			}
			checkRetryBackoffs(t, te.Spans(), tc.wantBackoffs)
		})
	}
}

func TestRunRetryBackoffTracingDisabled(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "false")

	// The span is started directly so that it is recording even though dev
	// tracing is disabled, proving that the environment gate alone prevents
	// the backoff from being recorded.
	spanCtx, span := tracer().Start(ctx, "Bucket.Attrs")
	attempt := 0
	err := run(spanCtx, func(context.Context) error {
		attempt++
		if attempt == 1 {
			return &googleapi.Error{Code: http.StatusServiceUnavailable}
		}
		return nil
	}, &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond}}, true)
	span.End()

	if err != nil {
		t.Fatalf("run() = %v, want nil", err)
	}
	spans := te.Spans()
	if len(spans) != 1 {
		t.Errorf("run() produced %d spans, want 1", len(spans))
	}
}

type staticAuthTokenProvider struct {
	token *auth.Token
	err   error
}

func (s *staticAuthTokenProvider) Token(ctx context.Context) (*auth.Token, error) {
	return s.token, s.err
}

func findSpansByName(spans tracetest.SpanStubs, name string) tracetest.SpanStubs {
	var matched tracetest.SpanStubs
	for _, s := range spans {
		if s.Name == name {
			matched = append(matched, s)
		}
	}
	return matched
}

func TestWrapAuthCredentialsForTracing(t *testing.T) {
	if wrapAuthCredentialsForTracing(nil) != nil {
		t.Errorf("expected nil when c is nil")
	}
	c := &auth.Credentials{}
	if wrapAuthCredentialsForTracing(c) != c {
		t.Errorf("expected original credentials when TokenProvider is nil")
	}
	c.TokenProvider = &staticAuthTokenProvider{token: &auth.Token{Value: "fake-token"}}
	wrapped := wrapAuthCredentialsForTracing(c)
	if wrapped == c {
		t.Errorf("expected a new copy of credentials, got original")
	}
	if _, ok := wrapped.TokenProvider.(*tracedTokenProvider); !ok {
		t.Errorf("expected TokenProvider to be *tracedTokenProvider")
	}
	wrapped2 := wrapAuthCredentialsForTracing(wrapped)
	if wrapped2 != wrapped {
		t.Errorf("expected original wrapped credentials on double wrap")
	}
	metricsWrapped := wrapAuthCredentials(wrapped, &clientMetrics{})
	if wrapAuthCredentialsForTracing(metricsWrapped) != metricsWrapped {
		t.Errorf("expected original credentials when already wrapped inside metricsTokenProvider")
	}
}

func TestTracedTokenProviderSpan(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	creds := wrapAuthCredentialsForTracing(&auth.Credentials{
		TokenProvider: &staticAuthTokenProvider{token: &auth.Token{Value: "fake-token"}},
	})

	parentCtx, parentSpan := startSpan(ctx, "Object.Writer")
	tok, err := creds.Token(parentCtx)
	endSpan(parentCtx, nil)
	if err != nil {
		t.Fatalf("creds.Token: %v", err)
	}
	if tok.Value != "fake-token" {
		t.Fatalf("got access token %q, want %q", tok.Value, "fake-token")
	}

	const wantSpanName = "cloud.google.com/go/storage.Auth.RefreshAccessToken"
	authSpans := findSpansByName(te.Spans(), wantSpanName)
	if len(authSpans) != 1 {
		t.Fatalf("expected 1 %q span, got %d", wantSpanName, len(authSpans))
	}
	if got, want := authSpans[0].Parent.SpanID(), parentSpan.SpanContext().SpanID(); got != want {
		t.Errorf("got parent span ID %v, want %v", got, want)
	}
}

func TestTracedTokenProviderError(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	wantErr := errors.New("token refresh failed")
	creds := wrapAuthCredentialsForTracing(&auth.Credentials{
		TokenProvider: &staticAuthTokenProvider{err: wantErr},
	})

	_, err := creds.Token(ctx)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}

	const wantSpanName = "cloud.google.com/go/storage.Auth.RefreshAccessToken"
	authSpans := findSpansByName(te.Spans(), wantSpanName)
	if len(authSpans) != 1 {
		t.Fatalf("expected 1 %q span, got %d", wantSpanName, len(authSpans))
	}
	span := authSpans[0]
	if span.Status.Code != otcodes.Error {
		t.Errorf("got span status code %v, want %v", span.Status.Code, otcodes.Error)
	}
	if span.Status.Description != wantErr.Error() {
		t.Errorf("got span status description %q, want %q", span.Status.Description, wantErr.Error())
	}
	hasExceptionEvent := false
	for _, ev := range span.Events {
		if ev.Name == "exception" {
			hasExceptionEvent = true
			break
		}
	}
	if !hasExceptionEvent {
		t.Errorf("expected exception event on error span, got events: %+v", span.Events)
	}
}

func TestTracedTokenProviderCacheHit(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

	cachedTok := &auth.Token{Value: "cached-token"}
	provider := &staticAuthTokenProvider{token: cachedTok}
	creds := wrapAuthCredentialsForTracing(&auth.Credentials{
		TokenProvider: provider,
	})

	// First fetch obtains a new token -> emits 1 span.
	if _, err := creds.Token(ctx); err != nil {
		t.Fatalf("first creds.Token: %v", err)
	}
	const wantSpanName = "cloud.google.com/go/storage.Auth.RefreshAccessToken"
	if got := len(findSpansByName(te.Spans(), wantSpanName)); got != 1 {
		t.Fatalf("after first fetch: expected 1 %q span, got %d", wantSpanName, got)
	}

	// Second fetch returns the same *auth.Token pointer (cache hit) -> no new span.
	if _, err := creds.Token(ctx); err != nil {
		t.Fatalf("second creds.Token: %v", err)
	}
	if got := len(findSpansByName(te.Spans(), wantSpanName)); got != 1 {
		t.Fatalf("after cache hit: expected 1 %q span, got %d", wantSpanName, got)
	}

	// Third fetch returns a newly refreshed *auth.Token pointer -> emits a 2nd span.
	provider.token = &auth.Token{Value: "refreshed-token"}
	if _, err := creds.Token(ctx); err != nil {
		t.Fatalf("third creds.Token: %v", err)
	}
	if got := len(findSpansByName(te.Spans(), wantSpanName)); got != 2 {
		t.Fatalf("after refresh: expected 2 %q spans, got %d", wantSpanName, got)
	}
}

func TestHTTPClientAuthTracingWithMetrics(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")
	t.Setenv("STORAGE_EMULATOR_HOST", "")

	mr := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(ctx)
	})

	baseProvider := &staticAuthTokenProvider{token: &auth.Token{Value: "fake-token"}}
	baseCreds := &auth.Credentials{TokenProvider: baseProvider}
	client, err := NewClient(ctx,
		option.WithAuthCredentials(baseCreds),
		withOtelMetrics(),
		withOtelDebugMetrics(),
		withMeterProvider(meterProvider),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	// Caller-owned credentials must not be mutated by client construction.
	if baseCreds.TokenProvider != auth.TokenProvider(baseProvider) {
		t.Errorf("caller-owned credentials were mutated: got TokenProvider %T, want %T", baseCreds.TokenProvider, baseProvider)
	}

	var hc *httpStorageClient
	switch tc := client.tc.(type) {
	case *metricsStorageClient:
		hc = tc.storageClient.(*httpStorageClient)
	case *httpStorageClient:
		hc = tc
	default:
		t.Fatalf("unexpected storageClient type: %T", client.tc)
	}
	if hc.creds == baseCreds {
		t.Errorf("httpStorageClient.creds aliases caller-owned credentials; want a clone")
	}
	// The HTTP transport built in NewClient observes the wrapped provider:
	// this branch installs it in NewClient before the http.Client is built
	// (the auth transport then wraps it in its own cache). The functional
	// check below verifies that a fetch through hc.creds emits one span.
	if _, err := hc.creds.Token(ctx); err != nil {
		t.Fatalf("hc.creds.Token: %v", err)
	}

	const wantSpanName = "cloud.google.com/go/storage.Auth.RefreshAccessToken"
	authSpans := findSpansByName(te.Spans(), wantSpanName)
	if len(authSpans) != 1 {
		t.Fatalf("expected exactly 1 %q span per fetch with metrics+tracing enabled, got %d", wantSpanName, len(authSpans))
	}
}

func TestTracedTokenProviderDevTracingDisabled(t *testing.T) {
	ctx := context.Background()
	te := testutil.NewOpenTelemetryTestExporter()
	t.Cleanup(func() {
		te.Unregister(ctx)
	})
	t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "false")

	baseCreds := &auth.Credentials{
		TokenProvider: &staticAuthTokenProvider{token: &auth.Token{Value: "fake-token"}},
	}
	sc, err := newHTTPStorageClient(ctx, withClientOptions(
		option.WithAuthCredentials(baseCreds),
	))
	if err != nil {
		t.Fatalf("newHTTPStorageClient: %v", err)
	}
	t.Cleanup(func() {
		_ = sc.Close()
	})

	hc := sc.(*httpStorageClient)
	tok, err := hc.creds.Token(ctx)
	if err != nil {
		t.Fatalf("hc.creds.Token: %v", err)
	}
	if tok.Value != "fake-token" {
		t.Fatalf("got access token %q, want %q", tok.Value, "fake-token")
	}

	const wantSpanName = "cloud.google.com/go/storage.Auth.RefreshAccessToken"
	if got := len(findSpansByName(te.Spans(), wantSpanName)); got > 0 {
		t.Fatalf("expected 0 %q spans when dev tracing is disabled, got %d", wantSpanName, got)
	}
}

func TestSetUploadIDAttribute(t *testing.T) {
	const sampleUploadID = "AJjja9aXX4R20FIZ2ddLnYddhJmD9Xm_Y2B_NEu5G2a6PzJiGRgPKLp6hEQpM"
	spanName := "Object.Writer"

	tests := []struct {
		name         string
		devTracing   string
		uploadID     string
		wantUploadID string
		wantAttrs    []attribute.KeyValue
	}{
		{
			name:         "Records upload.id when dev tracing enabled",
			devTracing:   "true",
			uploadID:     sampleUploadID,
			wantUploadID: sampleUploadID,
			wantAttrs:    append(getCommonAttributes(), attribute.String("gcp.storage.upload.id", sampleUploadID)),
		},
		{
			name:         "Omits upload.id when upload_id is empty",
			devTracing:   "true",
			uploadID:     "",
			wantUploadID: "",
			wantAttrs:    getCommonAttributes(),
		},
		{
			name:         "Omits upload.id when dev tracing disabled",
			devTracing:   "false",
			uploadID:     sampleUploadID,
			wantUploadID: "",
			wantAttrs:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			// TODO: Remove setting development env var upon launch.
			t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", tc.devTracing)

			ctx, _ = startSpan(ctx, spanName)
			setUploadIDAttribute(ctx, tc.uploadID)
			endSpan(ctx, nil)

			spans := te.Spans()
			if len(spans) != 1 {
				t.Fatalf("expected one span, got %d", len(spans))
			}
			gotSpan := spans[0]
			if got, want := gotSpan.Name, appendPackageName(spanName); got != want {
				t.Fatalf("got %s, want %s", got, want)
			}

			if tc.devTracing == "true" {
				wantSpan := createWantSpanStub(spanName, tc.wantAttrs)
				opts := []cmp.Option{
					cmp.Comparer(spanAttributesComparer),
				}
				if diff := testutil.Diff(gotSpan, wantSpan, opts...); diff != "" {
					t.Errorf("diff: -got, +want:\n%s\n", diff)
				}
			} else if len(gotSpan.Attributes) != 0 {
				t.Errorf("expected no attributes when dev tracing is disabled, got %v", gotSpan.Attributes)
			}
			verifyUploadIDAttribute(t, gotSpan, tc.wantUploadID)
		})
	}
}

func TestTrackingTransportUploadID(t *testing.T) {
	const sampleUploadID = "AJjja9aXX4R20FIZ2ddLnYddhJmD9Xm_Y2B_NEu5G2a6PzJiGRgPKLp6hEQpM"
	spanName := "Object.Writer"

	tests := []struct {
		name         string
		reqURL       string
		respHeader   http.Header
		wantUploadID string
		wantAttrs    []attribute.KeyValue
	}{
		{
			name:   "Extracts upload_id from Location response header",
			reqURL: "http://example.com/upload/storage/v1/b/my-bucket/o?uploadType=resumable",
			respHeader: http.Header{
				"Location": {"http://example.com/upload/storage/v1/b/my-bucket/o?uploadType=resumable&upload_id=" + sampleUploadID},
			},
			wantUploadID: sampleUploadID,
			wantAttrs:    append(getCommonAttributes(), attribute.String("gcp.storage.upload.id", sampleUploadID)),
		},
		{
			name:         "Omits upload.id when Location header not present",
			reqURL:       "http://example.com/storage/v1/b/my-bucket/o",
			wantUploadID: "",
			wantAttrs:    getCommonAttributes(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testutil.NewOpenTelemetryTestExporter()
			t.Cleanup(func() {
				te.Unregister(ctx)
			})

			// TODO: Remove setting development env var upon launch.
			t.Setenv("GO_STORAGE_DEV_OTEL_TRACING", "true")

			mock := &mockTransport{}
			mock.addResult(&http.Response{Status: "200 OK", Header: tc.respHeader}, nil)
			tt := &trackingTransport{base: mock}

			ctx, _ = startSpan(ctx, spanName)
			req, err := http.NewRequestWithContext(ctx, "POST", tc.reqURL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext failed: %v", err)
			}
			if _, err := tt.RoundTrip(req); err != nil {
				t.Fatalf("RoundTrip failed: %v", err)
			}
			endSpan(ctx, nil)

			spans := te.Spans()
			if len(spans) != 1 {
				t.Fatalf("expected one span, got %d", len(spans))
			}
			gotSpan := spans[0]
			wantSpan := createWantSpanStub(spanName, tc.wantAttrs)
			opts := []cmp.Option{
				cmp.Comparer(spanAttributesComparer),
			}
			if diff := testutil.Diff(gotSpan, wantSpan, opts...); diff != "" {
				t.Errorf("diff: -got, +want:\n%s\n", diff)
			}
			verifyUploadIDAttribute(t, gotSpan, tc.wantUploadID)
		})
	}
}

func verifyUploadIDAttribute(t *testing.T, span tracetest.SpanStub, wantUploadID string) {
	t.Helper()
	var gotUploadID string
	for _, attr := range span.Attributes {
		if attr.Key == "gcp.storage.upload.id" {
			gotUploadID = attr.Value.AsString()
		}
	}
	if gotUploadID != wantUploadID {
		t.Errorf("got upload_id %q, want %q", gotUploadID, wantUploadID)
	}
}
