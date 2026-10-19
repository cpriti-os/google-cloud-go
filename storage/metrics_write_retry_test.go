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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/api/option"
)

// A small JSON upload with the default ChunkSize (buffered in one chunk) and
// RetryAlways that gets 429 twice and then 200 is retried by the transport:
// attempts > operations and retries{error.type=RESOURCE_EXHAUSTED} must be
// recorded. (With ChunkSize = 0 the upload is a single unbuffered request that
// google-api-go-client never retries, so no retries can be recorded.)
func TestOneShotUploadRetriesRecorded(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"code":429,"message":"rate limited"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"config.json","bucket":"b","size":"10"}`)
	}))
	defer srv.Close()

	mr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	ctx := context.Background()
	c, err := NewClient(ctx, option.WithEndpoint(srv.URL), option.WithoutAuthentication(),
		withOtelMetrics(), withMeterProvider(mp))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	o := c.Bucket("b").Object("obsdemo/hot/config.json").Retryer(WithPolicy(RetryAlways), WithMaxAttempts(6))
	w := o.NewWriter(ctx) // default ChunkSize: small payload is buffered in one retryable chunk
	fmt.Fprint(w, `{"w":1}`)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v (calls=%d)", err, calls.Load())
	}

	if c, _ := metricPoints(t, mr, "gcp.storage.client.attempts", map[string]string{storageOperationAttr: "WriteObject"}); c != 3 {
		t.Errorf("attempts = %d, want 3", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.operations", map[string]string{storageOperationAttr: "WriteObject"}); c != 1 {
		t.Errorf("operations = %d, want 1", c)
	}
	if c, _ := metricPoints(t, mr, "gcp.storage.client.retries", map[string]string{"error.type": "RESOURCE_EXHAUSTED"}); c != 2 {
		t.Errorf("retries{RESOURCE_EXHAUSTED} = %d, want 2", c)
	}
}
