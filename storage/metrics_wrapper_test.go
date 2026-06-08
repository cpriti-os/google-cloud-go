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
	"testing"
)

type mockStorageClient struct {
	storageClient
	gotCtx context.Context
}

func (m *mockStorageClient) getSettings() *settings {
	return &settings{
		metricsContext: &metricsContext{
			instruments: &metricInstruments{},
		},
	}
}

func (m *mockStorageClient) GetBucket(ctx context.Context, bucket string, conds *BucketConditions, opts ...storageOption) (*BucketAttrs, error) {
	m.gotCtx = ctx
	return nil, nil
}

func TestMetricsWrappingClient(t *testing.T) {
	oldGate := enableMetricsDevelopmentGate
	enableMetricsDevelopmentGate = true
	defer func() { enableMetricsDevelopmentGate = oldGate }()

	mock := &mockStorageClient{}
	wrapped := wrapWithMetrics(mock)

	ctx := context.Background()
	_, _ = wrapped.GetBucket(ctx, "bucket", nil)

	if wrapped == mock {
		t.Errorf("wrapWithMetrics should wrap the client, but got original")
	}

	insts, ok := mock.gotCtx.Value(metricInstrumentsKey).(*metricInstruments)
	if !ok || insts == nil {
		t.Errorf("expected metricInstrumentsKey in context, but not found")
	}

	isGRPC, ok := mock.gotCtx.Value(transportKey).(bool)
	if !ok {
		t.Errorf("expected transportKey in context, but not found")
	}
	if isGRPC {
		t.Errorf("expected isGRPC to be false for mock client, got true")
	}
}
