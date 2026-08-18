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
	"testing"
	"time"
)

func TestDynamicChunkSizer_Recommendations(t *testing.T) {
	cfg := DefaultDynamicChunkConfig()
	cfg.MinChunkSize = 256 * 1024
	cfg.MaxChunkSize = 64 * 1024 * 1024
	cfg.InitialChunkSize = 16 * 1024 * 1024

	sizer := NewDynamicChunkSizer(cfg, nil)

	// Small objects (< 8 MiB) should recommend single-shot (0)
	if s := sizer.RecommendInitialChunkSize(4 * 1024 * 1024); s != 0 {
		t.Fatalf("expected 0 (single shot) for 4 MiB object, got %d", s)
	}

	// Medium object (20 MiB)
	if s := sizer.RecommendInitialChunkSize(20 * 1024 * 1024); s != 8*1024*1024 {
		t.Fatalf("expected 8 MiB for 20 MiB object, got %d", s)
	}

	// Large object (1 GiB)
	if s := sizer.RecommendInitialChunkSize(1024 * 1024 * 1024); s != 64*1024*1024 {
		t.Fatalf("expected 64 MiB for 1 GiB object, got %d", s)
	}

	// Unknown size (hint <= 0)
	if s := sizer.RecommendInitialChunkSize(0); s != 16*1024*1024 {
		t.Fatalf("expected 16 MiB default for unknown size, got %d", s)
	}
}

func TestDynamicChunkSizer_AIMDAdaptation(t *testing.T) {
	cfg := DefaultDynamicChunkConfig()
	cfg.MinChunkSize = 256 * 1024
	cfg.MaxChunkSize = 64 * 1024 * 1024
	cfg.InitialChunkSize = 16 * 1024 * 1024
	cfg.TargetLatency = 500 * time.Millisecond

	sizer := NewDynamicChunkSizer(cfg, nil)

	// Transfer 16 MiB in 100ms (high bandwidth ~160 MB/s). Should increase chunk size.
	newSize := sizer.RecordChunkTransfer(16*1024*1024, 100*time.Millisecond, false)
	if newSize <= 16*1024*1024 {
		t.Fatalf("expected chunk size to increase after fast transfer, got %d", newSize)
	}

	// Another fast transfer -> should grow further towards max
	for i := 0; i < 5; i++ {
		newSize = sizer.RecordChunkTransfer(int64(newSize), 100*time.Millisecond, false)
	}
	if newSize != 64*1024*1024 {
		t.Fatalf("expected chunk size to reach max 64 MiB, got %d", newSize)
	}

	// Now simulate a stall / error -> Multiplicative decrease
	decreasedSize := sizer.RecordChunkTransfer(int64(newSize), 5*time.Second, true)
	if decreasedSize >= newSize {
		t.Fatalf("expected chunk size to decrease after error/stall, got %d >= %d", decreasedSize, newSize)
	}
	if decreasedSize%GoogleAPIBaseChunkMultiple != 0 {
		t.Fatalf("chunk size %d is not a multiple of 256K", decreasedSize)
	}
}
