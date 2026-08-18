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
	"sync"
	"testing"
)

func TestMemoryGuardrailAcquireRelease(t *testing.T) {
	limit := int64(10 * 1024 * 1024) // 10 MiB
	mg := NewMemoryGuardrail(limit)

	if mg.Limit() != limit {
		t.Fatalf("expected limit %d, got %d", limit, mg.Limit())
	}
	if mg.InUse() != 0 {
		t.Fatalf("expected 0 in use, got %d", mg.InUse())
	}

	// Acquire 4 MiB
	if !mg.TryAcquire(4 * 1024 * 1024) {
		t.Fatalf("failed to acquire 4 MiB")
	}
	if mg.InUse() != 4*1024*1024 {
		t.Fatalf("expected 4 MiB in use, got %d", mg.InUse())
	}

	// Acquire another 5 MiB (total 9 MiB)
	if !mg.TryAcquire(5 * 1024 * 1024) {
		t.Fatalf("failed to acquire 5 MiB")
	}

	// Try acquiring 2 MiB (should fail as 9 + 2 > 10)
	if mg.TryAcquire(2 * 1024 * 1024) {
		t.Fatalf("expected acquisition to fail when exceeding limit")
	}

	// Release 5 MiB
	mg.Release(5 * 1024 * 1024)
	if mg.InUse() != 4*1024*1024 {
		t.Fatalf("expected 4 MiB in use after release, got %d", mg.InUse())
	}

	// Now acquiring 2 MiB should succeed (4 + 2 <= 10)
	if !mg.TryAcquire(2 * 1024 * 1024) {
		t.Fatalf("expected acquisition of 2 MiB to succeed after release")
	}
}

func TestMemoryGuardrailConcurrent(t *testing.T) {
	limit := int64(64 * 1024 * 1024) // 64 MiB
	mg := NewMemoryGuardrail(limit)

	chunkSize := int64(1024 * 1024) // 1 MiB
	numGoroutines := 100
	iterations := 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if mg.TryAcquire(chunkSize) {
					// Simulate short work
					inUse := mg.InUse()
					if inUse > limit {
						t.Errorf("memory limit exceeded: %d > %d", inUse, limit)
					}
					mg.Release(chunkSize)
				}
			}
		}()
	}

	wg.Wait()

	if mg.InUse() != 0 {
		t.Fatalf("expected 0 in use after all goroutines finished, got %d", mg.InUse())
	}
}

func TestMemoryGuardrailBufferPool(t *testing.T) {
	limit := int64(16 * 1024 * 1024)
	mg := NewMemoryGuardrail(limit)

	buf1 := mg.GetBuffer(2 * 1024 * 1024) // 2 MiB -> matches 4 MiB slab
	if buf1 == nil {
		t.Fatalf("expected non-nil buffer")
	}
	if len(buf1) != 2*1024*1024 {
		t.Fatalf("expected len 2 MiB, got %d", len(buf1))
	}
	if cap(buf1) < 2*1024*1024 {
		t.Fatalf("expected cap >= 2 MiB, got %d", cap(buf1))
	}

	mg.PutBuffer(buf1)
	if mg.InUse() != 0 {
		t.Fatalf("expected 0 in use after PutBuffer, got %d", mg.InUse())
	}
}
