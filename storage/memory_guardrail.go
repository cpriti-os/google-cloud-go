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
	"sync/atomic"
)

// DefaultGlobalMemoryLimit is the default global memory cap (256 MiB) allocated
// for adaptive prefetching and dynamic buffer pools across a GCS Client.
const DefaultGlobalMemoryLimit int64 = 256 * 1024 * 1024

// MemoryGuardrail tracks and enforces client-wide memory limits for prefetching
// and dynamic chunk buffering, preventing out-of-memory errors and GC pressure.
type MemoryGuardrail struct {
	maxMemoryBytes int64
	inUseBytes     atomic.Int64
	slabPool       *SlabPool
}

// NewMemoryGuardrail creates a new MemoryGuardrail with the specified byte limit.
// If maxMemoryBytes <= 0, DefaultGlobalMemoryLimit is used.
func NewMemoryGuardrail(maxMemoryBytes int64) *MemoryGuardrail {
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = DefaultGlobalMemoryLimit
	}
	return &MemoryGuardrail{
		maxMemoryBytes: maxMemoryBytes,
		slabPool:       NewSlabPool(),
	}
}

// TryAcquire attempts to reserve the requested number of bytes.
// Returns true if the reservation succeeded without exceeding the limit, false otherwise.
func (mg *MemoryGuardrail) TryAcquire(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	for {
		current := mg.inUseBytes.Load()
		if current+bytes > mg.maxMemoryBytes {
			return false
		}
		if mg.inUseBytes.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

// Release returns the reserved bytes back to the global budget.
func (mg *MemoryGuardrail) Release(bytes int64) {
	if bytes <= 0 {
		return
	}
	mg.inUseBytes.Add(-bytes)
}

// InUse returns the current bytes allocated under this guardrail.
func (mg *MemoryGuardrail) InUse() int64 {
	return mg.inUseBytes.Load()
}

// Limit returns the configured maximum memory limit in bytes.
func (mg *MemoryGuardrail) Limit() int64 {
	return mg.maxMemoryBytes
}

// Available returns the remaining available memory budget.
func (mg *MemoryGuardrail) Available() int64 {
	rem := mg.maxMemoryBytes - mg.inUseBytes.Load()
	if rem < 0 {
		return 0
	}
	return rem
}

// GetBuffer retrieves a recycled byte slice of at least the requested size from the slab pool
// if memory reservation succeeds. If memory reservation fails, it returns nil.
func (mg *MemoryGuardrail) GetBuffer(size int) []byte {
	slabSize := mg.slabPool.matchingSlabSize(size)
	if !mg.TryAcquire(int64(slabSize)) {
		return nil
	}
	buf := mg.slabPool.Get(size)
	return buf[:size]
}

// PutBuffer returns a buffer to the slab pool and releases its memory reservation.
func (mg *MemoryGuardrail) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	size := cap(buf)
	mg.slabPool.Put(buf)
	mg.Release(int64(size))
}

// SlabPool provides tiered recycling of byte slices via sync.Pool across common power-of-two sizes
// to eliminate GC allocations and heap fragmentation.
type SlabPool struct {
	pools map[int]*sync.Pool
}

// Standard slab sizes from 64 KiB up to 64 MiB.
var slabSizes = []int{
	64 * 1024,
	256 * 1024,
	1024 * 1024,
	4 * 1024 * 1024,
	16 * 1024 * 1024,
	32 * 1024 * 1024,
	64 * 1024 * 1024,
}

// NewSlabPool creates a new tiered SlabPool.
func NewSlabPool() *SlabPool {
	pools := make(map[int]*sync.Pool, len(slabSizes))
	for _, size := range slabSizes {
		s := size
		pools[s] = &sync.Pool{
			New: func() any {
				return make([]byte, s)
			},
		}
	}
	return &SlabPool{pools: pools}
}

func (sp *SlabPool) matchingSlabSize(size int) int {
	for _, s := range slabSizes {
		if s >= size {
			return s
		}
	}
	return size
}

// Get fetches a slice with capacity >= size.
func (sp *SlabPool) Get(size int) []byte {
	slab := sp.matchingSlabSize(size)
	if pool, ok := sp.pools[slab]; ok {
		buf := pool.Get().([]byte)
		return buf[:size]
	}
	return make([]byte, size)
}

// Put recycles a slice into the appropriate pool.
func (sp *SlabPool) Put(buf []byte) {
	slab := cap(buf)
	if pool, ok := sp.pools[slab]; ok {
		pool.Put(buf[:slab])
	}
}
