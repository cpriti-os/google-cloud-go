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
	"io"
	"testing"
	"time"
)

// BenchmarkDynamicChunkSizer_DecisionOverhead benchmarks the latency of calling
// RecommendInitialChunkSize and RecordChunkTransfer (ensuring sub-microsecond decision latency).
func BenchmarkDynamicChunkSizer_DecisionOverhead(b *testing.B) {
	guardrail := NewMemoryGuardrail(256 * 1024 * 1024)
	sizer := NewDynamicChunkSizer(DefaultDynamicChunkConfig(), guardrail)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = sizer.RecommendInitialChunkSize(100 * 1024 * 1024)
		_ = sizer.RecordChunkTransfer(16*1024*1024, 100*time.Millisecond, false)
	}
}

// BenchmarkMemoryGuardrail_GetPutBuffer measures slab buffer pool recycling efficiency.
func BenchmarkMemoryGuardrail_GetPutBuffer(b *testing.B) {
	guardrail := NewMemoryGuardrail(256 * 1024 * 1024)
	size := 8 * 1024 * 1024 // 8 MiB

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := guardrail.GetBuffer(size)
		if buf == nil {
			b.Fatal("failed to get buffer")
		}
		guardrail.PutBuffer(buf)
	}
}

// BenchmarkReadProcessing_StandardVsPrefetch compares reading a 64 MiB stream
// with simulated application compute processing (2ms compute per 2 MiB chunk)
// between standard sequential reading and adaptive prefetching.
func BenchmarkReadProcessing_Standard(b *testing.B) {
	totalSize := int64(64 * 1024 * 1024) // 64 MiB
	chunkSize := 2 * 1024 * 1024          // 2 MiB chunks
	simulatedNetworkDelay := 3 * time.Millisecond
	simulatedComputeDelay := 2 * time.Millisecond

	source := make([]byte, totalSize)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Standard sequential read loop: network read -> compute -> network read -> compute
		offset := int64(0)
		for offset < totalSize {
			// Simulate network fetch
			time.Sleep(simulatedNetworkDelay)
			end := offset + int64(chunkSize)
			if end > totalSize {
				end = totalSize
			}
			_ = source[offset:end]
			offset = end

			// Simulate application compute
			time.Sleep(simulatedComputeDelay)
		}
	}
}

func BenchmarkReadProcessing_AdaptivePrefetch(b *testing.B) {
	totalSize := int64(64 * 1024 * 1024) // 64 MiB
	chunkSize := 2 * 1024 * 1024          // 2 MiB chunks
	simulatedNetworkDelay := 3 * time.Millisecond
	simulatedComputeDelay := 2 * time.Millisecond

	source := make([]byte, totalSize)
	guardrail := NewMemoryGuardrail(64 * 1024 * 1024)

	fetcher := func(ctx context.Context, offset, length int64, dest []byte) (int, error) {
		time.Sleep(simulatedNetworkDelay)
		if offset >= totalSize {
			return 0, io.EOF
		}
		end := offset + length
		if end > totalSize {
			end = totalSize
		}
		n := copy(dest, source[offset:end])
		if offset+int64(n) >= totalSize {
			return n, io.EOF
		}
		return n, nil
	}

	cfg := AdaptivePrefetchConfig{
		ChunkSize: chunkSize,
		Depth:     2,
		Guardrail: guardrail,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		reader := NewAdaptivePrefetchReader(ctx, totalSize, fetcher, cfg)

		buf := make([]byte, chunkSize)
		for {
			n, err := io.ReadFull(reader, buf)
			if n > 0 {
				// Simulate application compute
				time.Sleep(simulatedComputeDelay)
			}
			if err != nil {
				break
			}
		}
		reader.Close()
	}
}

// BenchmarkUploadSimulation_StaticVsAdaptive compares upload latency for a 256 MiB object
// on a 10 Gbps network with 10ms RTT per RPC roundtrip.
func BenchmarkUploadSimulation_Static16MB(b *testing.B) {
	totalSize := int64(256 * 1024 * 1024) // 256 MiB
	chunkSize := int64(16 * 1024 * 1024)  // Fixed 16 MiB
	rpcRTT := 10 * time.Millisecond

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		uploaded := int64(0)
		for uploaded < totalSize {
			// Each chunk incurs RTT overhead
			time.Sleep(rpcRTT)
			uploaded += chunkSize
		}
	}
}

func BenchmarkUploadSimulation_AdaptiveAIMD(b *testing.B) {
	totalSize := int64(256 * 1024 * 1024) // 256 MiB
	rpcRTT := 10 * time.Millisecond

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sizer := NewDynamicChunkSizer(DefaultDynamicChunkConfig(), nil)
		chunkSize := int64(sizer.RecommendInitialChunkSize(totalSize)) // Starts at 64 MiB
		uploaded := int64(0)

		for uploaded < totalSize {
			time.Sleep(rpcRTT)
			uploaded += chunkSize
			chunkSize = int64(sizer.RecordChunkTransfer(chunkSize, rpcRTT, false))
		}
	}
}
