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

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
)

type BenchmarkRecord struct {
	Timestamp      string  `json:"timestamp"`
	Workload       string  `json:"workload"`
	Mode           string  `json:"mode"`
	ObjectSizeMB   float64 `json:"object_size_mb"`
	Concurrency    int     `json:"concurrency"`
	DurationSec    float64 `json:"duration_sec"`
	ThroughputMBps float64 `json:"throughput_mb_ps"`
	P50Sec         float64 `json:"p50_sec"`
	P90Sec         float64 `json:"p90_sec"`
	P99Sec         float64 `json:"p99_sec"`
	PMaxSec        float64 `json:"p_max_sec"`
	BarrierTimeSec float64 `json:"barrier_time_sec"`
	ComputeWaitPct float64 `json:"compute_wait_pct"`
	PeakRAMMB      float64 `json:"peak_ram_mb"`
	TotalAllocMB   float64 `json:"total_alloc_mb"`
	NumGCs         uint32  `json:"num_gcs"`
}

var (
	outDir       = flag.String("out_dir", "/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data", "Directory to output benchmark results")
	durationFlag = flag.Duration("duration", 60*time.Second, "Duration for benchmark run")
	verbose      = flag.Bool("verbose", true, "Verbose logging")
)

func main() {
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Printf("Error creating out_dir: %v\n", err)
		os.Exit(1)
	}

	csvPath := filepath.Join(*outDir, "benchmark_detailed_runs.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		fmt.Printf("Error creating CSV: %v\n", err)
		os.Exit(1)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV Header
	_ = writer.Write([]string{
		"timestamp", "workload", "mode", "object_size_mb", "concurrency",
		"duration_sec", "throughput_mb_ps", "p50_sec", "p90_sec", "p99_sec", "p_max_sec",
		"barrier_time_sec", "compute_wait_pct", "peak_ram_mb", "total_alloc_mb", "num_gcs",
	})

	var allRecords []BenchmarkRecord

	startTime := time.Now()
	fmt.Printf("================================================================================\n")
	fmt.Printf("Starting Exhaustive GCS SDK Auto-Tuning Benchmark Suite\n")
	fmt.Printf("Duration Target: %v | Start Time: %v\n", *durationFlag, startTime.Format(time.RFC3339))
	fmt.Printf("================================================================================\n\n")

	iteration := 0
	for time.Since(startTime) < *durationFlag {
		iteration++
		fmt.Printf("\n--- Iteration %d (Elapsed: %v / Target: %v) ---\n", iteration, time.Since(startTime).Truncate(time.Second), *durationFlag)

		// 1. Orbax AI Checkpoint Workloads (Writes)
		for _, sizeMB := range []int{64, 256, 512, 1024} {
			for _, nodes := range []int{8, 16, 32} {
				// A. Static Baseline (Fixed 16MB chunk, no stall adaptation)
				recStatic := runOrbaxCheckpoint(sizeMB, nodes, false)
				allRecords = append(allRecords, recStatic)
				writeCSVRecord(writer, recStatic)

				// B. Smart Tuned (Dynamic Chunk Sizing + AIMD + Stall Adaptation)
				recSmart := runOrbaxCheckpoint(sizeMB, nodes, true)
				allRecords = append(allRecords, recSmart)
				writeCSVRecord(writer, recSmart)

				if *verbose {
					fmt.Printf("[Orbax Checkpoint %4d MB x %2d nodes] Static Barrier: %6.2fs (P99: %6.2fs) | Smart Barrier: %6.2fs (P99: %6.2fs) -> Speedup: %.2fx\n",
						sizeMB, nodes, recStatic.BarrierTimeSec, recStatic.P99Sec, recSmart.BarrierTimeSec, recSmart.P99Sec, recStatic.BarrierTimeSec/recSmart.BarrierTimeSec)
				}
			}
		}

		// 2. AI/ML Dataset Streaming / DataLoader (Reads + Compute Interleaving)
		for _, batchMB := range []int{4, 16, 64} {
			for _, computeDelay := range []time.Duration{5 * time.Millisecond, 15 * time.Millisecond, 30 * time.Millisecond} {
				// A. Static Synchronous Read (No prefetching)
				recStatic := runDataLoader(batchMB, 16, computeDelay, "Static-NoPrefetch", 0)
				allRecords = append(allRecords, recStatic)
				writeCSVRecord(writer, recStatic)

				// B. Smart Adaptive Prefetch (Depth 1)
				recSmart1 := runDataLoader(batchMB, 16, computeDelay, "Smart-Prefetch-Depth1", 1)
				allRecords = append(allRecords, recSmart1)
				writeCSVRecord(writer, recSmart1)

				// C. Smart Adaptive Prefetch (Depth 2)
				recSmart2 := runDataLoader(batchMB, 16, computeDelay, "Smart-Prefetch-Depth2", 2)
				allRecords = append(allRecords, recSmart2)
				writeCSVRecord(writer, recSmart2)

				if *verbose {
					fmt.Printf("[DataLoader %2d MB Batch | %v Compute] Static Wait: %5.1f%% (%5.1f MB/s) | Smart-D2 Wait: %5.1f%% (%5.1f MB/s) -> Throughput: %.2fx\n",
						batchMB, computeDelay, recStatic.ComputeWaitPct, recStatic.ThroughputMBps, recSmart2.ComputeWaitPct, recSmart2.ThroughputMBps, recSmart2.ThroughputMBps/recStatic.ThroughputMBps)
				}
			}
		}

		// 3. Columnar Analytics / Parquet Slicing (Range Scans)
		for _, totalObjMB := range []int{128, 512} {
			recStatic := runColumnarScan(totalObjMB, false)
			allRecords = append(allRecords, recStatic)
			writeCSVRecord(writer, recStatic)

			recSmart := runColumnarScan(totalObjMB, true)
			allRecords = append(allRecords, recSmart)
			writeCSVRecord(writer, recSmart)

			if *verbose {
				fmt.Printf("[Columnar Scan %4d MB] Static: %6.2fs (%5.1f MB/s) | Smart: %6.2fs (%5.1f MB/s) -> Speedup: %.2fx\n",
					totalObjMB, recStatic.DurationSec, recStatic.ThroughputMBps, recSmart.DurationSec, recSmart.ThroughputMBps, recStatic.DurationSec/recSmart.DurationSec)
			}
		}

		// 4. Memory Guardrail Soak & GC Stability
		recUnbounded := runMemorySoak(100, false)
		allRecords = append(allRecords, recUnbounded)
		writeCSVRecord(writer, recUnbounded)

		recGuardrail := runMemorySoak(100, true)
		allRecords = append(allRecords, recGuardrail)
		writeCSVRecord(writer, recGuardrail)

		if *verbose {
			fmt.Printf("[Memory Guardrail Soak 100 Workers] Unbounded Peak RAM: %6.1f MB (GCs: %3d) | Guardrail Peak RAM: %6.1f MB (GCs: %3d) -> RAM Savings: %.1f%%\n",
				recUnbounded.PeakRAMMB, recUnbounded.NumGCs, recGuardrail.PeakRAMMB, recGuardrail.NumGCs, (recUnbounded.PeakRAMMB-recGuardrail.PeakRAMMB)/recUnbounded.PeakRAMMB*100.0)
		}
	}

	// Save summary JSON
	jsonPath := filepath.Join(*outDir, "benchmark_summary.json")
	jsonData, _ := json.MarshalIndent(allRecords, "", "  ")
	_ = os.WriteFile(jsonPath, jsonData, 0644)

	fmt.Printf("\n================================================================================\n")
	fmt.Printf("Benchmark Suite Completed Successfully!\n")
	fmt.Printf("Total Benchmark Records: %d\n", len(allRecords))
	fmt.Printf("CSV Output: %s\n", csvPath)
	fmt.Printf("JSON Output: %s\n", jsonPath)
	fmt.Printf("================================================================================\n")
}

func writeCSVRecord(w *csv.Writer, r BenchmarkRecord) {
	_ = w.Write([]string{
		r.Timestamp,
		r.Workload,
		r.Mode,
		fmt.Sprintf("%.2f", r.ObjectSizeMB),
		fmt.Sprintf("%d", r.Concurrency),
		fmt.Sprintf("%.4f", r.DurationSec),
		fmt.Sprintf("%.2f", r.ThroughputMBps),
		fmt.Sprintf("%.4f", r.P50Sec),
		fmt.Sprintf("%.4f", r.P90Sec),
		fmt.Sprintf("%.4f", r.P99Sec),
		fmt.Sprintf("%.4f", r.PMaxSec),
		fmt.Sprintf("%.4f", r.BarrierTimeSec),
		fmt.Sprintf("%.2f", r.ComputeWaitPct),
		fmt.Sprintf("%.2f", r.PeakRAMMB),
		fmt.Sprintf("%.2f", r.TotalAllocMB),
		fmt.Sprintf("%d", r.NumGCs),
	})
	w.Flush()
}

// -----------------------------------------------------------------------------
// Workload 1: Orbax AI Distributed Checkpointing
// -----------------------------------------------------------------------------
func runOrbaxCheckpoint(sizeMB int, numNodes int, smartTuned bool) BenchmarkRecord {
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	totalBytes := int64(sizeMB) * 1024 * 1024
	nodeDurations := make([]float64, numNodes)

	var wg sync.WaitGroup
	wg.Add(numNodes)

	barrierStart := time.Now()

	for nodeIdx := 0; nodeIdx < numNodes; nodeIdx++ {
		go func(idx int) {
			defer wg.Done()
			start := time.Now()

			var sizer *storage.DynamicChunkSizer
			chunkSize := int64(16 * 1024 * 1024) // Default static 16 MiB

			if smartTuned {
				sizer = storage.NewDynamicChunkSizer(storage.DefaultDynamicChunkConfig(), nil)
				chunkSize = int64(sizer.RecommendInitialChunkSize(totalBytes))
			}

			uploaded := int64(0)
			for uploaded < totalBytes {
				currentChunk := chunkSize
				if uploaded+currentChunk > totalBytes {
					currentChunk = totalBytes - uploaded
				}

				// Simulate network transfer: base rate ~1.2 GB/s per node with 5ms RPC RTT
				transferTime := time.Duration(float64(currentChunk)/(1.2*1024*1024*1024)*1000) * time.Millisecond
				if transferTime < 5*time.Millisecond {
					transferTime = 5 * time.Millisecond
				}

				// Introduce network jitter: 2% of chunks experience a transient stall / straggler (300ms - 800ms)
				isStall := rand.Float32() < 0.02
				if isStall {
					transferTime += time.Duration(300+rand.Intn(500)) * time.Millisecond
				}

				time.Sleep(transferTime)
				uploaded += currentChunk

				if smartTuned && sizer != nil {
					chunkSize = int64(sizer.RecordChunkTransfer(currentChunk, transferTime, isStall))
				}
			}

			nodeDurations[idx] = time.Since(start).Seconds()
		}(nodeIdx)
	}

	wg.Wait()
	barrierDuration := time.Since(barrierStart).Seconds()

	runtime.ReadMemStats(&mAfter)

	sort.Float64s(nodeDurations)
	p50 := nodeDurations[len(nodeDurations)*50/100]
	p90 := nodeDurations[len(nodeDurations)*90/100]
	p99 := nodeDurations[len(nodeDurations)*99/100]
	pMax := nodeDurations[len(nodeDurations)-1]

	totalTransferredMB := float64(sizeMB * numNodes)
	throughput := totalTransferredMB / barrierDuration

	mode := "Static-Fixed16MB"
	if smartTuned {
		mode = "Smart-DynamicChunk"
	}

	return BenchmarkRecord{
		Timestamp:      time.Now().Format(time.RFC3339),
		Workload:       "Orbax-Checkpoint-Write",
		Mode:           mode,
		ObjectSizeMB:   float64(sizeMB),
		Concurrency:    numNodes,
		DurationSec:    p50,
		ThroughputMBps: throughput,
		P50Sec:         p50,
		P90Sec:         p90,
		P99Sec:         p99,
		PMaxSec:        pMax,
		BarrierTimeSec: barrierDuration,
		ComputeWaitPct: 0.0,
		PeakRAMMB:      float64(mAfter.Sys) / (1024 * 1024),
		TotalAllocMB:   float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024),
		NumGCs:         mAfter.NumGC - mBefore.NumGC,
	}
}

// -----------------------------------------------------------------------------
// Workload 2: AI/ML Dataloader / Dataset Streaming
// -----------------------------------------------------------------------------
func runDataLoader(batchMB int, totalBatches int, computeDelay time.Duration, mode string, prefetchDepth int) BenchmarkRecord {
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	batchSize := int64(batchMB) * 1024 * 1024
	totalSize := batchSize * int64(totalBatches)
	networkTransferPerBatch := time.Duration(float64(batchSize)/(1.5*1024*1024*1024)*1000) * time.Millisecond
	if networkTransferPerBatch < 3*time.Millisecond {
		networkTransferPerBatch = 3 * time.Millisecond
	}

	sourceData := make([]byte, totalSize)
	var totalComputeWaitTime time.Duration

	start := time.Now()

	if prefetchDepth == 0 {
		// Synchronous sequential reading: fetch batch -> compute -> fetch batch -> compute
		offset := int64(0)
		for b := 0; b < totalBatches; b++ {
			// Network wait
			time.Sleep(networkTransferPerBatch)
			_ = sourceData[offset : offset+batchSize]
			offset += batchSize

			// Application compute
			time.Sleep(computeDelay)
		}
		totalComputeWaitTime = networkTransferPerBatch * time.Duration(totalBatches)
	} else {
		// Adaptive Prefetch Reader
		guardrail := storage.NewMemoryGuardrail(128 * 1024 * 1024)
		fetcher := func(ctx context.Context, offset, length int64, dest []byte) (int, error) {
			time.Sleep(networkTransferPerBatch)
			if offset >= totalSize {
				return 0, io.EOF
			}
			end := offset + length
			if end > totalSize {
				end = totalSize
			}
			n := copy(dest, sourceData[offset:end])
			if offset+int64(n) >= totalSize {
				return n, io.EOF
			}
			return n, nil
		}

		cfg := storage.AdaptivePrefetchConfig{
			ChunkSize: int(batchSize),
			Depth:     prefetchDepth,
			Guardrail: guardrail,
		}

		reader := storage.NewAdaptivePrefetchReader(context.Background(), totalSize, fetcher, cfg)
		buf := make([]byte, batchSize)

		for b := 0; b < totalBatches; b++ {
			t0 := time.Now()
			_, err := io.ReadFull(reader, buf)
			waitTime := time.Since(t0)
			totalComputeWaitTime += waitTime

			// Application compute
			time.Sleep(computeDelay)
			if err != nil {
				break
			}
		}
		reader.Close()
	}

	totalDuration := time.Since(start).Seconds()
	runtime.ReadMemStats(&mAfter)

	totalMB := float64(batchMB * totalBatches)
	throughput := totalMB / totalDuration
	waitPct := (totalComputeWaitTime.Seconds() / totalDuration) * 100.0

	return BenchmarkRecord{
		Timestamp:      time.Now().Format(time.RFC3339),
		Workload:       "DataLoader-Streaming",
		Mode:           mode,
		ObjectSizeMB:   float64(batchMB),
		Concurrency:    1,
		DurationSec:    totalDuration,
		ThroughputMBps: throughput,
		P50Sec:         totalDuration / float64(totalBatches),
		P90Sec:         totalDuration / float64(totalBatches),
		P99Sec:         totalDuration / float64(totalBatches),
		PMaxSec:        totalDuration,
		BarrierTimeSec: totalDuration,
		ComputeWaitPct: waitPct,
		PeakRAMMB:      float64(mAfter.Sys) / (1024 * 1024),
		TotalAllocMB:   float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024),
		NumGCs:         mAfter.NumGC - mBefore.NumGC,
	}
}

// -----------------------------------------------------------------------------
// Workload 3: Columnar Analytics / Parquet Range Scans
// -----------------------------------------------------------------------------
func runColumnarScan(objSizeMB int, smartTuned bool) BenchmarkRecord {
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	totalBytes := int64(objSizeMB) * 1024 * 1024
	numRowGroups := 10
	rangesPerRowGroup := 4
	rangeSize := int64(1 * 1024 * 1024) // 1 MiB column projection chunk

	sourceData := make([]byte, totalBytes)

	start := time.Now()

	if !smartTuned {
		// Standard range fetch on demand
		for rg := 0; rg < numRowGroups; rg++ {
			for col := 0; col < rangesPerRowGroup; col++ {
				// Incur network RTT + transfer time
				time.Sleep(8 * time.Millisecond)
				offset := int64(rg)*(totalBytes/int64(numRowGroups)) + int64(col)*rangeSize*2
				if offset+rangeSize <= totalBytes {
					_ = sourceData[offset : offset+rangeSize]
				}
			}
		}
	} else {
		// Adaptive prefetching with stride detection
		guardrail := storage.NewMemoryGuardrail(64 * 1024 * 1024)
		fetcher := func(ctx context.Context, offset, length int64, dest []byte) (int, error) {
			time.Sleep(8 * time.Millisecond)
			if offset >= totalBytes {
				return 0, io.EOF
			}
			end := offset + length
			if end > totalBytes {
				end = totalBytes
			}
			n := copy(dest, sourceData[offset:end])
			return n, nil
		}

		reader := storage.NewAdaptivePrefetchReader(context.Background(), totalBytes, fetcher, storage.AdaptivePrefetchConfig{
			ChunkSize: int(rangeSize),
			Depth:     2,
			Guardrail: guardrail,
		})

		buf := make([]byte, rangeSize)
		for rg := 0; rg < numRowGroups; rg++ {
			for col := 0; col < rangesPerRowGroup; col++ {
				offset := int64(rg)*(totalBytes/int64(numRowGroups)) + int64(col)*rangeSize*2
				_, _ = reader.Seek(offset, io.SeekStart)
				_, _ = io.ReadFull(reader, buf)
			}
		}
		reader.Close()
	}

	duration := time.Since(start).Seconds()
	runtime.ReadMemStats(&mAfter)

	totalReadMB := float64(numRowGroups * rangesPerRowGroup * int(rangeSize) / (1024 * 1024))
	throughput := totalReadMB / duration

	mode := "Static-OnDemand"
	if smartTuned {
		mode = "Smart-RangePrefetch"
	}

	return BenchmarkRecord{
		Timestamp:      time.Now().Format(time.RFC3339),
		Workload:       "Columnar-Parquet-Scan",
		Mode:           mode,
		ObjectSizeMB:   float64(objSizeMB),
		Concurrency:    1,
		DurationSec:    duration,
		ThroughputMBps: throughput,
		P50Sec:         duration,
		P90Sec:         duration,
		P99Sec:         duration,
		PMaxSec:        duration,
		BarrierTimeSec: duration,
		ComputeWaitPct: 0,
		PeakRAMMB:      float64(mAfter.Sys) / (1024 * 1024),
		TotalAllocMB:   float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024),
		NumGCs:         mAfter.NumGC - mBefore.NumGC,
	}
}

// -----------------------------------------------------------------------------
// Workload 4: High Concurrency Memory Guardrail Soak
// -----------------------------------------------------------------------------
func runMemorySoak(numWorkers int, useGuardrail bool) BenchmarkRecord {
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	chunkSize := 4 * 1024 * 1024 // 4 MiB
	iterationsPerWorker := 20

	var guardrail *storage.MemoryGuardrail
	if useGuardrail {
		guardrail = storage.NewMemoryGuardrail(64 * 1024 * 1024) // Hard 64 MiB limit
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	start := time.Now()
	var totalTransferredBytes atomic.Int64

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for it := 0; it < iterationsPerWorker; it++ {
				if useGuardrail {
					buf := guardrail.GetBuffer(chunkSize)
					if buf != nil {
						// Simulate transfer
						time.Sleep(1 * time.Millisecond)
						totalTransferredBytes.Add(int64(chunkSize))
						guardrail.PutBuffer(buf)
					} else {
						// Guardrail active backpressure fallback
						time.Sleep(2 * time.Millisecond)
					}
				} else {
					// Unbounded allocation (creates GC heap thrash)
					buf := make([]byte, chunkSize)
					time.Sleep(1 * time.Millisecond)
					totalTransferredBytes.Add(int64(chunkSize))
					_ = buf[0]
				}
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start).Seconds()
	runtime.ReadMemStats(&mAfter)

	totalMB := float64(totalTransferredBytes.Load()) / (1024 * 1024)
	throughput := totalMB / duration

	mode := "Unbounded-Alloc"
	if useGuardrail {
		mode = "MemoryGuardrail-SlabPool"
	}

	return BenchmarkRecord{
		Timestamp:      time.Now().Format(time.RFC3339),
		Workload:       "Memory-Guardrail-Soak",
		Mode:           mode,
		ObjectSizeMB:   4.0,
		Concurrency:    numWorkers,
		DurationSec:    duration,
		ThroughputMBps: throughput,
		P50Sec:         duration,
		P90Sec:         duration,
		P99Sec:         duration,
		PMaxSec:        duration,
		BarrierTimeSec: duration,
		ComputeWaitPct: 0,
		PeakRAMMB:      float64(mAfter.Sys) / (1024 * 1024),
		TotalAllocMB:   float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024),
		NumGCs:         mAfter.NumGC - mBefore.NumGC,
	}
}
