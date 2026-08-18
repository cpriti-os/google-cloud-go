package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
)

type WorkloadMetric struct {
	Timestamp      string  `json:"timestamp"`
	Workload       string  `json:"workload"`
	Mode           string  `json:"mode"`
	ObjectSizeMB   int     `json:"object_size_mb"`
	Concurrency    int     `json:"concurrency"`
	TotalMB        int     `json:"total_mb"`
	DurationSec    float64 `json:"duration_sec"`
	ThroughputMBs  float64 `json:"throughput_mb_ps"`
	LatencyAvgSec  float64 `json:"latency_avg_sec"`
	LatencyP50Sec  float64 `json:"latency_p50_sec"`
	LatencyP90Sec  float64 `json:"latency_p90_sec"`
	LatencyP99Sec  float64 `json:"latency_p99_sec"`
	UserCPUMs      float64 `json:"user_cpu_ms"`
	SysCPUMs       float64 `json:"sys_cpu_ms"`
	TotalCPUMs     float64 `json:"total_cpu_ms"`
	CPUUtilPercent float64 `json:"cpu_util_percent"`
	AllocHeapMB    float64 `json:"alloc_heap_mb"`
	TotalAllocMB   float64 `json:"total_alloc_mb"`
	SysMemMB       float64 `json:"sys_mem_mb"`
	GCCycles       uint32  `json:"gc_cycles"`
	GCPauseMs      float64 `json:"gc_pause_ms"`
}

func getCPUTimeMs() (float64, float64) {
	var r syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	userMs := float64(r.Utime.Sec)*1000.0 + float64(r.Utime.Usec)/1000.0
	sysMs := float64(r.Stime.Sec)*1000.0 + float64(r.Stime.Usec)/1000.0
	return userMs, sysMs
}

func calcPercentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func generatePayload(sizeMB int) []byte {
	buf := make([]byte, sizeMB*1024*1024)
	rand.Read(buf[:min(len(buf), 4096)])
	return buf
}

func runRealGCSUpload(ctx context.Context, client *storage.Client, bucket string, sizeMB, concurrency int, autoTune bool) WorkloadMetric {
	mode := "Static-Default16MB"
	var cfg *storage.AutoTuningConfig
	if autoTune {
		mode = "Smart-AutoTuning32MB"
		cfg = storage.DefaultAutoTuningConfig()
	}

	payload := generatePayload(sizeMB)

	runtime.GC()
	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)
	uStart, sStart := getCPUTimeMs()

	wallStart := time.Now()
	var wg sync.WaitGroup
	latencies := make([]float64, concurrency)
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tStart := time.Now()
			objName := fmt.Sprintf("real_bench_upload_%s_%dmb_w%d.dat", mode, sizeMB, id)
			w := client.Bucket(bucket).Object(objName).NewWriter(ctx)
			if autoTune {
				w.AutoTuning = cfg
			}
			if _, err := io.Copy(w, bytes.NewReader(payload)); err != nil {
				w.Close()
				errCh <- err
				return
			}
			if err := w.Close(); err != nil {
				errCh <- err
				return
			}
			latencies[id] = time.Since(tStart).Seconds()
		}(i)
	}

	wg.Wait()
	close(errCh)
	duration := time.Since(wallStart).Seconds()

	uEnd, sEnd := getCPUTimeMs()
	var mEnd runtime.MemStats
	runtime.ReadMemStats(&mEnd)

	for err := range errCh {
		log.Printf("Upload error: %v", err)
	}

	userCPU := uEnd - uStart
	sysCPU := sEnd - sStart
	totalCPU := userCPU + sysCPU
	cpuUtil := (totalCPU / (duration * 1000.0)) * 100.0 / float64(runtime.NumCPU())

	totalMB := sizeMB * concurrency
	throughput := float64(totalMB) / duration

	var sumLat float64
	for _, l := range latencies {
		sumLat += l
	}
	avgLat := sumLat / float64(len(latencies))

	return WorkloadMetric{
		Timestamp:      time.Now().Format(time.RFC3339),
		Workload:       "Orbax-AI-Checkpoint-Upload",
		Mode:           mode,
		ObjectSizeMB:   sizeMB,
		Concurrency:    concurrency,
		TotalMB:        totalMB,
		DurationSec:    duration,
		ThroughputMBs:  throughput,
		LatencyAvgSec:  avgLat,
		LatencyP50Sec:  calcPercentile(latencies, 0.50),
		LatencyP90Sec:  calcPercentile(latencies, 0.90),
		LatencyP99Sec:  calcPercentile(latencies, 0.99),
		UserCPUMs:      userCPU,
		SysCPUMs:       sysCPU,
		TotalCPUMs:     totalCPU,
		CPUUtilPercent: cpuUtil,
		AllocHeapMB:    float64(mEnd.Alloc) / (1024 * 1024),
		TotalAllocMB:   float64(mEnd.TotalAlloc-mStart.TotalAlloc) / (1024 * 1024),
		SysMemMB:       float64(mEnd.Sys) / (1024 * 1024),
		GCCycles:       mEnd.NumGC - mStart.NumGC,
		GCPauseMs:      float64(mEnd.PauseTotalNs-mStart.PauseTotalNs) / 1e6,
	}
}

func runRealGCSDownload(ctx context.Context, client *storage.Client, bucket string, sizeMB, concurrency int, autoTune bool) WorkloadMetric {
	mode := "Static-DefaultReader"
	var cfg *storage.AutoTuningConfig
	if autoTune {
		mode = "Smart-AdaptivePrefetch"
		cfg = storage.DefaultAutoTuningConfig()
	}

	runtime.GC()
	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)
	uStart, sStart := getCPUTimeMs()

	wallStart := time.Now()
	var wg sync.WaitGroup
	latencies := make([]float64, concurrency)
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tStart := time.Now()
			objName := fmt.Sprintf("real_bench_upload_Static-Default16MB_%dmb_w%d.dat", sizeMB, id)
			obj := client.Bucket(bucket).Object(objName)
			var rc io.ReadCloser
			var err error
			if autoTune {
				rc, err = obj.NewAdaptiveReader(ctx, cfg)
			} else {
				rc, err = obj.NewReader(ctx)
			}
			if err != nil {
				errCh <- err
				return
			}
			defer rc.Close()

			buf := make([]byte, 1024*1024)
			if _, err := io.CopyBuffer(io.Discard, rc, buf); err != nil {
				errCh <- err
				return
			}
			latencies[id] = time.Since(tStart).Seconds()
		}(i)
	}

	wg.Wait()
	close(errCh)
	duration := time.Since(wallStart).Seconds()

	uEnd, sEnd := getCPUTimeMs()
	var mEnd runtime.MemStats
	runtime.ReadMemStats(&mEnd)

	for err := range errCh {
		log.Printf("Download error: %v", err)
	}

	userCPU := uEnd - uStart
	sysCPU := sEnd - sStart
	totalCPU := userCPU + sysCPU
	cpuUtil := (totalCPU / (duration * 1000.0)) * 100.0 / float64(runtime.NumCPU())

	totalMB := sizeMB * concurrency
	throughput := float64(totalMB) / duration

	var sumLat float64
	for _, l := range latencies {
		sumLat += l
	}
	avgLat := sumLat / float64(len(latencies))

	return WorkloadMetric{
		Timestamp:      time.Now().Format(time.RFC3339),
		Workload:       "AI-DataLoader-Stream-Download",
		Mode:           mode,
		ObjectSizeMB:   sizeMB,
		Concurrency:    concurrency,
		TotalMB:        totalMB,
		DurationSec:    duration,
		ThroughputMBs:  throughput,
		LatencyAvgSec:  avgLat,
		LatencyP50Sec:  calcPercentile(latencies, 0.50),
		LatencyP90Sec:  calcPercentile(latencies, 0.90),
		LatencyP99Sec:  calcPercentile(latencies, 0.99),
		UserCPUMs:      userCPU,
		SysCPUMs:       sysCPU,
		TotalCPUMs:     totalCPU,
		CPUUtilPercent: cpuUtil,
		AllocHeapMB:    float64(mEnd.Alloc) / (1024 * 1024),
		TotalAllocMB:   float64(mEnd.TotalAlloc-mStart.TotalAlloc) / (1024 * 1024),
		SysMemMB:       float64(mEnd.Sys) / (1024 * 1024),
		GCCycles:       mEnd.NumGC - mStart.NumGC,
		GCPauseMs:      float64(mEnd.PauseTotalNs-mStart.PauseTotalNs) / 1e6,
	}
}

func main() {
	bucket := flag.String("bucket", "cpriti-sdk-autotune-bench", "GCS bucket name")
	csvOut := flag.String("out_csv", "benchmark_data/real_gcs_live_metrics.csv", "Output CSV file path")
	flag.Parse()

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create GCS client: %v", err)
	}
	defer client.Close()

	file, err := os.Create(*csvOut)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{
		"Timestamp", "Workload", "Mode", "ObjectSizeMB", "Concurrency", "TotalMB",
		"DurationSec", "ThroughputMBs", "LatencyAvgSec", "LatencyP50Sec", "LatencyP90Sec", "LatencyP99Sec",
		"UserCPUMs", "SysCPUMs", "TotalCPUMs", "CPUUtilPercent",
		"AllocHeapMB", "TotalAllocMB", "SysMemMB", "GCCycles", "GCPauseMs",
	}
	writer.Write(headers)

	fmt.Println("================================================================================")
	fmt.Printf("EXECUTING LIVE GCS BENCHMARK SUITE AGAINST PROD: gs://%s\n", *bucket)
	fmt.Println("================================================================================")

	configs := []struct {
		sizeMB      int
		concurrency int
	}{
		{64, 4},
		{128, 4},
		{256, 4},
	}

	recordMetric := func(m WorkloadMetric) {
		row := []string{
			m.Timestamp, m.Workload, m.Mode, fmt.Sprintf("%d", m.ObjectSizeMB), fmt.Sprintf("%d", m.Concurrency), fmt.Sprintf("%d", m.TotalMB),
			fmt.Sprintf("%.4f", m.DurationSec), fmt.Sprintf("%.2f", m.ThroughputMBs), fmt.Sprintf("%.4f", m.LatencyAvgSec), fmt.Sprintf("%.4f", m.LatencyP50Sec), fmt.Sprintf("%.4f", m.LatencyP90Sec), fmt.Sprintf("%.4f", m.LatencyP99Sec),
			fmt.Sprintf("%.2f", m.UserCPUMs), fmt.Sprintf("%.2f", m.SysCPUMs), fmt.Sprintf("%.2f", m.TotalCPUMs), fmt.Sprintf("%.2f", m.CPUUtilPercent),
			fmt.Sprintf("%.2f", m.AllocHeapMB), fmt.Sprintf("%.2f", m.TotalAllocMB), fmt.Sprintf("%.2f", m.SysMemMB), fmt.Sprintf("%d", m.GCCycles), fmt.Sprintf("%.3f", m.GCPauseMs),
		}
		writer.Write(row)
		writer.Flush()
	}

	fmt.Println("\n--- WORKLOAD 1: LIVE ORBAX CHECKPOINT UPLOADS (GCS PROD) ---")
	for _, cfg := range configs {
		fmt.Printf("\n>>> Running Upload: %d MB x %d workers (%d MB Total)\n", cfg.sizeMB, cfg.concurrency, cfg.sizeMB*cfg.concurrency)
		mStatic := runRealGCSUpload(ctx, client, *bucket, cfg.sizeMB, cfg.concurrency, false)
		recordMetric(mStatic)
		fmt.Printf("  [Static 16MB Default] Duration: %6.2fs | Throughput: %7.1f MB/s | P99: %5.2fs | Total CPU: %6.1fms | HeapAlloc: %6.1fMB | GC: %d (Pause: %.1fms)\n",
			mStatic.DurationSec, mStatic.ThroughputMBs, mStatic.LatencyP99Sec, mStatic.TotalCPUMs, mStatic.TotalAllocMB, mStatic.GCCycles, mStatic.GCPauseMs)

		mSmart := runRealGCSUpload(ctx, client, *bucket, cfg.sizeMB, cfg.concurrency, true)
		recordMetric(mSmart)
		fmt.Printf("  [Smart Auto-Tuning  ] Duration: %6.2fs | Throughput: %7.1f MB/s | P99: %5.2fs | Total CPU: %6.1fms | HeapAlloc: %6.1fMB | GC: %d (Pause: %.1fms)\n",
			mSmart.DurationSec, mSmart.ThroughputMBs, mSmart.LatencyP99Sec, mSmart.TotalCPUMs, mSmart.TotalAllocMB, mSmart.GCCycles, mSmart.GCPauseMs)

		fmt.Printf("  ===> Real Speedup: %.2fx | Throughput: +%.1f%% | CPU Delta: %+.1fms | Heap Delta: %+.1fMB\n",
			mStatic.DurationSec/mSmart.DurationSec,
			((mSmart.ThroughputMBs-mStatic.ThroughputMBs)/mStatic.ThroughputMBs)*100.0,
			mSmart.TotalCPUMs-mStatic.TotalCPUMs,
			mSmart.TotalAllocMB-mStatic.TotalAllocMB)
	}

	fmt.Println("\n--- WORKLOAD 2: LIVE AI DATALOADER STREAMING DOWNLOADS (GCS PROD) ---")
	for _, cfg := range configs {
		fmt.Printf("\n>>> Running Download: %d MB x %d workers (%d MB Total)\n", cfg.sizeMB, cfg.concurrency, cfg.sizeMB*cfg.concurrency)
		mStatic := runRealGCSDownload(ctx, client, *bucket, cfg.sizeMB, cfg.concurrency, false)
		recordMetric(mStatic)
		fmt.Printf("  [Static Default Reader] Duration: %6.2fs | Throughput: %7.1f MB/s | P99: %5.2fs | Total CPU: %6.1fms | HeapAlloc: %6.1fMB | GC: %d (Pause: %.1fms)\n",
			mStatic.DurationSec, mStatic.ThroughputMBs, mStatic.LatencyP99Sec, mStatic.TotalCPUMs, mStatic.TotalAllocMB, mStatic.GCCycles, mStatic.GCPauseMs)

		mSmart := runRealGCSDownload(ctx, client, *bucket, cfg.sizeMB, cfg.concurrency, true)
		recordMetric(mSmart)
		fmt.Printf("  [Smart Adaptive Reader] Duration: %6.2fs | Throughput: %7.1f MB/s | P99: %5.2fs | Total CPU: %6.1fms | HeapAlloc: %6.1fMB | GC: %d (Pause: %.1fms)\n",
			mSmart.DurationSec, mSmart.ThroughputMBs, mSmart.LatencyP99Sec, mSmart.TotalCPUMs, mSmart.TotalAllocMB, mSmart.GCCycles, mSmart.GCPauseMs)

		fmt.Printf("  ===> Real Speedup: %.2fx | Throughput: +%.1f%% | CPU Delta: %+.1fms | Heap Delta: %+.1fMB\n",
			mStatic.DurationSec/mSmart.DurationSec,
			((mSmart.ThroughputMBs-mStatic.ThroughputMBs)/mStatic.ThroughputMBs)*100.0,
			mSmart.TotalCPUMs-mStatic.TotalCPUMs,
			mSmart.TotalAllocMB-mStatic.TotalAllocMB)
	}

	fmt.Println("\n================================================================================")
	fmt.Printf("ALL LIVE GCS PROD BENCHMARKS COMPLETE! Results written to: %s\n", *csvOut)
	fmt.Println("================================================================================")
}
