package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
)

func getCPUTicks() float64 {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil { return 0 }
	fields := strings.Fields(string(b))
	if len(fields) < 15 { return 0 }
	u, _ := strconv.ParseFloat(fields[13], 64)
	s, _ := strconv.ParseFloat(fields[14], 64)
	return u + s
}

func main() {
	outPath := "/usr/local/google/home/cpriti/.gemini/jetski/brain/9d3761a8-b7d0-4249-bcb9-c4ed239286b8/scratch/live_metrics.csv"
	csvFile, _ := os.Create(outPath)
	
	csvFile.WriteString("TimeSec,TargetAppMBps,SDKIngestMBps,EwmaVelocity,Workers,ChunkSizeMB,HeapMB,CpuLoad\n")

	payload := make([]byte, 16*1024*1024)
	crand.Read(payload[:1000])

	ctx := context.Background()
	client, _ := storage.NewGRPCClient(ctx)
	defer client.Close()
	bucket := client.Bucket("cpriti-sdk-autotune-bench")

	cfg := storage.DefaultAutoTuningConfig()
	cfg.MaxMemoryBudget = 512 * 1024 * 1024
	w := bucket.Object(fmt.Sprintf("metrics_run_%d.bin", time.Now().Unix())).NewAdaptivePCUWriter(ctx, cfg)

	var runningBytes atomic.Int64
	var currentTargetMBps atomic.Int64
	currentTargetMBps.Store(10) 

	// Background Poller Thread
	go func() {
		start := time.Now()
		var lastBytes int64
		lastTicks := getCPUTicks()
		
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			now := time.Now()
			elapsedSec := int(now.Sub(start).Seconds())
			if elapsedSec >= 610 { break }

			currBytes := runningBytes.Load()
			ingestMBps := float64(currBytes-lastBytes) / (1024 * 1024)
			lastBytes = currBytes

			currTicks := getCPUTicks()
			cpuLoad := (currTicks - lastTicks) / 100.0
			lastTicks = currTicks

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			heapMB := float64(m.Alloc) / (1024 * 1024)
			
			agent := storage.GetGlobalAIAgent()
			inflow, _ := agent.GetMetricsSnapshot()
			policy := agent.PredictUploadPolicy(0, 512*1024*1024)

			str := fmt.Sprintf("%d,%d,%.2f,%.2f,%d,%d,%.1f,%.2f\n",
				elapsedSec, currentTargetMBps.Load(), ingestMBps, inflow, policy.Concurrency, policy.PCUPartSize/(1024*1024), heapMB, cpuLoad)
			csvFile.WriteString(str)
			csvFile.Sync()
		}
	}()

	go func() {
		for {
			time.Sleep(time.Duration(30 + rand.IntN(40)) * time.Second)
			newTarget := int64(2 + rand.IntN(348))
			currentTargetMBps.Store(newTarget)
		}
	}()

	fmt.Println("🚀 10-Minute Continuous Plottable Workload Started")
	endTime := time.Now().Add(10 * time.Minute)
	
	for time.Now().Before(endTime) {
		tTarget := currentTargetMBps.Load()
		bufSize := 1 * 1024 * 1024
		t0 := time.Now()
		
		n, err := w.Write(payload[:bufSize])
		if err != nil { break }
		runningBytes.Add(int64(n))
		
		expectedPace := float64(bufSize) / float64(tTarget*1024*1024)
		actualPace := time.Since(t0).Seconds()
		
		if actualPace < expectedPace {
			time.Sleep(time.Duration((expectedPace - actualPace) * float64(time.Second)))
		}
	}

	w.Close()
	csvFile.Close()
	fmt.Println("✅ 10-Minute Workload Closed.")
}
