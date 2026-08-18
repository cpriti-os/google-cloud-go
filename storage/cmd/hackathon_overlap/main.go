package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

type SecondData struct {
	TargetMBps   int64
	BaseMBps     float64
	TunedMBps    float64
	TunedWorkers int
	TunedChunkMB int
}

func main() {
	payload := make([]byte, 16*1024*1024)
	crand.Read(payload[:1000])

	// Hackathon Stage Narrative Profile (180 seconds / 3 Minutes)
	targetProfile := make([]int64, 180)
	stages := []struct {
		duration int
		target   int64
	}{
		{30, 20},  // Warmup: 20 MB/s
		{30, 1},   // Deep valley: 1 MB/s -> Drops to 1 Worker dynamically
		{30, 250}, // Ramp up: 250 MB/s -> Workers explode to handle it
		{30, 420}, // Extreme spike: 420 MB/s -> Hit plateau
		{30, 100}, // Stabilization
		{30, 380}, // Grand finale
	}
	sec := 0
	for _, s := range stages {
		for i := 0; i < s.duration; i++ {
			if sec < 180 {
				targetProfile[sec] = s.target
				sec++
			}
		}
	}
	masterData := make([]SecondData, 180)
	for i := 0; i < 180; i++ {
		masterData[i].TargetMBps = targetProfile[i]
	}
	ctx := context.Background()
	client, _ := storage.NewGRPCClient(ctx)
	defer client.Close()
	bucket := client.Bucket("cpriti-sdk-autotune-bench")

	fmt.Println("==================================================================")
	fmt.Println(" 🚀 EXECUTING 10-MINUTE OVERLAP TRACE (5 Min Base + 5 Min Tuned)")
	fmt.Println("==================================================================")

	// --- RUN 1: UNTUNED BASELINE ---
	fmt.Println("[1/2] Running Untuned Baseline SDK Trace (gRPC)...")
	bw := bucket.Object("trace_base.bin").NewWriter(ctx)
	bw.ChunkSize = 16 * 1024 * 1024 // Standard

	runTraceCore(bw, false, targetProfile, &masterData)
	bw.Close()

	// --- RUN 2: TUNED ADAPTIVE AI ---
	fmt.Println("[2/2] Running AI Adaptive SDK Trace (gRPC + PCU)...")
	cfg := storage.DefaultAutoTuningConfig()
	cfg.MaxMemoryBudget = 1024 * 1024 * 1024
	tw := bucket.Object("trace_tuned.bin").NewAdaptivePCUWriter(ctx, cfg)

	runTraceCore(tw, true, targetProfile, &masterData)
	tw.Close()

	// --- WRITE OUTPUT ---
	outPath := "/usr/local/google/home/cpriti/.gemini/jetski/brain/9d3761a8-b7d0-4249-bcb9-c4ed239286b8/10_Minute_Overlap_Data.csv"
	csvFile, _ := os.Create(outPath)
	defer csvFile.Close()
	csvFile.WriteString("TimeSec,TargetAppMBps,BaseSDK_MBps,TunedSDK_MBps,TunedWorkers,TunedChunkMB\n")
	for i, d := range masterData {
		csvFile.WriteString(fmt.Sprintf("%d,%d,%.2f,%.2f,%d,%d\n", i, d.TargetMBps, d.BaseMBps, d.TunedMBps, d.TunedWorkers, d.TunedChunkMB))
	}

	fmt.Println("✅ Complete! Overlap Data Exported to 10_Minute_Overlap_Data.csv")
}

func runTraceCore(w interface {
	Write(p []byte) (n int, err error)
}, isTuned bool, profile []int64, data *[]SecondData) {
	t0Total := time.Now()
	payload := make([]byte, 1024*1024) // 1MB blocks

	for sec := 0; sec < len(profile); sec++ {
		targetMBps := profile[sec]

		// tSecStart := time.Now()
		var bytesThisSec int64 = 0

		secTimeout := time.After(time.Second)
		// Perfect Smooth Flow Pacing (Micro-batching)
		chunkSize := 1024 * 1024
	
		targetBytes := int64(targetMBps * 1024 * 1024)
		if targetMBps > 0 {
			delayPerChunk := time.Second / time.Duration(targetMBps)
			InnerLoop:
			for bytesThisSec < targetBytes {
				select {
				case <-secTimeout:
					break InnerLoop
				default:
					cStart := time.Now()
					w.Write(payload[:chunkSize])
					bytesThisSec += int64(chunkSize)
					// Sleep exact remainder to perfectly pace
					time.Sleep(delayPerChunk - time.Since(cStart))
				}
			}
		} else {
			<-secTimeout // Target is 0, just wait
		}
		achieved := float64(bytesThisSec) / (1024 * 1024)

		// Fill Master Data Array
		if isTuned {
			(*data)[sec].TunedMBps = achieved
			agent := storage.GetGlobalAIAgent()
			policy := agent.PredictUploadPolicy(0, 512*1024*1024)
			(*data)[sec].TunedWorkers = policy.Concurrency
			(*data)[sec].TunedChunkMB = policy.ChunkSize / (1024 * 1024)
		} else {
			(*data)[sec].BaseMBps = achieved
		}
	}
	fmt.Printf("   -> Trace run completed in %v\n", time.Since(t0Total))
}
