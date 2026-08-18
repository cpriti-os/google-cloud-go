package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

type SecondData struct {
	TargetMBps    int64
	BaseMBps      float64
	TunedMBps     float64
	TunedWorkers  int
	TunedChunkMB  int
}

func main() {
	payload := make([]byte, 16*1024*1024)
	crand.Read(payload[:1000])

	// 1. Generate a deterministic 5-minute demand profile array (300 seconds)
	// We'll change the target throughput every 15-45 seconds
	targetProfile := make([]int64, 300)
	var currTarget int64 = 10
	var ticksLeft = 15
	for i := 0; i < 300; i++ {
		if ticksLeft <= 0 {
			currTarget = int64(2 + rand.IntN(348))
			ticksLeft = 15 + rand.IntN(30)
		}
		targetProfile[i] = currTarget
		ticksLeft--
	}

	masterData := make([]SecondData, 300)
	for i:=0; i<300; i++ { masterData[i].TargetMBps = targetProfile[i] }

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
	cfg.MaxMemoryBudget = 512 * 1024 * 1024
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

func runTraceCore(w interface{Write(p []byte) (n int, err error)}, isTuned bool, profile []int64, data *[]SecondData) {
	t0Total := time.Now()
	payload := make([]byte, 1024*1024) // 1MB blocks

	for sec := 0; sec < len(profile); sec++ {
		targetMBps := profile[sec]
		
		tSecStart := time.Now()
		var bytesThisSec int64 = 0
		
		secTimeout := time.After(time.Second)
	InnerLoop:
		for {
			select {
			case <-secTimeout:
				break InnerLoop
			default:
				_, err := w.Write(payload)
				if err != nil { break InnerLoop }
				bytesThisSec += int64(len(payload))

				// Throttle application to match TargetMBps
				elapsed := time.Since(tSecStart).Seconds()
				expectedBytes := float64(targetMBps * 1024 * 1024) * elapsed
				drift := expectedBytes - float64(bytesThisSec) // BUG HERE float64(bytesThisSec)
				if drift < 0 { 
					waitTime := (-drift) / float64(targetMBps * 1024 * 1024)
					if waitTime > 0.005 { time.Sleep(time.Duration(waitTime * float64(time.Second))) }
				}
			}
		}

		achieved := float64(bytesThisSec) / (1024 * 1024)
		
		// Fill Master Data Array
		if isTuned {
			(*data)[sec].TunedMBps = achieved
			agent := storage.GetGlobalAIAgent()
			policy := agent.PredictUploadPolicy(0, 512*1024*1024)
			(*data)[sec].TunedWorkers = policy.Concurrency
			(*data)[sec].TunedChunkMB = policy.PCUPartSize / (1024*1024)
		} else {
			(*data)[sec].BaseMBps = achieved
		}
	}
	fmt.Printf("   -> Trace run completed in %v\n", time.Since(t0Total))
}
