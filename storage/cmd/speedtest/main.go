package main
import (
	"context"
	"fmt"
	"time"
	"cloud.google.com/go/storage"
)
func main() {
	ctx := context.Background()
	client, _ := storage.NewClient(ctx)
	b := client.Bucket("cpriti-sdk-autotune-bench")
	
	payload := make([]byte, 1024*1024)
	
	// Test Tuned
	cfg := storage.DefaultAutoTuningConfig()
	tw := b.Object("test_tuned.bin").NewAdaptivePCUWriter(ctx, cfg)
	
	start := time.Now()
	for i := 0; i < 500; i++ { tw.Write(payload) } // Write 500MB
	tw.Close()
	dur := time.Since(start).Seconds()
	
	fmt.Printf("Tuned PCU Speed: %.1f MB/s\n", 500.0 / dur)
}
