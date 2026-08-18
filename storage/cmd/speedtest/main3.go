package main
import (
	"context"
	"fmt"
	"sync"
	"time"
	"cloud.google.com/go/storage"
)
func main() {
	ctx := context.Background()
	client, _ := storage.NewClient(ctx)
	b := client.Bucket("cpriti-sdk-autotune-bench")
	payload := make([]byte, 16*1024*1024)
	
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := b.Object(fmt.Sprintf("test_manual_%d.bin", i)).NewWriter(ctx)
			w.ChunkSize = 64 * 1024 * 1024
			w.Write(payload)
			w.Close()
		}(i)
	}
	wg.Wait()
	dur := time.Since(start).Seconds()
	
	// 32 workers * 16MB = 512MB
	fmt.Printf("Raw Multi-part GCS Speed: %.1f MB/s\n", 512.0 / dur)
}
