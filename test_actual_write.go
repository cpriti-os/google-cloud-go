package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/storage"
)

func main() {
	ctx := context.Background()

	// Create client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	bucketName := "cpriti-dp-central1"
	objectName := "test-2mib-write"

	buf := make([]byte, 2*1024*1024) // 2MiB

	// Warmup
	w := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	w.ChunkSize = 2 * 1024 * 1024
	if _, err := w.Write(buf); err != nil {
		log.Fatalf("Warmup write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("Warmup close failed: %v", err)
	}

	// Benchmark
	start := time.Now()
	for i := 0; i < 10; i++ {
		w := client.Bucket(bucketName).Object(fmt.Sprintf("%s-%d", objectName, i)).NewWriter(ctx)
		w.ChunkSize = 2 * 1024 * 1024

		if _, err := w.Write(buf); err != nil {
			log.Fatalf("Write failed: %v", err)
		}
		if err := w.Close(); err != nil {
			log.Fatalf("Close failed: %v", err)
		}
	}
	fmt.Printf("10x 2MiB writes took %v\n", time.Since(start))
}
