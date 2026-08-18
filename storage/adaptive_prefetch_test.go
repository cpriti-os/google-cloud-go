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
	"bytes"
	"context"
	"io"
	"testing"
)

func TestAdaptivePrefetchReader_SequentialRead(t *testing.T) {
	// Generate 16 MiB dummy data
	dataSize := int64(16 * 1024 * 1024)
	sourceData := make([]byte, dataSize)
	for i := range sourceData {
		sourceData[i] = byte(i % 251)
	}

	fetchCount := 0
	fetcher := func(ctx context.Context, offset, length int64, dest []byte) (int, error) {
		fetchCount++
		if offset >= dataSize {
			return 0, io.EOF
		}
		end := offset + length
		if end > dataSize {
			end = dataSize
		}
		n := copy(dest, sourceData[offset:end])
		if offset+int64(n) >= dataSize {
			return n, io.EOF
		}
		return n, nil
	}

	guardrail := NewMemoryGuardrail(32 * 1024 * 1024)
	cfg := AdaptivePrefetchConfig{
		ChunkSize: 2 * 1024 * 1024, // 2 MiB chunks
		Depth:     2,
		Guardrail: guardrail,
	}

	ctx := context.Background()
	reader := NewAdaptivePrefetchReader(ctx, dataSize, fetcher, cfg)
	defer reader.Close()

	readBuf := make([]byte, 512*1024) // Read in 512 KiB increments
	var result bytes.Buffer

	for {
		n, err := reader.Read(readBuf)
		if n > 0 {
			result.Write(readBuf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
	}

	if int64(result.Len()) != dataSize {
		t.Fatalf("expected read size %d, got %d", dataSize, result.Len())
	}
	if !bytes.Equal(result.Bytes(), sourceData) {
		t.Fatalf("read content mismatch")
	}

	// Verify all memory is released after Close()
	if err := reader.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if guardrail.InUse() != 0 {
		t.Fatalf("expected 0 memory in use after close, got %d", guardrail.InUse())
	}
}

func TestAdaptivePrefetchReader_Seek(t *testing.T) {
	dataSize := int64(8 * 1024 * 1024)
	sourceData := make([]byte, dataSize)
	for i := range sourceData {
		sourceData[i] = byte(i % 199)
	}

	fetcher := func(ctx context.Context, offset, length int64, dest []byte) (int, error) {
		if offset >= dataSize {
			return 0, io.EOF
		}
		end := offset + length
		if end > dataSize {
			end = dataSize
		}
		n := copy(dest, sourceData[offset:end])
		return n, nil
	}

	guardrail := NewMemoryGuardrail(16 * 1024 * 1024)
	cfg := AdaptivePrefetchConfig{
		ChunkSize: 1 * 1024 * 1024,
		Depth:     1,
		Guardrail: guardrail,
	}

	ctx := context.Background()
	reader := NewAdaptivePrefetchReader(ctx, dataSize, fetcher, cfg)
	defer reader.Close()

	// Read first 100 bytes
	buf := make([]byte, 100)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		t.Fatalf("failed initial read: %v", err)
	}
	if !bytes.Equal(buf, sourceData[:100]) {
		t.Fatalf("initial read data mismatch")
	}

	// Seek to offset 4 MiB
	seekPos := int64(4 * 1024 * 1024)
	pos, err := reader.Seek(seekPos, io.SeekStart)
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if pos != seekPos {
		t.Fatalf("expected seek pos %d, got %d", seekPos, pos)
	}

	// Read 100 bytes from new seek position
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		t.Fatalf("failed post-seek read: %v", err)
	}
	if !bytes.Equal(buf, sourceData[seekPos:seekPos+100]) {
		t.Fatalf("post-seek read data mismatch")
	}
}
