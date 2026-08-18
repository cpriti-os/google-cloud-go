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
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	// GoogleAPIBaseChunkMultiple is 256 KiB, required by GCS resumable upload chunking.
	GoogleAPIBaseChunkMultiple = 256 * 1024

	// DefaultMinUploadChunkSize is the minimum chunk size (256 KiB).
	DefaultMinUploadChunkSize = 256 * 1024

	// DefaultMaxUploadChunkSize is the maximum chunk size (64 MiB).
	DefaultMaxUploadChunkSize = 64 * 1024 * 1024

	// DefaultInitialUploadChunkSize is the standard 16 MiB default.
	DefaultInitialUploadChunkSize = 16 * 1024 * 1024

	// DefaultUploadTargetLatency is the ideal transfer duration per chunk.
	DefaultUploadTargetLatency = 500 * time.Millisecond

	// DefaultUploadAlpha is the EWMA smoothing factor for bandwidth estimation.
	DefaultUploadAlpha = 0.3
)

// DynamicChunkConfig configures the dynamic upload chunk and part sizer.
type DynamicChunkConfig struct {
	MinChunkSize     int
	MaxChunkSize     int
	InitialChunkSize int
	TargetLatency    time.Duration
	Alpha            float64
}

// DefaultDynamicChunkConfig returns the default configuration.
func DefaultDynamicChunkConfig() DynamicChunkConfig {
	return DynamicChunkConfig{
		MinChunkSize:     DefaultMinUploadChunkSize,
		MaxChunkSize:     DefaultMaxUploadChunkSize,
		InitialChunkSize: DefaultInitialUploadChunkSize,
		TargetLatency:    DefaultUploadTargetLatency,
		Alpha:            DefaultUploadAlpha,
	}
}

// DynamicChunkSizer dynamically adjusts upload chunk and shard sizes based on
// object size hints, observed network throughput, and retry/stall feedback using
// an AIMD (Additive Increase / Multiplicative Decrease) control loop.
type DynamicChunkSizer struct {
	cfg              DynamicChunkConfig
	currentChunkSize int
	smoothedBps      float64 // smoothed bytes per second (EWMA)
	guardrail        *MemoryGuardrail
	mu               sync.RWMutex
}

// NewDynamicChunkSizer creates a new DynamicChunkSizer.
func NewDynamicChunkSizer(cfg DynamicChunkConfig, guardrail *MemoryGuardrail) *DynamicChunkSizer {
	if cfg.MinChunkSize <= 0 {
		cfg.MinChunkSize = DefaultMinUploadChunkSize
	}
	if cfg.MaxChunkSize < cfg.MinChunkSize {
		cfg.MaxChunkSize = DefaultMaxUploadChunkSize
	}
	if cfg.InitialChunkSize < cfg.MinChunkSize {
		cfg.InitialChunkSize = cfg.MinChunkSize
	}
	if cfg.InitialChunkSize > cfg.MaxChunkSize {
		cfg.InitialChunkSize = cfg.MaxChunkSize
	}
	if cfg.TargetLatency <= 0 {
		cfg.TargetLatency = DefaultUploadTargetLatency
	}
	if cfg.Alpha <= 0 || cfg.Alpha > 1.0 {
		cfg.Alpha = DefaultUploadAlpha
	}

	return &DynamicChunkSizer{
		cfg:              cfg,
		currentChunkSize: roundToChunkMultiple(cfg.InitialChunkSize),
		guardrail:        guardrail,
	}
}

func roundToChunkMultiple(size int) int {
	if size <= 0 {
		return 0
	}
	rem := size % GoogleAPIBaseChunkMultiple
	if rem != 0 {
		size += (GoogleAPIBaseChunkMultiple - rem)
	}
	return size
}

// RecommendInitialChunkSize calculates the initial chunk size for an upload
// based on the object size hint and current network state.
// If objectSizeHint <= 8 MiB, it returns 0 to recommend single-shot media upload.
func (s *DynamicChunkSizer) RecommendInitialChunkSize(objectSizeHint int64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// For small objects under 8 MiB, bypass resumable upload protocol to eliminate 1-2 RTTs.
	if objectSizeHint > 0 && objectSizeHint <= 8*1024*1024 {
		return 0
	}

	if objectSizeHint > 0 {
		if objectSizeHint <= 32*1024*1024 {
			return 8 * 1024 * 1024
		}
		if objectSizeHint <= 128*1024*1024 {
			return 16 * 1024 * 1024
		}
		if objectSizeHint <= 512*1024*1024 {
			return 32 * 1024 * 1024
		}
		// For massive objects (> 512 MiB, ML checkpoints), scale up initial chunk.
		return s.clampChunkSize(64 * 1024 * 1024)
	}

	return s.currentChunkSize
}

// RecordChunkTransfer updates throughput metrics and adjusts chunk size for subsequent chunks.
func (s *DynamicChunkSizer) RecordChunkTransfer(bytesTransferred int64, duration time.Duration, isErrorOrStall bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bytesTransferred <= 0 || duration <= 0 {
		if isErrorOrStall {
			s.currentChunkSize = s.clampChunkSize(s.currentChunkSize / 2)
		}
		return s.currentChunkSize
	}

	instantBps := float64(bytesTransferred) / duration.Seconds()
	if s.smoothedBps == 0 {
		s.smoothedBps = instantBps
	} else {
		s.smoothedBps = s.cfg.Alpha*instantBps + (1.0-s.cfg.Alpha)*s.smoothedBps
	}

	if isErrorOrStall {
		// Multiplicative decrease: halve the chunk size to reduce retry retransmission cost and memory lockup.
		s.currentChunkSize = s.clampChunkSize(s.currentChunkSize / 2)
	} else {
		// If transfer finished faster than target latency and network throughput is high, scale up chunk size.
		if duration < s.cfg.TargetLatency {
			// Estimate bandwidth-delay product for target latency.
			targetChunk := int(s.smoothedBps * s.cfg.TargetLatency.Seconds())
			if targetChunk > s.currentChunkSize {
				// Additive / bounded ramp-up
				growth := (targetChunk - s.currentChunkSize) / 2
				if growth < GoogleAPIBaseChunkMultiple {
					growth = GoogleAPIBaseChunkMultiple
				}
				s.currentChunkSize = s.clampChunkSize(s.currentChunkSize + growth)
			}
		} else if duration > 2*s.cfg.TargetLatency {
			// Transfer was slow: moderate downscale
			s.currentChunkSize = s.clampChunkSize(int(float64(s.currentChunkSize) * 0.75))
		}
	}

	return s.currentChunkSize
}

func (s *DynamicChunkSizer) clampChunkSize(size int) int {
	rounded := roundToChunkMultiple(size)
	if rounded < s.cfg.MinChunkSize {
		rounded = s.cfg.MinChunkSize
	}
	if rounded > s.cfg.MaxChunkSize {
		rounded = s.cfg.MaxChunkSize
	}

	// If memory guardrail is configured and available budget is constrained, clamp further.
	if s.guardrail != nil {
		available := int(s.guardrail.Available())
		if available > s.cfg.MinChunkSize && rounded > available {
			rounded = roundToChunkMultiple(available)
		}
	}

	return rounded
}

// CurrentChunkSize returns the current chunk size setting.
func (s *DynamicChunkSizer) CurrentChunkSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentChunkSize
}

// SmoothedThroughputMBps returns the current smoothed throughput estimate in MB/s.
func (s *DynamicChunkSizer) SmoothedThroughputMBps() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return (s.smoothedBps / (1024 * 1024))
}

// CalculateBDPChunkSize computes the optimal chunk size to fill the bandwidth-delay product
// for a given target network latency without exceeding the memory budget.
func (s *DynamicChunkSizer) CalculateBDPChunkSize(targetLatency time.Duration) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if targetLatency <= 0 {
		targetLatency = s.cfg.TargetLatency
	}
	if s.smoothedBps <= 0 {
		return s.currentChunkSize
	}

	targetBytes := int(s.smoothedBps * targetLatency.Seconds())
	return s.clampChunkSize(targetBytes)
}

// CalculateDynamicPCUConfig determines the optimal part size and worker concurrency
// for Parallel Composite Uploads based on total payload size, measured network velocity,
// and client memory budget.
func CalculateDynamicPCUConfig(totalSize int64, smoothedMBps float64, memoryBudget int64) (partSize int, workerCount int) {
	if totalSize <= 0 {
		return 32 * 1024 * 1024, 4
	}
	if memoryBudget <= 0 {
		memoryBudget = DefaultGlobalMemoryLimit
	}

	// Base part sizing from payload
	if totalSize < 256*1024*1024 {
		partSize = 16 * 1024 * 1024
		workerCount = 4
	} else if totalSize < 1024*1024*1024 {
		partSize = 32 * 1024 * 1024
		workerCount = 4
	} else if totalSize < 4*1024*1024*1024 {
		partSize = 64 * 1024 * 1024
		workerCount = 8
	} else {
		partSize = 128 * 1024 * 1024
		workerCount = 16
	}

	// Scale worker count dynamically if measured network throughput is high
	if smoothedMBps > 100.0 && totalSize >= 512*1024*1024 {
		workerCount *= 2
		if workerCount > 32 {
			workerCount = 32
		}
	}

	// Enforce memory guardrail: workers * partSize <= memoryBudget
	requiredMemory := int64(workerCount * partSize)
	for requiredMemory > memoryBudget && workerCount > 2 {
		workerCount /= 2
		requiredMemory = int64(workerCount * partSize)
	}
	for requiredMemory > memoryBudget && partSize > 16*1024*1024 {
		partSize /= 2
		requiredMemory = int64(workerCount * partSize)
	}

	return partSize, workerCount
}

// AdaptivePCUWriter provides a transparent, dynamic Parallel Composite Upload stream.
// It automatically partitions incoming write streams into dynamically sized parts (e.g., 16MB - 128MB)
// based on payload size and network bandwidth, uploads them concurrently across worker routines,
// and atomically composes them into the destination object on Close().
type AdaptivePCUWriter struct {
	ctx           context.Context
	cancel        context.CancelFunc
	bucket        *BucketHandle
	object        string
	cfg           *AutoTuningConfig
	guardrail     *MemoryGuardrail
	dynamicPartSz int
	concurrency   int

	currentPartBuf []byte
	currentPartPos int
	partIndex      int
	partObjects    []*ObjectHandle

	err       error
	mu        sync.Mutex
	wg        sync.WaitGroup
	uploadSem chan struct{}
	closed    bool
}

// NewAdaptivePCUWriter creates a new AdaptivePCUWriter on the given object handle.
func (o *ObjectHandle) NewAdaptivePCUWriter(ctx context.Context, cfg *AutoTuningConfig) *AdaptivePCUWriter {
	cfg = cfg.Normalize()
	guardrail := NewMemoryGuardrail(cfg.MaxMemoryBudget)

	aiAgent := GetGlobalAIAgent()
	policy := aiAgent.PredictUploadPolicy(0, guardrail.Limit())

	partSize := policy.PCUPartSize
	if partSize <= 0 {
		partSize = cfg.PCUPartSize
	}
	concurrency := policy.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	wCtx, cancel := context.WithCancel(ctx)
	return &AdaptivePCUWriter{
		ctx:            wCtx,
		cancel:         cancel,
		bucket:         o.c.Bucket(o.bucket),
		object:         o.object,
		cfg:            cfg,
		guardrail:      guardrail,
		dynamicPartSz:  partSize,
		concurrency:    concurrency,
		currentPartBuf: make([]byte, partSize),
		uploadSem:      make(chan struct{}, 64),
	}
}

// Write writes data to the PCU stream, automatically partitioning into dynamic parts.
func (w *AdaptivePCUWriter) Write(p []byte) (int, error) {
	startTimer := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}
	if w.err != nil {
		return 0, w.err
	}

	total := len(p)
	for len(p) > 0 {
		avail := len(w.currentPartBuf) - w.currentPartPos
		toCopy := len(p)
		if toCopy > avail {
			toCopy = avail
		}
		copy(w.currentPartBuf[w.currentPartPos:], p[:toCopy])
		w.currentPartPos += toCopy
		p = p[toCopy:]

		if w.currentPartPos >= len(w.currentPartBuf) {
			w.flushCurrentPartLocked()
		}
	}
	
	// TELEMETRY BUG FIX: Inform AI Agent of incoming velocities!
	GetGlobalAIAgent().RecordWriteInflow(total, time.Since(startTimer))
	
	return total, nil 
}
func (w *AdaptivePCUWriter) flushCurrentPartLocked() {
	if w.currentPartPos == 0 || w.err != nil {
		return
	}

	partData := make([]byte, w.currentPartPos)
	copy(partData, w.currentPartBuf[:w.currentPartPos])
	w.currentPartPos = 0

	partName := fmt.Sprintf("%s.tmp_part_%d_%d", w.object, time.Now().UnixNano(), w.partIndex)
	partObj := w.bucket.Object(partName)
	w.partObjects = append(w.partObjects, partObj)
	w.partIndex++

	w.uploadSem <- struct{}{}
	w.wg.Add(1)
	go func(obj *ObjectHandle, data []byte) {
		defer func() {
			<-w.uploadSem
			w.wg.Done()
		}()

		pw := obj.NewWriter(w.ctx)
		pw.ChunkSize = 64 * 1024 * 1024
		if _, err := pw.Write(data); err != nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.mu.Unlock()
			_ = pw.Close()
			return
		}
		if err := pw.Close(); err != nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.mu.Unlock()
		}
	}(partObj, partData)
}

// Close finalizes all pending parts and composes them into the destination object.
func (w *AdaptivePCUWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true

	// Flush remaining bytes
	if w.currentPartPos > 0 && w.err == nil {
		w.flushCurrentPartLocked()
	}
	w.mu.Unlock()

	// Wait for all concurrent part uploads
	w.wg.Wait()

	if w.err != nil {
		w.cleanupParts(w.partObjects)
		return w.err
	}

	if len(w.partObjects) == 0 {
		// Empty object write
		emptyW := w.bucket.Object(w.object).NewWriter(w.ctx)
		return emptyW.Close()
	}

	if len(w.partObjects) == 1 {
		// Single part: rename/copy directly
		_, err := w.bucket.Object(w.object).CopierFrom(w.partObjects[0]).Run(w.ctx)
		_ = w.partObjects[0].Delete(w.ctx)
		return err
	}

	// Compose parts
	composer := w.bucket.Object(w.object).ComposerFrom(w.partObjects...)
	composer.DeleteSourceObjects = true
	_, err := composer.Run(w.ctx)
	return err
}

func (w *AdaptivePCUWriter) cleanupParts(parts []*ObjectHandle) {
	for _, p := range parts {
		_ = p.Delete(context.Background())
	}
}
