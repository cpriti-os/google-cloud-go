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
	"errors"
	"io"
	"strings"
	"sync"

	"google.golang.org/api/googleapi"
)

const (
	// DefaultPrefetchChunkSize is 8 MiB per prefetched block.
	DefaultPrefetchChunkSize = 8 * 1024 * 1024

	// DefaultPrefetchDepth is 1 chunk lookahead.
	DefaultPrefetchDepth = 1
)

// RangeFetcher is a function that retrieves a byte range from GCS.
type RangeFetcher func(ctx context.Context, offset, length int64, dest []byte) (int, error)

// AdaptivePrefetchConfig configures the adaptive prefetching reader.
type AdaptivePrefetchConfig struct {
	ChunkSize int
	Depth     int
	Guardrail *MemoryGuardrail
}

// DefaultAdaptivePrefetchConfig returns the default configuration.
func DefaultAdaptivePrefetchConfig() AdaptivePrefetchConfig {
	return AdaptivePrefetchConfig{
		ChunkSize: DefaultPrefetchChunkSize,
		Depth:     DefaultPrefetchDepth,
		Guardrail: nil,
	}
}

// NewAdaptiveReader creates an io.ReadCloser that wraps standard GCS range fetches
// with client-side lookahead prefetching and memory guardrails when enabled.
// If cfg is nil or cfg.Enabled is false, it falls back to standard NewReader.
func (o *ObjectHandle) NewAdaptiveReader(ctx context.Context, cfg *AutoTuningConfig, opts ...ReaderOption) (io.ReadCloser, error) {
	if cfg == nil || !cfg.Enabled {
		return o.NewReader(ctx, opts...)
	}
	normCfg := cfg.Normalize()
	guardrail := NewMemoryGuardrail(normCfg.MaxMemoryBudget)
	chunkSize := DefaultPrefetchChunkSize

	var totalSize int64
	if attrs, err := o.Attrs(ctx); err == nil && attrs != nil {
		totalSize = attrs.Size
	}

	fetcher := func(fetchCtx context.Context, offset, length int64, dest []byte) (int, error) {
		if totalSize > 0 && offset >= totalSize {
			return 0, io.EOF
		}
		r, err := o.NewRangeReader(fetchCtx, offset, length, opts...)
		if err != nil {
			var gerr *googleapi.Error
			if errors.As(err, &gerr) && gerr.Code == 416 {
				return 0, io.EOF
			}
			if strings.Contains(err.Error(), "416") || strings.Contains(err.Error(), "InvalidRange") {
				return 0, io.EOF
			}
			return 0, err
		}
		defer r.Close()
		n, err := io.ReadFull(r, dest)
		if err == io.ErrUnexpectedEOF {
			return n, io.EOF
		}
		return n, err
	}

	prefetchCfg := AdaptivePrefetchConfig{
		ChunkSize: chunkSize,
		Depth:     normCfg.PrefetchDepth,
		Guardrail: guardrail,
	}

	return NewAdaptivePrefetchReader(ctx, totalSize, fetcher, prefetchCfg), nil
}

// NewAdaptiveRangeReader creates an io.ReadCloser that wraps a specific sub-range
// of a GCS object with adaptive prefetching and memory bounding.
// If cfg is nil or !cfg.Enabled, it falls back to standard NewRangeReader.
func (o *ObjectHandle) NewAdaptiveRangeReader(ctx context.Context, offset, length int64, cfg *AutoTuningConfig, opts ...ReaderOption) (io.ReadCloser, error) {
	if cfg == nil || !cfg.Enabled {
		return o.NewRangeReader(ctx, offset, length, opts...)
	}
	normCfg := cfg.Normalize()
	guardrail := NewMemoryGuardrail(normCfg.MaxMemoryBudget)

	targetEnd := offset + length
	fetcher := func(fetchCtx context.Context, off, size int64, dest []byte) (int, error) {
		if off >= targetEnd {
			return 0, io.EOF
		}
		fetchLen := size
		if off+fetchLen > targetEnd {
			fetchLen = targetEnd - off
		}
		if fetchLen <= 0 {
			return 0, io.EOF
		}
		r, err := o.NewRangeReader(fetchCtx, off, fetchLen, opts...)
		if err != nil {
			var gerr *googleapi.Error
			if errors.As(err, &gerr) && gerr.Code == 416 {
				return 0, io.EOF
			}
			if strings.Contains(err.Error(), "416") || strings.Contains(err.Error(), "InvalidRange") {
				return 0, io.EOF
			}
			return 0, err
		}
		defer r.Close()
		n, err := io.ReadFull(r, dest[:fetchLen])
		if err == io.ErrUnexpectedEOF {
			return n, io.EOF
		}
		return n, err
	}

	aiAgent := GetGlobalAIAgent()
	policy := aiAgent.PredictReadPolicy(length, guardrail.Limit())

	prefetchCfg := AdaptivePrefetchConfig{
		ChunkSize: policy.InitialChunkSize,
		Depth:     policy.PrefetchDepth,
		Guardrail: guardrail,
	}

	return NewAdaptivePrefetchReader(ctx, targetEnd, fetcher, prefetchCfg), nil
}

type prefetchBlock struct {
	offset int64
	data   []byte
	n      int
	err    error
}

type prefetchFuture chan *prefetchBlock

// AdaptivePrefetchReader provides an intelligent read-ahead stream wrapper over GCS objects.
// It overlaps network I/O with application compute, detects sequential access patterns,
// enforces global memory limits, dynamically ramps up chunk sizes to saturate NICs,
// and prevents GC churn via buffer recycling.
type AdaptivePrefetchReader struct {
	ctx       context.Context
	cancel    context.CancelFunc
	fetcher   RangeFetcher
	totalSize int64
	cfg       AdaptivePrefetchConfig
	guardrail *MemoryGuardrail

	// Dynamic sizing state
	currentChunkSize int
	maxChunkSize     int
	dynamicDepth     int
	blocksRead       int

	// Stream state
	readOffset      int64
	currentBlock    []byte
	currentBlockPos int
	currentBlockErr error

	// Ordered prefetch future queue
	queue        chan prefetchFuture
	nextFetchOff int64

	mu     sync.Mutex
	closed bool
}

// NewAdaptivePrefetchReader creates a new AdaptivePrefetchReader.
func NewAdaptivePrefetchReader(ctx context.Context, totalSize int64, fetcher RangeFetcher, cfg AdaptivePrefetchConfig) *AdaptivePrefetchReader {
	guardrail := cfg.Guardrail
	if guardrail == nil {
		guardrail = NewMemoryGuardrail(DefaultGlobalMemoryLimit)
	}

	aiAgent := GetGlobalAIAgent()
	policy := aiAgent.PredictReadPolicy(totalSize, guardrail.Limit())

	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = policy.InitialChunkSize
	}
	if cfg.Depth <= 0 {
		cfg.Depth = policy.PrefetchDepth
	}

	readerCtx, cancel := context.WithCancel(ctx)

	r := &AdaptivePrefetchReader{
		ctx:              readerCtx,
		cancel:           cancel,
		fetcher:          fetcher,
		totalSize:        totalSize,
		cfg:              cfg,
		guardrail:        guardrail,
		currentChunkSize: cfg.ChunkSize,
		maxChunkSize:     policy.MaxChunkSize,
		dynamicDepth:     cfg.Depth,
		blocksRead:       0,
		queue:            make(chan prefetchFuture, 8),
		nextFetchOff:     0,
	}

	// Trigger initial prefetches
	r.mu.Lock()
	for i := 0; i < r.dynamicDepth; i++ {
		r.scheduleNextPrefetchLocked()
	}
	r.mu.Unlock()

	return r
}

func (r *AdaptivePrefetchReader) scheduleNextPrefetchLocked() {
	if r.closed || (r.totalSize > 0 && r.nextFetchOff >= r.totalSize) {
		return
	}

	offset := r.nextFetchOff
	chunkSize := r.currentChunkSize
	if r.totalSize > 0 && offset+int64(chunkSize) > r.totalSize {
		chunkSize = int(r.totalSize - offset)
	}
	if chunkSize <= 0 {
		return
	}

	// Clamp to available guardrail memory if constrained
	if available := int(r.guardrail.Available()); available > 0 && available < chunkSize {
		if available >= 1024*1024 {
			chunkSize = available
		}
	}

	r.nextFetchOff += int64(chunkSize)

	future := make(prefetchFuture, 1)
	r.queue <- future

	go func(fut prefetchFuture, off int64, size int) {
		buf := r.guardrail.GetBuffer(size)
		if buf == nil {
			buf = make([]byte, size)
		}

		n, err := r.fetcher(r.ctx, off, int64(size), buf)
		if r.ctx.Err() != nil {
			r.guardrail.PutBuffer(buf)
			fut <- &prefetchBlock{offset: off, err: r.ctx.Err()}
			return
		}

		fut <- &prefetchBlock{offset: off, data: buf, n: n, err: err}
	}(future, offset, chunkSize)
}

// Read reads up to len(p) bytes from the prefetched stream into p.
func (r *AdaptivePrefetchReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Check if current block has remaining bytes
	if r.currentBlockPos < len(r.currentBlock) {
		n := copy(p, r.currentBlock[r.currentBlockPos:])
		r.currentBlockPos += n
		r.readOffset += int64(n)
		return n, nil
	}

	// Return error if previous block had a terminal error (like EOF)
	if r.currentBlockErr != nil {
		return 0, r.currentBlockErr
	}

	// Release previous block memory back to pool
	if r.currentBlock != nil {
		r.guardrail.PutBuffer(r.currentBlock)
		r.currentBlock = nil
	}

	// Dynamic scaling: ramp up chunk size on sequential reads
	r.blocksRead++
	if r.blocksRead > 1 && r.currentChunkSize < r.maxChunkSize {
		r.currentChunkSize *= 2
		if r.currentChunkSize > r.maxChunkSize {
			r.currentChunkSize = r.maxChunkSize
		}
	}

	// Fetch next future from prefetch queue
	var future prefetchFuture
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case fut, ok := <-r.queue:
		if !ok {
			return 0, io.EOF
		}
		future = fut
	default:
		// Consumer read starvation detected (queue was drained before next block arrived).
		// Scale up prefetch depth and jump chunk size to max to saturate the pipe!
		if r.dynamicDepth < 4 {
			r.dynamicDepth++
		}
		r.currentChunkSize = r.maxChunkSize

		if r.totalSize > 0 && r.readOffset >= r.totalSize {
			return 0, io.EOF
		}
		r.scheduleNextPrefetchLocked()
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case future = <-r.queue:
		}
	}

	// Wait for future to resolve
	var block *prefetchBlock
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case b := <-future:
		block = b
	}

	if block == nil {
		return 0, io.EOF
	}

	// Schedule following prefetch chunk to maintain dynamic lookahead depth
	r.scheduleNextPrefetchLocked()

	if block.err != nil && block.n == 0 {
		if block.data != nil {
			r.guardrail.PutBuffer(block.data)
		}
		r.currentBlockErr = block.err
		return 0, block.err
	}

	r.currentBlock = block.data[:block.n]
	r.currentBlockPos = 0
	r.currentBlockErr = block.err

	n := copy(p, r.currentBlock)
	r.currentBlockPos = n
	r.readOffset += int64(n)
	return n, nil
}

// Seek resets the stream offset. If seeking sequentially, prefetching continues;
// if seeking non-sequentially, active queued chunks are discarded and prefetch resets.
func (r *AdaptivePrefetchReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, io.ErrClosedPipe
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.readOffset + offset
	case io.SeekEnd:
		if r.totalSize <= 0 {
			return 0, errors.New("cannot seek from end with unknown total size")
		}
		newOffset = r.totalSize + offset
	default:
		return 0, errors.New("invalid seek whence")
	}

	if newOffset < 0 {
		return 0, errors.New("negative seek offset")
	}

	if newOffset == r.readOffset {
		return newOffset, nil
	}

	// Non-sequential seek: purge current block and queued prefetches
	if r.currentBlock != nil {
		r.guardrail.PutBuffer(r.currentBlock)
		r.currentBlock = nil
	}
	r.currentBlockPos = 0
	r.currentBlockErr = nil

	// Drain queue
DRAIN:
	for {
		select {
		case future := <-r.queue:
			if future != nil {
				select {
				case b := <-future:
					if b != nil && b.data != nil {
						r.guardrail.PutBuffer(b.data)
					}
				default:
				}
			}
		default:
			break DRAIN
		}
	}

	r.readOffset = newOffset
	r.nextFetchOff = newOffset
	r.currentChunkSize = r.cfg.ChunkSize
	r.blocksRead = 0

	// Restart prefetch from new offset
	for i := 0; i < r.dynamicDepth; i++ {
		r.scheduleNextPrefetchLocked()
	}

	return newOffset, nil
}

// Close terminates all active background prefetches and reclaims all memory buffers.
func (r *AdaptivePrefetchReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel()

	if r.currentBlock != nil {
		r.guardrail.PutBuffer(r.currentBlock)
		r.currentBlock = nil
	}

	// Drain and free remaining queued buffers
DRAIN:
	for {
		select {
		case future := <-r.queue:
			if future != nil {
				select {
				case b := <-future:
					if b != nil && b.data != nil {
						r.guardrail.PutBuffer(b.data)
					}
				default:
				}
			}
		default:
			break DRAIN
		}
	}

	return nil
}
