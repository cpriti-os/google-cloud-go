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
	"testing"
	"time"
)

func TestAdaptiveAIAgent_WorkloadClassification(t *testing.T) {
	agent := NewAdaptiveAIAgent(nil)

	// Test 1: Large Checkpoint Hint
	c1 := agent.ClassifyWorkload(512 * 1024 * 1024)
	if c1 != WorkloadClassLargeCheckpoint {
		t.Errorf("Expected WorkloadClassLargeCheckpoint, got %v", c1)
	}

	// Test 2: Small Random I/O Hint
	c2 := agent.ClassifyWorkload(64 * 1024)
	if c2 != WorkloadClassSmallRandomIO {
		t.Errorf("Expected WorkloadClassSmallRandomIO, got %v", c2)
	}

	// Test 3: Telemetry-driven Small Random I/O
	agentRandom := NewAdaptiveAIAgent(nil)
	for i := 0; i < 20; i++ {
		// Non-sequential 4KB reads
		agentRandom.RecordRead(int64(i*1000000), 4096, 5*time.Millisecond, false)
	}
	c3 := agentRandom.ClassifyWorkload(0)
	if c3 != WorkloadClassSmallRandomIO {
		t.Errorf("Expected WorkloadClassSmallRandomIO from random telemetry, got %v", c3)
	}

	// Test 4: Telemetry-driven Streaming Sequential
	agentStream := NewAdaptiveAIAgent(nil)
	var off int64
	for i := 0; i < 20; i++ {
		// Sequential 8MB reads
		agentStream.RecordRead(off, 8*1024*1024, 50*time.Millisecond, false)
		off += 8 * 1024 * 1024
	}
	c4 := agentStream.ClassifyWorkload(0)
	if c4 != WorkloadClassStreamingSequential {
		t.Errorf("Expected WorkloadClassStreamingSequential from sequential telemetry, got %v", c4)
	}
}

func TestAdaptiveAIAgent_PredictUploadPolicy(t *testing.T) {
	agent := NewAdaptiveAIAgent(nil)

	// Small file upload policy
	smallPolicy := agent.PredictUploadPolicy(128*1024, 256*1024*1024)
	if smallPolicy.ChunkSize > 256*1024 {
		t.Errorf("Expected small chunk <= 256KB for small file, got %d", smallPolicy.ChunkSize)
	}
	if !smallPolicy.DirectUpload {
		t.Errorf("Expected DirectUpload=true for 128KB file")
	}

	// Large 1GB checkpoint policy
	largePolicy := agent.PredictUploadPolicy(1024*1024*1024, 256*1024*1024)
	if largePolicy.ChunkSize < 32*1024*1024 {
		t.Errorf("Expected large chunk >= 32MB for 1GB checkpoint, got %d", largePolicy.ChunkSize)
	}
	if largePolicy.DirectUpload {
		t.Errorf("Expected DirectUpload=false for 1GB checkpoint")
	}
}

func TestAdaptiveAIAgent_PredictReadPolicy(t *testing.T) {
	agent := NewAdaptiveAIAgent(nil)

	// Small random read policy
	smallRead := agent.PredictReadPolicy(256*1024, 256*1024*1024)
	if smallRead.InitialChunkSize != 256*1024 {
		t.Errorf("Expected 256KB initial chunk for small random read, got %d", smallRead.InitialChunkSize)
	}
	if !smallRead.IsSparse {
		t.Errorf("Expected IsSparse=true for small random read")
	}

	// Large streaming read policy
	largeRead := agent.PredictReadPolicy(1024*1024*1024, 256*1024*1024)
	if largeRead.InitialChunkSize != 8*1024*1024 {
		t.Errorf("Expected 8MB initial chunk for streaming read, got %d", largeRead.InitialChunkSize)
	}
	if largeRead.MaxChunkSize < 64*1024*1024 {
		t.Errorf("Expected MaxChunkSize >= 64MB for streaming read, got %d", largeRead.MaxChunkSize)
	}
	if largeRead.IsSparse {
		t.Errorf("Expected IsSparse=false for streaming read")
	}
}

func TestAdaptiveAIAgent_FeedbackReward(t *testing.T) {
	agent := NewAdaptiveAIAgent(nil)
	agent.UpdateFeedback(150.0, 0.050, 128.0)
	if agent.rewardEwma <= 0 {
		t.Errorf("Expected positive reward for 150MB/s and 50ms latency, got %f", agent.rewardEwma)
	}
}

func TestAdaptiveAIAgent_ProducerInflowTracking(t *testing.T) {
	agent := NewAdaptiveAIAgent(nil)

	// Simulate slow producer (e.g., 2 MB/s)
	for i := 0; i < 10; i++ {
		agent.RecordWriteInflow(2*1024*1024, 1000*time.Millisecond)
	}
	slowPolicy := agent.PredictUploadPolicy(0, 256*1024*1024)
	if slowPolicy.ChunkSize > 4*1024*1024 {
		t.Errorf("Expected small chunk <= 4MB for slow producer, got %d", slowPolicy.ChunkSize)
	}

	// Simulate fast producer (e.g., 200 MB/s)
	for i := 0; i < 10; i++ {
		agent.RecordWriteInflow(64*1024*1024, 300*time.Millisecond)
	}
	fastPolicy := agent.PredictUploadPolicy(0, 256*1024*1024)
	if fastPolicy.ChunkSize < 32*1024*1024 {
		t.Errorf("Expected large chunk >= 32MB for fast producer, got %d", fastPolicy.ChunkSize)
	}
}

func TestAdaptiveAIAgent_PrefetchSelfHealing(t *testing.T) {
	agent := NewAdaptiveAIAgent(nil)

	// Simulate high prefetch waste (e.g., app requested 1MB but 50MB was discarded)
	agent.RecordPrefetchFeedback(1*1024*1024, 50*1024*1024, false)
	policy := agent.PredictReadPolicy(100*1024*1024, 256*1024*1024)

	// Should disable or throttle prefetching when hit ratio is low (< 40%)
	if policy.Strategy != PrefetchStrategyDisabled {
		t.Errorf("Expected PrefetchStrategyDisabled when waste ratio > 60%%, got %v", policy.Strategy)
	}
	if policy.PrefetchDepth != 0 {
		t.Errorf("Expected PrefetchDepth=0 when prefetch is unhelpful, got %d", policy.PrefetchDepth)
	}
}
