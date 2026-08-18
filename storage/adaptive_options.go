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

// AutoTuningConfig encapsulates client-side adaptive auto-tuning settings.
// Auto-tuning is an opt-in feature designed to optimize high-throughput AI/ML
// and analytics workloads (e.g., Orbax checkpointing, PyTorch DataLoader streaming,
// columnar Parquet scans) while strictly bounding memory and CPU usage.
type AutoTuningConfig struct {
	// Enabled determines whether adaptive auto-tuning is active.
	// Defaults to false (opt-in).
	Enabled bool

	// MaxMemoryBudget defines the client-wide memory ceiling in bytes for
	// prefetch slab buffers and dynamic memory guardrails.
	// If 0, defaults to 256 MiB.
	MaxMemoryBudget int64

	// InitialUploadChunkSize defines the starting chunk size for uploads in bytes.
	// If 0, the SDK dynamically infers the optimal initial chunk size from
	// the payload size (e.g., single-shot for <=8MB, 64MB for >=256MB).
	InitialUploadChunkSize int

	// MaxUploadChunkSize defines the upper bound for AIMD upload chunk scaling.
	// Defaults to 64 MiB (or up to 256 MiB if explicitly set).
	MaxUploadChunkSize int

	// PrefetchDepth sets the lookahead block queue depth for sequential reads.
	// Defaults to 2 blocks.
	PrefetchDepth int
}

// DefaultAutoTuningConfig returns the recommended auto-tuning configuration
// for high-throughput AI/ML and analytics workloads.
func DefaultAutoTuningConfig() *AutoTuningConfig {
	return &AutoTuningConfig{
		Enabled:                true,
		MaxMemoryBudget:        256 * 1024 * 1024, // 256 MiB
		InitialUploadChunkSize: 0,                 // Dynamic inference
		MaxUploadChunkSize:     64 * 1024 * 1024,  // 64 MiB
		PrefetchDepth:          2,
	}
}

// Normalize ensures that all parameters in AutoTuningConfig have valid non-zero
// defaults if enabled.
func (c *AutoTuningConfig) Normalize() *AutoTuningConfig {
	if c == nil {
		return &AutoTuningConfig{Enabled: false}
	}
	cfg := *c
	if cfg.MaxMemoryBudget <= 0 {
		cfg.MaxMemoryBudget = 256 * 1024 * 1024
	}
	if cfg.MaxUploadChunkSize <= 0 {
		cfg.MaxUploadChunkSize = 64 * 1024 * 1024
	}
	if cfg.PrefetchDepth <= 0 {
		cfg.PrefetchDepth = 2
	}
	return &cfg
}
