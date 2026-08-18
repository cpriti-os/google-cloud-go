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
// Parallel Composite Uploads, columnar Parquet scans) while strictly bounding
// memory and CPU usage.
type AutoTuningConfig struct {
	// Enabled determines whether adaptive auto-tuning is active.
	// Defaults to false (opt-in).
	Enabled bool

	// MaxMemoryBudget defines the client-wide memory ceiling in bytes for
	// prefetch slab buffers, PCU part buffers, and dynamic memory guardrails.
	// If 0, defaults to 256 MiB.
	MaxMemoryBudget int64

	// InitialUploadChunkSize defines the starting chunk size for uploads in bytes.
	// Defaults to 32 MiB (32 * 1024 * 1024) in DefaultAutoTuningConfig.
	InitialUploadChunkSize int

	// MaxUploadChunkSize defines the upper bound for AIMD upload chunk scaling.
	// Defaults to 64 MiB (or up to 256 MiB if explicitly configured).
	MaxUploadChunkSize int

	// PCUPartSize defines the initial part size for Parallel Composite Uploads.
	// Defaults to 32 MiB (32 * 1024 * 1024), scaling dynamically up to 128-256 MiB
	// for multi-gigabyte payloads to minimize GCS Compose tree depth.
	PCUPartSize int

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
		InitialUploadChunkSize: 32 * 1024 * 1024,  // 32 MiB default initial chunk
		MaxUploadChunkSize:     64 * 1024 * 1024,  // 64 MiB max chunk
		PCUPartSize:            32 * 1024 * 1024,  // 32 MiB default PCU part size
		PrefetchDepth:          2,                 // 2-block lookahead pipeline
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
	if cfg.InitialUploadChunkSize <= 0 {
		cfg.InitialUploadChunkSize = 32 * 1024 * 1024
	}
	if cfg.MaxUploadChunkSize <= 0 {
		cfg.MaxUploadChunkSize = 64 * 1024 * 1024
	}
	if cfg.PCUPartSize <= 0 {
		cfg.PCUPartSize = 32 * 1024 * 1024
	}
	if cfg.PrefetchDepth <= 0 {
		cfg.PrefetchDepth = 2
	}
	return &cfg
}

// DynamicPCUPartSize computes the optimal part size for Parallel Composite Uploads (PCU).
// GCS Compose has a limit of 32 source objects per single compose call.
// Choosing larger part sizes for multi-gigabyte AI/ML model checkpoints prevents
// deep hierarchical Compose trees, cuts API metadata overhead, and reduces temporary object cleanup costs.
func DynamicPCUPartSize(totalObjectSize int64, maxConcurrency int) int {
	const (
		minPart   = 16 * 1024 * 1024  // 16 MiB
		mediumPart = 32 * 1024 * 1024 // 32 MiB
		largePart = 64 * 1024 * 1024  // 64 MiB
		xlargePart = 128 * 1024 * 1024 // 128 MiB
		maxPart   = 256 * 1024 * 1024 // 256 MiB
		maxComposeLimit = 32
	)

	if totalObjectSize <= 0 {
		return mediumPart // Default to 32 MiB if size unknown
	}

	// Calculate part size needed to stay within a single Compose step (<= 32 parts)
	targetPartSize := totalObjectSize / maxComposeLimit
	switch {
	case targetPartSize <= int64(minPart):
		return minPart
	case targetPartSize <= int64(mediumPart):
		return mediumPart
	case targetPartSize <= int64(largePart):
		return largePart
	case targetPartSize <= int64(xlargePart):
		return xlargePart
	default:
		return maxPart
	}
}
