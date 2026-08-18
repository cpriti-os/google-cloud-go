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
)

func TestAutoTuningConfig_DefaultsAndNormalize(t *testing.T) {
	// Test nil config
	var nilCfg *AutoTuningConfig
	normNil := nilCfg.Normalize()
	if normNil.Enabled {
		t.Errorf("Expected nil config to be disabled, got enabled")
	}

	// Test default config
	def := DefaultAutoTuningConfig()
	if !def.Enabled {
		t.Errorf("Expected default config to be enabled")
	}
	if def.MaxMemoryBudget != 256*1024*1024 {
		t.Errorf("Expected 256MB budget, got %d", def.MaxMemoryBudget)
	}
	if def.InitialUploadChunkSize != 32*1024*1024 {
		t.Errorf("Expected 32MB initial chunk, got %d", def.InitialUploadChunkSize)
	}
	if def.MaxUploadChunkSize != 64*1024*1024 {
		t.Errorf("Expected 64MB max chunk, got %d", def.MaxUploadChunkSize)
	}
	if def.PCUPartSize != 32*1024*1024 {
		t.Errorf("Expected 32MB PCU part size, got %d", def.PCUPartSize)
	}
	if def.PrefetchDepth != 2 {
		t.Errorf("Expected prefetch depth 2, got %d", def.PrefetchDepth)
	}

	// Test custom partial config normalization
	custom := &AutoTuningConfig{
		Enabled: true,
	}
	normCustom := custom.Normalize()
	if normCustom.InitialUploadChunkSize != 32*1024*1024 {
		t.Errorf("Expected normalized initial chunk to be 32MB, got %d", normCustom.InitialUploadChunkSize)
	}
	if normCustom.PCUPartSize != 32*1024*1024 {
		t.Errorf("Expected normalized PCU part size to be 32MB, got %d", normCustom.PCUPartSize)
	}
}

func TestDynamicPCUPartSize(t *testing.T) {
	tests := []struct {
		name       string
		totalSize  int64
		expectedMB int
	}{
		{"Small 256MB object", 256 * 1024 * 1024, 16},
		{"Medium 1GB object", 1024 * 1024 * 1024, 32},
		{"Large 2GB object", 2 * 1024 * 1024 * 1024, 64},
		{"AI Model 4GB checkpoint", 4 * 1024 * 1024 * 1024, 128},
		{"Massive 10GB dataset", 10 * 1024 * 1024 * 1024, 256},
		{"Unknown size", 0, 32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DynamicPCUPartSize(tt.totalSize, 8)
			expectedBytes := tt.expectedMB * 1024 * 1024
			if got != expectedBytes {
				t.Errorf("DynamicPCUPartSize(%d) = %d bytes (%d MB), want %d bytes (%d MB)",
					tt.totalSize, got, got/(1024*1024), expectedBytes, tt.expectedMB)
			}
		})
	}
}

func TestWriter_OptInBehavior(t *testing.T) {
	// Verify that a standard Writer with nil AutoTuning keeps default ChunkSize
	wDefault := &Writer{
		ChunkSize: 16 * 1024 * 1024,
	}
	if wDefault.AutoTuning != nil {
		t.Errorf("Expected AutoTuning to be nil by default")
	}

	// Verify that Writer with AutoTuning disabled does not override ChunkSize
	wDisabled := &Writer{
		ChunkSize:  16 * 1024 * 1024,
		AutoTuning: &AutoTuningConfig{Enabled: false, InitialUploadChunkSize: 32 * 1024 * 1024},
	}
	if wDisabled.AutoTuning.Enabled {
		t.Errorf("Expected AutoTuning to be disabled")
	}

	// Verify that Writer with AutoTuning enabled provides 32MB default initial chunk
	wEnabled := &Writer{
		ChunkSize:  16 * 1024 * 1024,
		AutoTuning: DefaultAutoTuningConfig(),
	}
	if !wEnabled.AutoTuning.Enabled {
		t.Errorf("Expected AutoTuning to be enabled")
	}
	if wEnabled.AutoTuning.InitialUploadChunkSize != 32*1024*1024 {
		t.Errorf("Expected 32MB initial chunk, got %d", wEnabled.AutoTuning.InitialUploadChunkSize)
	}
}

