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
	if def.MaxUploadChunkSize != 64*1024*1024 {
		t.Errorf("Expected 64MB max chunk, got %d", def.MaxUploadChunkSize)
	}
	if def.PrefetchDepth != 2 {
		t.Errorf("Expected prefetch depth 2, got %d", def.PrefetchDepth)
	}

	// Test custom partial config normalization
	custom := &AutoTuningConfig{
		Enabled: true,
	}
	normCustom := custom.Normalize()
	if normCustom.MaxMemoryBudget != 256*1024*1024 {
		t.Errorf("Expected normalized budget to be 256MB, got %d", normCustom.MaxMemoryBudget)
	}
	if normCustom.MaxUploadChunkSize != 64*1024*1024 {
		t.Errorf("Expected normalized max chunk to be 64MB, got %d", normCustom.MaxUploadChunkSize)
	}
	if normCustom.PrefetchDepth != 2 {
		t.Errorf("Expected normalized prefetch depth to be 2, got %d", normCustom.PrefetchDepth)
	}
}
