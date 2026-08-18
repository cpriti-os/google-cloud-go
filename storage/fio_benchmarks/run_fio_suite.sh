#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

FIO_BIN="/tmp/fio_bin/bin/fio"
WORK_DIR="/tmp/fio_bench_dir"
OUT_DIR="/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/fio_benchmarks/results"

mkdir -p "${WORK_DIR}" "${OUT_DIR}"

echo "================================================================================"
echo "Running Comprehensive FIO Workload Benchmark Suite for GCS & GCSFuse"
echo "Binary: ${FIO_BIN} ($(${FIO_BIN} --version))"
echo "Output Directory: ${OUT_DIR}"
echo "================================================================================"

PROFILES=("ai_dataloader_seq_read" "orbax_checkpoint_seq_write" "columnar_parquet_scan" "small_file_high_qps")

for PROFILE in "${PROFILES[@]}"; do
    echo ""
    echo "--------------------------------------------------------------------------------"
    echo "Executing FIO Profile: [${PROFILE}]"
    echo "--------------------------------------------------------------------------------"
    
    # Run FIO with JSON+ output format
    ${FIO_BIN} storage/fio_benchmarks/gcs_workloads.fio \
        --section="${PROFILE}" \
        --output-format=json \
        --output="${OUT_DIR}/${PROFILE}.json"
        
    echo "Finished ${PROFILE}, output saved to ${OUT_DIR}/${PROFILE}.json"
done

echo ""
echo "Cleaning up scratch files in ${WORK_DIR}..."
rm -rf "${WORK_DIR:?}"/*

echo "================================================================================"
echo "Parsing FIO Results and Generating Report..."
echo "================================================================================"
/tmp/bench_env/bin/python3 storage/fio_benchmarks/parse_fio_results.py "${OUT_DIR}"

echo "FIO benchmark suite completed successfully!"
