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
MOUNT_DIR="/usr/local/google/home/cpriti/gcs_bench_mount"
OUT_DIR="/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/fio_benchmarks/results"

mkdir -p "${MOUNT_DIR}" "${OUT_DIR}"

echo "================================================================================"
echo "Running LIVE FIO Workload Benchmark Suite against Google Cloud Storage (Prod)"
echo "Bucket Mount: ${MOUNT_DIR}"
echo "Binary: ${FIO_BIN} ($(${FIO_BIN} --version))"
echo "Output Directory: ${OUT_DIR}"
echo "================================================================================"

PROFILES=("orbax_checkpoint_seq_write" "ai_dataloader_seq_read" "columnar_parquet_scan" "small_file_high_qps")

for PROFILE in "${PROFILES[@]}"; do
    echo ""
    echo "--------------------------------------------------------------------------------"
    echo "Executing Live GCS FIO Profile: [${PROFILE}]"
    echo "--------------------------------------------------------------------------------"
    
    ${FIO_BIN} storage/fio_benchmarks/gcs_workloads.fio \
        --section="${PROFILE}" \
        --output-format=json \
        --output="${OUT_DIR}/${PROFILE}.json"
        
    echo "Finished ${PROFILE}, output saved to ${OUT_DIR}/${PROFILE}.json"
done

echo ""
echo "Cleaning up benchmark files in GCS bucket mount (${MOUNT_DIR})..."
rm -f "${MOUNT_DIR}"/fio_live_gcs_*.dat || true

echo "================================================================================"
echo "Parsing Live GCS FIO Results and Generating Report..."
echo "================================================================================"
/tmp/bench_env/bin/python3 storage/fio_benchmarks/parse_fio_results.py "${OUT_DIR}"

echo "Live GCS FIO benchmark suite completed successfully!"
