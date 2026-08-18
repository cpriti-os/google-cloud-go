#!/usr/bin/env python3
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

import json
import os
import glob
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_fio_directory(results_dir):
    json_files = glob.glob(os.path.join(results_dir, "*.json"))
    records = []

    for fpath in json_files:
        profile_name = os.path.splitext(os.path.basename(fpath))[0]
        try:
            with open(fpath, 'r') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {fpath}: {e}")
            continue

        for job in data.get('jobs', []):
            jobname = job.get('jobname', profile_name)
            
            # Read stats
            read_stat = job.get('read', {})
            read_iops = read_stat.get('iops', 0)
            read_bw_mb = read_stat.get('bw_bytes', 0) / (1024 * 1024)
            read_lat_p50_ms = read_stat.get('clat_ns', {}).get('percentile', {}).get('50.000000', 0) / 1e6
            read_lat_p99_ms = read_stat.get('clat_ns', {}).get('percentile', {}).get('99.000000', 0) / 1e6

            # Write stats
            write_stat = job.get('write', {})
            write_iops = write_stat.get('iops', 0)
            write_bw_mb = write_stat.get('bw_bytes', 0) / (1024 * 1024)
            write_lat_p50_ms = write_stat.get('clat_ns', {}).get('percentile', {}).get('50.000000', 0) / 1e6
            write_lat_p99_ms = write_stat.get('clat_ns', {}).get('percentile', {}).get('99.000000', 0) / 1e6

            total_iops = read_iops + write_iops
            total_bw_mb = read_bw_mb + write_bw_mb

            records.append({
                'Profile': profile_name,
                'IOPS': total_iops,
                'Throughput (MB/s)': total_bw_mb,
                'Read P99 (ms)': read_lat_p99_ms,
                'Write P99 (ms)': write_lat_p99_ms,
                'CPU Sys (%)': job.get('sys_cpu', 0),
                'CPU User (%)': job.get('usr_cpu', 0),
            })

    if not records:
        print("No FIO job records found.")
        return

    df = pd.DataFrame(records)
    print("\n+--------------------------------+------------+-------------------+---------------+----------------+")
    print("| FIO Workload Profile           | Total IOPS | Throughput (MB/s) | Read P99 (ms) | Write P99 (ms) |")
    print("+--------------------------------+------------+-------------------+---------------+----------------+")
    for _, row in df.iterrows():
        print(f"| {row['Profile']:<30} | {row['IOPS']:>10.1f} | {row['Throughput (MB/s)']:>17.2f} | {row['Read P99 (ms)']:>13.3f} | {row['Write P99 (ms)']:>14.3f} |")
    print("+--------------------------------+------------+-------------------+---------------+----------------+")

    # Generate Visualization
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Barplot of Throughput
    sns.barplot(data=df, x='Profile', y='Throughput (MB/s)', ax=axes[0], palette='crest')
    axes[0].set_title("FIO Workload Bandwidth Profile", fontsize=15, weight='bold')
    axes[0].set_xticklabels(axes[0].get_xticklabels(), rotation=20, ha='right')

    # Barplot of IOPS
    sns.barplot(data=df, x='Profile', y='IOPS', ax=axes[1], palette='flare')
    axes[1].set_title("FIO Workload IOPS Profile", fontsize=15, weight='bold')
    axes[1].set_xticklabels(axes[1].get_xticklabels(), rotation=20, ha='right')

    plt.tight_layout()
    plot_path = os.path.join(results_dir, "fio_workloads_benchmark.png")
    plt.savefig(plot_path, dpi=300)
    plt.close()
    print(f"\nFIO summary plot saved to: {plot_path}")

if __name__ == "__main__":
    res_dir = sys.argv[1] if len(sys.argv) > 1 else "/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/fio_benchmarks/results"
    parse_fio_directory(res_dir)
