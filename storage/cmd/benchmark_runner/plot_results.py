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

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def generate_all_plots(csv_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print(f"Reading benchmark data from {csv_path}...")
    df = pd.read_csv(csv_path)

    # Set styling
    plt.style.use('seaborn-v0_8-whitegrid')
    sns.set_context("talk")
    palette = {"Static-Fixed16MB": "#E74C3C", "Smart-DynamicChunk": "#2ECC71",
               "Static-NoPrefetch": "#E74C3C", "Smart-Prefetch-Depth1": "#3498DB", "Smart-Prefetch-Depth2": "#2ECC71",
               "Static-OnDemand": "#E74C3C", "Smart-RangePrefetch": "#2ECC71",
               "Unbounded-Alloc": "#E74C3C", "MemoryGuardrail-SlabPool": "#2ECC71"}

    # -------------------------------------------------------------------------
    # Plot 1: Orbax AI Checkpoint Barrier & Tail Latency (P50, P99, PMax)
    # -------------------------------------------------------------------------
    orbax_df = df[df['workload'] == 'Orbax-Checkpoint-Write']
    if not orbax_df.empty:
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))

        # 1A: Barrier Completion Time vs Object Size (32 nodes)
        nodes_32 = orbax_df[orbax_df['concurrency'] == 32]
        if not nodes_32.empty:
            sns.barplot(data=nodes_32, x='object_size_mb', y='barrier_time_sec', hue='mode',
                        palette=palette, ax=axes[0], errorbar=None)
            axes[0].set_title("Orbax 32-Node Barrier Completion Time", fontsize=16, weight='bold')
            axes[0].set_xlabel("Object Size per Node (MB)", fontsize=14)
            axes[0].set_ylabel("Total Barrier Time (seconds - lower is better)", fontsize=14)
            axes[0].legend(title="SDK Mode")

        # 1B: Tail Latency Reduction (P50 vs P99 vs PMax for 512MB objects)
        obj_512 = orbax_df[orbax_df['object_size_mb'] == 512]
        if not obj_512.empty:
            melted = pd.melt(obj_512, id_vars=['mode', 'concurrency'], 
                             value_vars=['p50_sec', 'p99_sec', 'p_max_sec'],
                             var_name='percentile', value_name='latency_sec')
            melted['percentile'] = melted['percentile'].map({'p50_sec': 'P50', 'p99_sec': 'P99', 'p_max_sec': 'PMax (Straggler)'})
            sns.barplot(data=melted, x='percentile', y='latency_sec', hue='mode',
                        palette=palette, ax=axes[1], errorbar=None)
            axes[1].set_title("Tail Latency & Straggler Impact (512 MB Objects)", fontsize=16, weight='bold')
            axes[1].set_xlabel("Latency Percentile", fontsize=14)
            axes[1].set_ylabel("Latency (seconds - lower is better)", fontsize=14)
            axes[1].legend(title="SDK Mode")

        plt.tight_layout()
        p1_path = os.path.join(output_dir, "orbax_checkpoint_tail_latency.png")
        plt.savefig(p1_path, dpi=300)
        plt.close()
        print(f"Saved: {p1_path}")

    # -------------------------------------------------------------------------
    # Plot 2: AI/ML Dataloader: Throughput & GPU Starvation
    # -------------------------------------------------------------------------
    dl_df = df[df['workload'] == 'DataLoader-Streaming']
    if not dl_df.empty:
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))

        # 2A: Throughput across batch sizes
        sns.barplot(data=dl_df, x='object_size_mb', y='throughput_mb_ps', hue='mode',
                    palette=palette, ax=axes[0], errorbar=None)
        axes[0].set_title("AI/ML Dataloader Streaming Throughput", fontsize=16, weight='bold')
        axes[0].set_xlabel("Batch Shard Size (MB)", fontsize=14)
        axes[0].set_ylabel("Effective Throughput (MB/s - higher is better)", fontsize=14)
        axes[0].legend(title="SDK Mode")

        # 2B: GPU Compute Starvation / Wait Time Percentage
        sns.barplot(data=dl_df, x='object_size_mb', y='compute_wait_pct', hue='mode',
                    palette=palette, ax=axes[1], errorbar=None)
        axes[1].set_title("GPU Compute Wait Time / I/O Starvation (%)", fontsize=16, weight='bold')
        axes[1].set_xlabel("Batch Shard Size (MB)", fontsize=14)
        axes[1].set_ylabel("Time Compute Was Idle Waiting for I/O (%)", fontsize=14)
        axes[1].legend(title="SDK Mode")

        plt.tight_layout()
        p2_path = os.path.join(output_dir, "ai_dataloader_throughput_starvation.png")
        plt.savefig(p2_path, dpi=300)
        plt.close()
        print(f"Saved: {p2_path}")

    # -------------------------------------------------------------------------
    # Plot 3: Memory Guardrail & GC Footprint
    # -------------------------------------------------------------------------
    mem_df = df[df['workload'] == 'Memory-Guardrail-Soak']
    if not mem_df.empty:
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))

        # 3A: Peak System RAM Allocated
        sns.barplot(data=mem_df, x='mode', y='peak_ram_mb', palette=palette, ax=axes[0], errorbar=None)
        axes[0].set_title("Peak Process Memory (RAM) Under 100 Workers", fontsize=16, weight='bold')
        axes[0].set_ylabel("Peak RAM (MB - lower is better)", fontsize=14)
        axes[0].set_xlabel("Memory Strategy", fontsize=14)

        # 3B: Total Go Heap Allocations & GC Count
        sns.barplot(data=mem_df, x='mode', y='total_alloc_mb', palette=palette, ax=axes[1], errorbar=None)
        axes[1].set_title("Total Go Heap Allocations (GC Pressure)", fontsize=16, weight='bold')
        axes[1].set_ylabel("Cumulative Heap Allocated (MB - lower is better)", fontsize=14)
        axes[1].set_xlabel("Memory Strategy", fontsize=14)

        plt.tight_layout()
        p3_path = os.path.join(output_dir, "memory_guardrail_gc_footprint.png")
        plt.savefig(p3_path, dpi=300)
        plt.close()
        print(f"Saved: {p3_path}")

    # -------------------------------------------------------------------------
    # Plot 4: Overall Speedup & Efficiency Summary
    # -------------------------------------------------------------------------
    summary_data = []
    # 1. Checkpoint speedup
    if not orbax_df.empty:
        stat_bar = orbax_df[orbax_df['mode'] == 'Static-Fixed16MB']['barrier_time_sec'].mean()
        smart_bar = orbax_df[orbax_df['mode'] == 'Smart-DynamicChunk']['barrier_time_sec'].mean()
        if smart_bar > 0:
            summary_data.append({'Metric': 'Orbax Checkpoint Barrier Time', 'Speedup': stat_bar / smart_bar, 'Category': 'Latency Speedup'})

    # 2. Dataloader throughput gain
    if not dl_df.empty:
        stat_tp = dl_df[dl_df['mode'] == 'Static-NoPrefetch']['throughput_mb_ps'].mean()
        smart_tp = dl_df[dl_df['mode'] == 'Smart-Prefetch-Depth2']['throughput_mb_ps'].mean()
        if stat_tp > 0:
            summary_data.append({'Metric': 'Dataloader Streaming Throughput', 'Speedup': smart_tp / stat_tp, 'Category': 'Throughput Gain'})

    # 3. Columnar scan speedup
    col_df = df[df['workload'] == 'Columnar-Parquet-Scan']
    if not col_df.empty:
        stat_col = col_df[col_df['mode'] == 'Static-OnDemand']['duration_sec'].mean()
        smart_col = col_df[col_df['mode'] == 'Smart-RangePrefetch']['duration_sec'].mean()
        if smart_col > 0:
            summary_data.append({'Metric': 'Columnar Parquet Scan Duration', 'Speedup': stat_col / smart_col, 'Category': 'Latency Speedup'})

    # 4. RAM reduction
    if not mem_df.empty:
        unb_ram = mem_df[mem_df['mode'] == 'Unbounded-Alloc']['peak_ram_mb'].mean()
        grd_ram = mem_df[mem_df['mode'] == 'MemoryGuardrail-SlabPool']['peak_ram_mb'].mean()
        if grd_ram > 0:
            summary_data.append({'Metric': 'Memory Efficiency (RAM Density)', 'Speedup': unb_ram / grd_ram, 'Category': 'Memory Savings'})

    if summary_data:
        sum_df = pd.DataFrame(summary_data)
        plt.figure(figsize=(12, 6))
        ax = sns.barplot(data=sum_df, x='Metric', y='Speedup', palette=['#2ECC71', '#3498DB', '#9B59B6', '#F39C12'])
        plt.axhline(1.0, color='gray', linestyle='--', linewidth=1.5, label='Baseline (1.0x)')
        plt.title("GCS SDK Auto-Tuning Summary: Multiplier Improvements vs Baseline", fontsize=16, weight='bold')
        plt.ylabel("Improvement Multiplier (x - higher is better)", fontsize=14)
        plt.xticks(rotation=15, ha='right', fontsize=12)
        for p in ax.patches:
            height = p.get_height()
            if not np.isnan(height):
                ax.annotate(f"{height:.2f}x",
                            (p.get_x() + p.get_width() / 2., height),
                            ha='center', va='bottom', fontsize=12, weight='bold',
                            xytext=(0, 5), textcoords='offset points')
        plt.tight_layout()
        p4_path = os.path.join(output_dir, "overall_speedup_summary.png")
        plt.savefig(p4_path, dpi=300)
        plt.close()
        print(f"Saved: {p4_path}")

    print("\nAll visualization plots generated successfully in:", output_dir)

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data/benchmark_detailed_runs.csv"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data/plots"
    generate_all_plots(csv_file, out_dir)
