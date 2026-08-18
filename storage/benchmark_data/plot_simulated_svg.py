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
import csv

def generate_svg_charts():
    csv_file = "benchmark_data/simulated_io_comprehensive_metrics.csv"
    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        return

    workloads = []
    without_tp = []
    with_tp = []
    speedups = []
    without_cpu = []
    with_cpu = []

    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Group by workload
    for i in range(0, len(rows), 2):
        r1 = rows[i]
        r2 = rows[i+1]
        name = r1["Workload"]
        w_tp = float(r1["ThroughputMBps"])
        a_tp = float(r2["ThroughputMBps"])
        w_dur = float(r1["DurationSec"])
        a_dur = float(r2["DurationSec"])
        speedup = w_dur / a_dur if a_dur > 0 else 1.0

        workloads.append(name)
        without_tp.append(w_tp)
        with_tp.append(a_tp)
        speedups.append(speedup)
        without_cpu.append(float(r1["TotalCPUMs"]))
        with_cpu.append(float(r2["TotalCPUMs"]))

    # 1. Throughput & Speedup SVG
    svg1 = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 950 500" width="100%" height="100%">
  <rect width="100%" height="100%" fill="#1e1e2e" rx="12" />
  <text x="475" y="40" fill="#cdd6f4" font-size="20" font-weight="bold" text-anchor="middle" font-family="system-ui, -apple-system, sans-serif">
    High-Fidelity Simulated I/O Benchmark: Throughput (MB/s) &amp; Speedup
  </text>
  <text x="475" y="65" fill="#a6adc8" font-size="13" text-anchor="middle" font-family="system-ui, -apple-system, sans-serif">
    Comparing 'Without Smart Tuning' (Baseline) vs 'With Dynamic AI Auto-Tuning'
  </text>

  <!-- Legend -->
  <rect x="250" y="85" width="16" height="16" fill="#f38ba8" rx="3" />
  <text x="275" y="98" fill="#cdd6f4" font-size="12" font-family="system-ui, -apple-system, sans-serif">Without Smart Tuning (Static)</text>
  <rect x="530" y="85" width="16" height="16" fill="#a6e3a1" rx="3" />
  <text x="555" y="98" fill="#cdd6f4" font-size="12" font-family="system-ui, -apple-system, sans-serif">With Dynamic AI Auto-Tuning</text>
"""

    y_offset = 140
    for idx, name in enumerate(workloads):
        wtp = without_tp[idx]
        atp = with_tp[idx]
        spd = speedups[idx]

        # Normalization scale for bar width (max 800 MB/s -> 450px)
        max_val = 800.0
        w_w = max(4, int((wtp / max_val) * 450))
        a_w = max(4, int((atp / max_val) * 450))

        svg1 += f"""
  <!-- {name} -->
  <text x="220" y="{y_offset + 18}" fill="#cdd6f4" font-size="13" font-weight="600" text-anchor="end" font-family="system-ui, -apple-system, sans-serif">{name}</text>
  
  <!-- Without bar -->
  <rect x="230" y="{y_offset}" width="{w_w}" height="14" fill="#f38ba8" rx="3" />
  <text x="{235 + w_w}" y="{y_offset + 11}" fill="#f38ba8" font-size="11" font-weight="bold" font-family="monospace">{wtp:.1f} MB/s</text>

  <!-- With bar -->
  <rect x="230" y="{y_offset + 18}" width="{a_w}" height="14" fill="#a6e3a1" rx="3" />
  <text x="{235 + a_w}" y="{y_offset + 29}" fill="#a6e3a1" font-size="11" font-weight="bold" font-family="monospace">{atp:.1f} MB/s</text>

  <!-- Speedup badge -->
  <rect x="800" y="{y_offset + 4}" width="110" height="24" fill="#313244" rx="6" stroke="#89b4fa" stroke-width="1.5" />
  <text x="855" y="{y_offset + 20}" fill="#89b4fa" font-size="12" font-weight="bold" text-anchor="middle" font-family="system-ui, -apple-system, sans-serif">{spd:.2f}x Speedup</text>
"""
        y_offset += 68

    svg1 += "</svg>"

    out_svg_path = "benchmark_data/plots/simulated_io_throughput_speedup.svg"
    os.makedirs(os.path.dirname(out_svg_path), exist_ok=True)
    with open(out_svg_path, "w") as f:
        f.write(svg1)
    print(f"Generated SVG: {out_svg_path}")

if __name__ == "__main__":
    generate_svg_charts()
