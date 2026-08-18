#!/usr/bin/env python3
import csv
import os

csv_path = '/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data/real_gcs_live_metrics.csv'
out_dir = '/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data/plots'
artifact_dir = '/usr/local/google/home/cpriti/.gemini/jetski/brain/9d3761a8-b7d0-4249-bcb9-c4ed239286b8'

rows = []
with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

upload_rows = [r for r in rows if r['Workload'] == 'Orbax-AI-Checkpoint-Upload']

svg = '''<svg width="850" height="420" xmlns="http://www.w3.org/2000/svg" style="background:#ffffff; font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;">
  <style>
    .title { font-size: 16px; font-weight: bold; fill: #1f2937; }
    .subtitle { font-size: 12px; fill: #6b7280; }
    .axis { stroke: #d1d5db; stroke-width: 1; }
    .label { font-size: 12px; fill: #4b5563; }
    .val { font-size: 11px; font-weight: bold; }
    .legend { font-size: 12px; fill: #374151; }
  </style>
  
  <text x="20" y="30" class="title">Real GCS Prod CPU Time &amp; Heap Memory Allocation</text>
  <text x="20" y="48" class="subtitle">Direct measurements via getrusage (User+Sys CPU) &amp; Go runtime.MemStats during live uploads to GCS</text>
  
  <!-- Left Panel: CPU Time (ms) -->
  <text x="50" y="85" style="font-size: 14px; font-weight: bold; fill: #111827;">Total CPU Time (ms) [User + Sys CPU]</text>
  <line x1="50" y1="320" x2="400" y2="320" class="axis" />
  <line x1="50" y1="100" x2="50" y2="320" class="axis" />
'''

categories = ['256 MB (4x64)', '512 MB (4x128)', '1024 MB (4x256)']
payload_sizes = [256, 512, 1024]

# Scale CPU: max ~8000ms -> height 200px (1000ms = 25px)
for i, size in enumerate(payload_sizes):
    sub = [r for r in upload_rows if int(r['TotalMB']) == size]
    static_r = next((r for r in sub if 'Static' in r['Mode']), None)
    smart_r = next((r for r in sub if 'Smart' in r['Mode']), None)
    
    if static_r and smart_r:
        s_cpu = float(static_r['TotalCPUMs'])
        m_cpu = float(smart_r['TotalCPUMs'])
        
        bx = 70 + i * 110
        # Static bar
        h1 = (s_cpu / 1000.0) * 25.0
        y1 = 320 - h1
        svg += f'  <rect x="{bx}" y="{y1:.1f}" width="40" height="{h1:.1f}" fill="#f59e0b" rx="3" />\n'
        svg += f'  <text x="{bx+20}" y="{y1-5:.1f}" text-anchor="middle" class="val" fill="#b45309">{s_cpu:.0f}ms</text>\n'
        
        # Smart bar
        h2 = (m_cpu / 1000.0) * 25.0
        y2 = 320 - h2
        svg += f'  <rect x="{bx+44}" y="{y2:.1f}" width="40" height="{h2:.1f}" fill="#10b981" rx="3" />\n'
        svg += f'  <text x="{bx+64}" y="{y2-5:.1f}" text-anchor="middle" class="val" fill="#047857">{m_cpu:.0f}ms</text>\n'
        
        # Category label
        svg += f'  <text x="{bx+42}" y="340" text-anchor="middle" class="label">{categories[i]}</text>\n'

svg += '''
  <!-- Right Panel: Heap Memory (MB) -->
  <text x="470" y="85" style="font-size: 14px; font-weight: bold; fill: #111827;">Peak Heap Memory (MB) [Capped by Guardrail]</text>
  <line x1="470" y1="320" x2="820" y2="320" class="axis" />
  <line x1="470" y1="100" x2="470" y2="320" class="axis" />
'''

# Scale Heap: max ~150MB -> height 200px (1 MB = 1.33 px)
for i, size in enumerate(payload_sizes):
    sub = [r for r in upload_rows if int(r['TotalMB']) == size]
    static_r = next((r for r in sub if 'Static' in r['Mode']), None)
    smart_r = next((r for r in sub if 'Smart' in r['Mode']), None)
    
    if static_r and smart_r:
        s_mem = float(static_r['AllocHeapMB'])
        m_mem = float(smart_r['AllocHeapMB'])
        
        bx = 490 + i * 110
        # Static bar
        h1 = s_mem * 1.33
        y1 = 320 - h1
        svg += f'  <rect x="{bx}" y="{y1:.1f}" width="40" height="{h1:.1f}" fill="#6366f1" rx="3" />\n'
        svg += f'  <text x="{bx+20}" y="{y1-5:.1f}" text-anchor="middle" class="val" fill="#4338ca">{s_mem:.1f}M</text>\n'
        
        # Smart bar
        h2 = m_mem * 1.33
        y2 = 320 - h2
        svg += f'  <rect x="{bx+44}" y="{y2:.1f}" width="40" height="{h2:.1f}" fill="#06b6d4" rx="3" />\n'
        svg += f'  <text x="{bx+64}" y="{y2-5:.1f}" text-anchor="middle" class="val" fill="#0e7490">{m_mem:.1f}M</text>\n'
        
        # Category label
        svg += f'  <text x="{bx+42}" y="340" text-anchor="middle" class="label">{categories[i]}</text>\n'

# Legend
svg += '''
  <!-- Legend -->
  <g transform="translate(160, 385)">
    <rect x="0" y="0" width="16" height="16" fill="#f59e0b" rx="2" />
    <text x="22" y="13" class="legend">Static CPU Time</text>
    <rect x="150" y="0" width="16" height="16" fill="#10b981" rx="2" />
    <text x="172" y="13" class="legend">Smart Auto-Tuning CPU Time</text>
    <rect x="390" y="0" width="16" height="16" fill="#6366f1" rx="2" />
    <text x="412" y="13" class="legend">Static Heap</text>
    <rect x="520" y="0" width="16" height="16" fill="#06b6d4" rx="2" />
    <text x="542" y="13" class="legend">Smart Heap (Capped)</text>
  </g>
</svg>
'''

out_svg = os.path.join(out_dir, 'real_gcs_cpu_memory.svg')
art_svg = os.path.join(artifact_dir, 'real_gcs_cpu_memory.svg')
with open(out_svg, 'w') as f:
    f.write(svg)
with open(art_svg, 'w') as f:
    f.write(svg)

print("Real GCS CPU & Memory SVG charts generated successfully!")
