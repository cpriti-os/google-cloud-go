#!/usr/bin/env python3
import csv
import os

csv_path = '/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data/real_gcs_live_metrics.csv'
out_dir = '/usr/local/google/home/cpriti/GO_SDK_Repos/cpriti-google-cloud-go/storage/benchmark_data/plots'
artifact_dir = '/usr/local/google/home/cpriti/.gemini/jetski/brain/9d3761a8-b7d0-4249-bcb9-c4ed239286b8'

os.makedirs(out_dir, exist_ok=True)
os.makedirs(artifact_dir, exist_ok=True)

rows = []
with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# Filter upload rows
upload_rows = [r for r in rows if r['Workload'] == 'Orbax-AI-Checkpoint-Upload']

# Generate SVG Bar Chart for Real Upload Throughput & Duration
svg = '''<svg width="850" height="420" xmlns="http://www.w3.org/2000/svg" style="background:#ffffff; font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;">
  <style>
    .title { font-size: 16px; font-weight: bold; fill: #1f2937; }
    .subtitle { font-size: 12px; fill: #6b7280; }
    .axis { stroke: #d1d5db; stroke-width: 1; }
    .label { font-size: 12px; fill: #4b5563; }
    .val { font-size: 11px; font-weight: bold; }
    .legend { font-size: 12px; fill: #374151; }
  </style>
  
  <text x="20" y="30" class="title">Real GCS Prod Live Performance (Multi-Worker Uploads to gs://cpriti-sdk-autotune-bench)</text>
  <text x="20" y="48" class="subtitle">Direct Go SDK uploads over live network comparing Static 16MB Chunking vs Smart AIMD Auto-Tuning (32MB+)</text>
  
  <!-- Left Panel: Throughput (MB/s) -->
  <text x="50" y="85" style="font-size: 14px; font-weight: bold; fill: #111827;">Network Throughput (MB/s) - Higher is Better</text>
  <line x1="50" y1="320" x2="400" y2="320" class="axis" />
  <line x1="50" y1="100" x2="50" y2="320" class="axis" />
'''

# Draw bars for 256MB, 512MB, 1024MB
# Static vs Smart
# Scale: max throughput ~35 MB/s -> height 200px (1 MB/s = 5.7 px)
categories = ['256 MB (4x64)', '512 MB (4x128)', '1024 MB (4x256)']
payload_sizes = [256, 512, 1024]

for i, size in enumerate(payload_sizes):
    sub = [r for r in upload_rows if int(r['TotalMB']) == size]
    static_r = next((r for r in sub if 'Static' in r['Mode']), None)
    smart_r = next((r for r in sub if 'Smart' in r['Mode']), None)
    
    if static_r and smart_r:
        s_tp = float(static_r['ThroughputMBs'])
        m_tp = float(smart_r['ThroughputMBs'])
        
        bx = 70 + i * 110
        # Static bar
        h1 = s_tp * 5.7
        y1 = 320 - h1
        svg += f'  <rect x="{bx}" y="{y1:.1f}" width="40" height="{h1:.1f}" fill="#9ca3af" rx="3" />\n'
        svg += f'  <text x="{bx+20}" y="{y1-5:.1f}" text-anchor="middle" class="val" fill="#374151">{s_tp:.1f}</text>\n'
        
        # Smart bar
        h2 = m_tp * 5.7
        y2 = 320 - h2
        svg += f'  <rect x="{bx+44}" y="{y2:.1f}" width="40" height="{h2:.1f}" fill="#10b981" rx="3" />\n'
        svg += f'  <text x="{bx+64}" y="{y2-5:.1f}" text-anchor="middle" class="val" fill="#047857">{m_tp:.1f}</text>\n'
        
        # Category label
        svg += f'  <text x="{bx+42}" y="340" text-anchor="middle" class="label">{categories[i]}</text>\n'

svg += '''
  <!-- Right Panel: Duration (s) -->
  <text x="470" y="85" style="font-size: 14px; font-weight: bold; fill: #111827;">Transfer Duration (s) - Lower is Faster</text>
  <line x1="470" y1="320" x2="820" y2="320" class="axis" />
  <line x1="470" y1="100" x2="470" y2="320" class="axis" />
'''

# Scale: max duration ~70s -> height 200px (1s = 2.8 px)
for i, size in enumerate(payload_sizes):
    sub = [r for r in upload_rows if int(r['TotalMB']) == size]
    static_r = next((r for r in sub if 'Static' in r['Mode']), None)
    smart_r = next((r for r in sub if 'Smart' in r['Mode']), None)
    
    if static_r and smart_r:
        s_dur = float(static_r['DurationSec'])
        m_dur = float(smart_r['DurationSec'])
        speedup = s_dur / m_dur
        
        bx = 490 + i * 110
        # Static bar
        h1 = s_dur * 2.8
        y1 = 320 - h1
        svg += f'  <rect x="{bx}" y="{y1:.1f}" width="40" height="{h1:.1f}" fill="#ef4444" rx="3" />\n'
        svg += f'  <text x="{bx+20}" y="{y1-5:.1f}" text-anchor="middle" class="val" fill="#b91c1c">{s_dur:.1f}s</text>\n'
        
        # Smart bar
        h2 = m_dur * 2.8
        y2 = 320 - h2
        svg += f'  <rect x="{bx+44}" y="{y2:.1f}" width="40" height="{h2:.1f}" fill="#3b82f6" rx="3" />\n'
        svg += f'  <text x="{bx+64}" y="{y2-5:.1f}" text-anchor="middle" class="val" fill="#1d4ed8">{m_dur:.1f}s</text>\n'
        
        # Speedup pill
        svg += f'  <text x="{bx+42}" y="360" text-anchor="middle" style="font-size:11px; font-weight:bold; fill:#047857;">{speedup:.2f}x Faster</text>\n'
        
        # Category label
        svg += f'  <text x="{bx+42}" y="340" text-anchor="middle" class="label">{categories[i]}</text>\n'

# Legend
svg += '''
  <!-- Legend -->
  <g transform="translate(180, 385)">
    <rect x="0" y="0" width="16" height="16" fill="#9ca3af" rx="2" />
    <text x="22" y="13" class="legend">Static 16MB Default</text>
    <rect x="180" y="0" width="16" height="16" fill="#10b981" rx="2" />
    <text x="202" y="13" class="legend">Smart Auto-Tuning (32MB+)</text>
  </g>
</svg>
'''

out_svg = os.path.join(out_dir, 'real_gcs_upload_performance.svg')
art_svg = os.path.join(artifact_dir, 'real_gcs_upload_performance.svg')
with open(out_svg, 'w') as f:
    f.write(svg)
with open(art_svg, 'w') as f:
    f.write(svg)

print("Real GCS upload SVG charts generated successfully!")
