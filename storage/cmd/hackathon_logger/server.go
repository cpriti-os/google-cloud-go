package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/overlap.csv", func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open("/usr/local/google/home/cpriti/.gemini/jetski/brain/9d3761a8-b7d0-4249-bcb9-c4ed239286b8/10_Minute_Overlap_Data.csv")
		if err == nil {
			defer f.Close()
			w.Header().Set("Content-Type", "text/csv")
			io.Copy(w, f)
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<!DOCTYPE html>
<html><head><title>Overlap Telemetry Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.tailwindcss.com"></script>
<style> body { background: #0f172a; color: white; padding: 2rem; font-family: sans-serif; } </style>
</head><body>
<h1 class="text-3xl font-bold text-center mb-8">⏱️ 10-Minute Perfect Execution Overlap</h1>
<div class="grid grid-cols-1 md:grid-cols-2 gap-8">
  <div class="bg-slate-800 p-4 rounded-xl border border-slate-700 shadow-xl col-span-1 md:col-span-2"><canvas id="speedChart" height="80"></canvas></div>
  <div class="bg-slate-800 p-4 rounded-xl border border-slate-700 shadow-xl"><canvas id="workerChart"></canvas></div>
  <div class="bg-slate-800 p-4 rounded-xl border border-slate-700 shadow-xl"><canvas id="cpuChart"></canvas></div>
</div>
<script>
let charts = {};
function makeChart(id, title, labels) {
	let ctx = document.getElementById(id).getContext('2d');
	let colors = ['#64748b', '#ef4444', '#10b981']; // Gray, Red, Green
	let datasets = labels.map((l, i) => ({
		label: l, data: [], borderColor: colors[i], tension: 0.1, fill: false, borderWidth: l.includes('Target')? 1: 3
	}));
	charts[id] = new Chart(ctx, {
		type: 'line', data: {labels: [], datasets: datasets},
		options: { responsive: true, animation: {duration: 0}, plugins: {title: {display: true, color: 'white', font:{size:20}, text: title}}, scales: {x:{grid:{color:'#334155'}},y:{grid:{color:'#334155'}, beginAtZero:true}} }
	});
}
makeChart('speedChart', 'A/B Test Parallel Timelines', ['Target Traffic Demanded (MB/s)', 'Baseline SDK Uploaded (MB/s)', 'Tuned AI PCU Uploaded (MB/s)']);
makeChart('workerChart', 'AI Orchestration Topology', ['Tuned PCU Workers', 'Chunk Scale Size (MB)']);

async function update() {
	let res = await fetch('/overlap.csv');
	let txt = await res.text();
	let lines = txt.trim().split('\n').slice(1);
	
	let t=[], tag=[], base=[], tuned=[], w=[], csz=[];
	lines.forEach(l => {
		let p = l.split(','); if(p.length < 5) return;
		t.push(p[0]+'s'); tag.push(p[1]); base.push(p[2]); tuned.push(p[3]); w.push(p[4]); csz.push(p[5]);
	});

	let updateC = (id, tx, ...d) => {
		if(!charts[id]) return;
		charts[id].data.labels = tx;
		d.forEach((data, i) => charts[id].data.datasets[i].data = data);
		charts[id].update();
	};

	updateC('speedChart', t, tag, base, tuned);
	updateC('workerChart', t, w, csz);
}
setInterval(update, 1000); update();
</script></body></html>`))
	})
	
	fmt.Println("Hosting telemetry overlap charts on :8081")
	http.ListenAndServe(":8081", nil)
}
