#!/bin/bash
# Round: rustthin with single-shot parity fix (poc3), rustthin-only + native-h1 parallel baseline.
set -x
BUCKET=cpriti-thickthin-poc-asse1
OUT=/var/log/bench5
mkdir -p $OUT
exec > >(tee -a $OUT/startup.log) 2>&1
stage() { echo "=== STAGE: $1 $(date -u) ==="; echo "$1" > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm5/STAGE || true; }
export HOME=/root PATH=/root/.cargo/bin:/usr/local/go/bin:$PATH GOFLAGS=-mod=mod GOPATH=/root/go GOCACHE=/root/.cache/go-build
stage "fetch-build"
mkdir -p /opt/poc3 && cd /opt/poc3 && gsutil -q cp gs://$BUCKET/poc/poc3.tgz /tmp/poc3.tgz && tar -xzf /tmp/poc3.tgz
cd /opt/poc3/gcs_core && /usr/bin/time -f "rust build: wall=%e s" cargo build --release 2>&1 | tail -2
cd /opt/poc3/gothin && mkdir -p bin && go build -o bin/bench2_rust ./cmd/bench2_rust && CGO_ENABLED=0 go build -o bin/bench2_native ./cmd/bench2_native && ls -la bin
./bin/bench2_rust check 2>&1 | tee $OUT/check.txt
run() { local name=$1; shift; echo "##### $name $(date -u)"; /usr/bin/time -f "rss_max_kb=%M user=%U sys=%S wall=%e" "$@" 2>&1 | tee $OUT/$name.txt; gsutil -q cp $OUT/$name.txt gs://$BUCKET/results/vm5/ || true; }
SIZES=1MiB,16MiB,128MiB,512MiB; ITERS=10
for round in 1 2; do
  stage "seq-round$round"
  run seq_native_h2_r$round   ./bin/bench2_native -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_native_h1_r$round   env NATIVE_HTTP1=1 ./bin/bench2_native -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_rustthin_r$round    ./bin/bench2_rust -sizes $SIZES -iters $ITERS -list-iters 10
done
stage "parallel"
threads() { local name=$1; shift; "$@" > $OUT/$name.txt 2>&1 & local pid=$!; local max=0
  while kill -0 $pid 2>/dev/null; do local t=$(ls /proc/$pid/task 2>/dev/null | wc -l); if [ "$t" -gt "$max" ]; then max=$t; cat /proc/$pid/task/*/comm 2>/dev/null | sort | uniq -c | sort -rn > $OUT/$name.threads.txt; fi; sleep 0.2; done
  wait $pid; echo "max_os_threads=$max" | tee -a $OUT/$name.txt; cat $OUT/$name.txt; gsutil -q cp $OUT/$name.txt $OUT/$name.threads.txt gs://$BUCKET/results/vm5/ || true; }
for par in 16 64 256; do
  threads par${par}_native_h1  env NATIVE_HTTP1=1 ./bin/bench2_native -sizes 16MiB -iters 256 -parallel $par -list-iters 0
  threads par${par}_rustthin   ./bin/bench2_rust -sizes 16MiB -iters 256 -parallel $par -list-iters 0
done
stage "upload"
gsutil -q cp -r $OUT gs://$BUCKET/results/vm5/
echo DONE > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm5/STAGE
