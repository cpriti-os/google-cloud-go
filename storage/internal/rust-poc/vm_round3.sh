#!/bin/bash
# Round 3: outlier re-run (128/512 MiB, 15 iters, all 4 backends) + thread-name breakdown at par=256.
set -x
BUCKET=cpriti-thickthin-poc-asse1
OUT=/var/log/bench3
mkdir -p $OUT
exec > >(tee -a $OUT/startup.log) 2>&1
stage() { echo "=== STAGE: $1 $(date -u) ==="; echo "$1" > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm3/STAGE || true; }
export HOME=/root PATH=/root/.cargo/bin:/usr/local/go/bin:$PATH
cd /opt/poc/gothin || { stage "FAILED-no-src"; exit 1; }
run() { local name=$1; shift; echo "##### $name $(date -u)"; /usr/bin/time -f "rss_max_kb=%M user=%U sys=%S wall=%e" "$@" 2>&1 | tee $OUT/$name.txt; gsutil -q cp $OUT/$name.txt gs://$BUCKET/results/vm3/ || true; }

stage "seq-round3"
SIZES=128MiB,512MiB; ITERS=15
run seq_native_h2_r3   ./bin/bench_native -sizes $SIZES -iters $ITERS -list-iters 10
run seq_native_h1_r3   env NATIVE_HTTP1=1 ./bin/bench_native -sizes $SIZES -iters $ITERS -list-iters 10
run seq_rust_block_r3  ./bin/bench_rust -sizes $SIZES -iters $ITERS -list-iters 10
run seq_rust_async_r3  env RUST_ASYNC=1 ./bin/bench_rust -sizes $SIZES -iters $ITERS -list-iters 10

stage "threadnames"
threadnames() { # backend env... ; samples thread names at peak
  local name=$1; shift
  "$@" ./bin/bench_rust -sizes 16MiB -iters 512 -parallel 256 -list-iters 0 > $OUT/$name.run.txt 2>&1 &
  local pid=$!; local max=0
  while kill -0 $pid 2>/dev/null; do
    local t=$(ls /proc/$pid/task 2>/dev/null | wc -l)
    if [ "$t" -gt "$max" ]; then max=$t; cat /proc/$pid/task/*/comm 2>/dev/null | sort | uniq -c | sort -rn > $OUT/$name.threads.txt; fi
    sleep 0.2
  done
  wait $pid
  echo "max_os_threads=$max" >> $OUT/$name.threads.txt
  cat $OUT/$name.threads.txt; gsutil -q cp $OUT/$name.threads.txt gs://$BUCKET/results/vm3/ || true
}
threadnames par256_rust_block_threads env
threadnames par256_rust_async_threads env RUST_ASYNC=1
# native for comparison
( ./bin/bench_native -sizes 16MiB -iters 512 -parallel 256 -list-iters 0 > $OUT/par256_native_h1.run.txt 2>&1 & pid=$!; max=0; while kill -0 $pid 2>/dev/null; do t=$(ls /proc/$pid/task 2>/dev/null | wc -l); if [ "$t" -gt "$max" ]; then max=$t; cat /proc/$pid/task/*/comm | sort | uniq -c | sort -rn > $OUT/par256_native_threads.txt; fi; sleep 0.2; done; echo "max_os_threads=$max" >> $OUT/par256_native_threads.txt; cat $OUT/par256_native_threads.txt )

stage "upload"
gsutil -q cp -r $OUT gs://$BUCKET/results/vm3/
echo DONE > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm3/STAGE
