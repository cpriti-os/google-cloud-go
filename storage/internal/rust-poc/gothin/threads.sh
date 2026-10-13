#!/bin/bash
# Runs a backend with N parallel workers and samples OS thread count.
b=$1; par=$2; shift 2
./bin/bench_$b -sizes 16MiB -iters 256 -parallel $par -list-iters 0 "$@" &
pid=$!
max=0
while kill -0 $pid 2>/dev/null; do
  t=$(ls /proc/$pid/task 2>/dev/null | wc -l)
  [ "$t" -gt "$max" ] && max=$t
  sleep 0.2
done
wait $pid
echo "backend=$b parallel=$par max_os_threads=$max"
