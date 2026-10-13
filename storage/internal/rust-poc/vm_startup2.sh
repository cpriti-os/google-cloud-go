#!/bin/bash
# VM startup script, run 2: 1:1 facade (rustthin) vs native storage, same workload code.
set -x
BUCKET=cpriti-thickthin-poc-asse1
OUT=/var/log/bench4
mkdir -p $OUT
exec > >(tee -a $OUT/startup.log) 2>&1
echo "=== START $(date -u) host=$(hostname) ==="
stage() { echo "=== STAGE: $1 $(date -u) ==="; echo "$1" > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm4/STAGE || true; }

stage "apt"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq build-essential cmake curl file procps time >/dev/null

stage "toolchains"
curl -sSL https://go.dev/dl/go1.26.6.linux-amd64.tar.gz -o /tmp/go.tgz || curl -sSL https://go.dev/dl/go1.25.3.linux-amd64.tar.gz -o /tmp/go.tgz
rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/go.tgz
export HOME=/root
curl -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain stable --no-modify-path
export PATH=/root/.cargo/bin:/usr/local/go/bin:$PATH
go version; cargo --version; ldd --version | head -1; nproc; free -g | head -2

stage "fetch-src"
mkdir -p /opt/poc && cd /opt/poc
gsutil -q cp gs://$BUCKET/poc/poc2.tgz /tmp/poc.tgz
tar -xzf /tmp/poc.tgz

stage "build-rust"
cd /opt/poc/gcs_core && rm -rf target
/usr/bin/time -f "rust build: wall=%e s user=%U sys=%S maxrss_kb=%M" cargo build --release 2>&1 | tail -3
ls -la target/release/libgcs_core.a

stage "build-go"
cd /opt/poc/gothin && rm -rf bin && mkdir bin
export GOFLAGS=-mod=mod GOPATH=/root/go GOCACHE=/root/.cache/go-build
/usr/bin/time -f "go native build: wall=%e s" env CGO_ENABLED=0 go build -o bin/bench2_native ./cmd/bench2_native
/usr/bin/time -f "go cgo build: wall=%e s" go build -o bin/bench2_rust ./cmd/bench2_rust
go build -o bin/bench_rust ./cmd/bench_rust
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/bench2_native_stripped ./cmd/bench2_native
go build -ldflags="-s -w" -o bin/bench2_rust_stripped ./cmd/bench2_rust
ls -la bin/; ldd bin/bench2_rust

stage "ffi-micro-and-check"
./bin/bench_rust ffi
./bin/bench2_rust check 2>&1 | tee $OUT/check.txt; gsutil -q cp $OUT/check.txt gs://$BUCKET/results/vm4/ || true

run() { local name=$1; shift; echo "##### $name $(date -u)"; /usr/bin/time -f "rss_max_kb=%M user=%U sys=%S wall=%e" "$@" 2>&1 | tee $OUT/$name.txt; gsutil -q cp $OUT/$name.txt gs://$BUCKET/results/vm4/ || true; }

SIZES=1MiB,16MiB,128MiB,512MiB
ITERS=10
for round in 1 2; do
  stage "seq-round$round"
  run seq_native_h2_r$round  ./bin/bench2_native -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_native_h1_r$round  env NATIVE_HTTP1=1 ./bin/bench2_native -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_rustthin_r$round   ./bin/bench2_rust -sizes $SIZES -iters $ITERS -list-iters 10
done

stage "parallel"
threads() { # name, cmd...   (samples thread count + names at peak)
  local name=$1; shift
  "$@" > $OUT/$name.txt 2>&1 &
  local pid=$!; local max=0
  while kill -0 $pid 2>/dev/null; do
    local t=$(ls /proc/$pid/task 2>/dev/null | wc -l)
    if [ "$t" -gt "$max" ]; then max=$t; cat /proc/$pid/task/*/comm 2>/dev/null | sort | uniq -c | sort -rn > $OUT/$name.threads.txt; fi
    sleep 0.2
  done
  wait $pid
  echo "max_os_threads=$max" | tee -a $OUT/$name.txt
  cat $OUT/$name.txt; gsutil -q cp $OUT/$name.txt $OUT/$name.threads.txt gs://$BUCKET/results/vm4/ || true
}
for par in 16 64 256; do
  threads par${par}_native_h1  env NATIVE_HTTP1=1 ./bin/bench2_native -sizes 16MiB -iters 256 -parallel $par -list-iters 0
  threads par${par}_rustthin   ./bin/bench2_rust -sizes 16MiB -iters 256 -parallel $par -list-iters 0
done

stage "upload"
gsutil -q cp -r $OUT gs://$BUCKET/results/vm4/
echo "=== DONE $(date -u) ==="
echo DONE > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm4/STAGE
