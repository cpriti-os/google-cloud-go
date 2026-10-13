#!/bin/bash
# Startup script for the thick/thin Go<->Rust benchmark VM.
# Everything logs to /var/log/bench/ and is uploaded to gs://$BUCKET/results/vm/.
set -x
BUCKET=cpriti-thickthin-poc-asse1
OUT=/var/log/bench
mkdir -p $OUT
exec > >(tee -a $OUT/startup.log) 2>&1
echo "=== START $(date -u) host=$(hostname) ==="

stage() { echo "=== STAGE: $1 $(date -u) ==="; echo "$1" > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm/STAGE || true; }

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
gsutil -q cp gs://$BUCKET/poc/poc.tgz /tmp/poc.tgz
tar -xzf /tmp/poc.tgz
ls -la /opt/poc /opt/poc/gothin/bin || true

stage "cloudtop-binary-compat"
# Try the binaries built on the cloudtop (gLinux, glibc 2.39) on Debian 12 (glibc 2.36).
chmod +x /opt/poc/gothin/bin/* || true
echo "--- native static binary:"; /opt/poc/gothin/bin/bench_native -sizes 1MiB -iters 1 -list-iters 1 2>&1 | tail -5
echo "--- cgo dynamic binary:"; /opt/poc/gothin/bin/bench_rust ffi 2>&1 | tail -5
echo "--- cgo -static binary (glibc static, getaddrinfo warning at link time):"; /opt/poc/gothin/bin/bench_rust_static -sizes 1MiB -iters 1 -list-iters 1 2>&1 | tail -5

stage "build-rust"
cd /opt/poc/gcs_core && rm -rf target
/usr/bin/time -f "rust build: wall=%e s user=%U sys=%S maxrss_kb=%M" cargo build --release 2>&1 | tail -5
ls -la target/release/libgcs_core.a target/release/libgcs_core.so

stage "build-go"
cd /opt/poc/gothin && rm -rf bin && mkdir bin
export GOFLAGS=-mod=mod GOPATH=/root/go GOCACHE=/root/.cache/go-build
/usr/bin/time -f "go native build: wall=%e s" env CGO_ENABLED=0 go build -o bin/bench_native ./cmd/bench_native
/usr/bin/time -f "go cgo build: wall=%e s" go build -o bin/bench_rust ./cmd/bench_rust
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/bench_native_stripped ./cmd/bench_native
go build -ldflags="-s -w" -o bin/bench_rust_stripped ./cmd/bench_rust
ls -la bin/; file bin/bench_rust; ldd bin/bench_rust
chmod +x threads.sh

stage "ffi-micro"
./bin/bench_rust ffi

run() { # name, env..., cmd...
  local name=$1; shift
  echo "##### $name $(date -u)"
  /usr/bin/time -f "rss_max_kb=%M user=%U sys=%S wall=%e" "$@" 2>&1 | tee $OUT/$name.txt
  gsutil -q cp $OUT/$name.txt gs://$BUCKET/results/vm/ || true
}

SIZES=1MiB,16MiB,128MiB,512MiB
ITERS=10
for round in 1 2; do
  stage "seq-round$round"
  run seq_native_h2_r$round          ./bin/bench_native -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_native_h1_r$round          env NATIVE_HTTP1=1 ./bin/bench_native -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_rust_block_r$round         ./bin/bench_rust -sizes $SIZES -iters $ITERS -list-iters 10
  run seq_rust_async_r$round         env RUST_ASYNC=1 ./bin/bench_rust -sizes $SIZES -iters $ITERS -list-iters 10
done

stage "parallel"
for par in 16 64 256; do
  run par${par}_native_h2   ./threads.sh native $par
  run par${par}_native_h1   env NATIVE_HTTP1=1 ./threads.sh native $par
  run par${par}_rust_block  ./threads.sh rust $par
  run par${par}_rust_async  env RUST_ASYNC=1 ./threads.sh rust $par
done

stage "cancel"
run cancel_async ./bin/bench_rust cancel $BUCKET

stage "upload"
dmesg | tail -20 > $OUT/dmesg_tail.txt || true
gsutil -q cp -r $OUT gs://$BUCKET/results/vm/
echo "=== DONE $(date -u) ==="
echo DONE > $OUT/STAGE; gsutil -q cp $OUT/STAGE gs://$BUCKET/results/vm/STAGE
