// bench_rust: thin Go wrapper -> Rust gcs_core (cgo).
//
//	bench_rust            blocking FFI backend
//	RUST_ASYNC=1 bench_rust   async FFI backend (tokio spawn + callback)
//	bench_rust ffi        FFI microbenchmarks
//	bench_rust cancel     ctx cancellation test (async FFI)
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"poc/gothin/bench"
	"poc/gothin/gcsrust"
)

type rust struct {
	c     *gcsrust.Client
	async bool
}

func (r rust) Name() string {
	if r.async {
		return "go-rust-async"
	}
	return "go-rust"
}

func (r rust) Write(ctx context.Context, bucket, object string, data []byte) error {
	if r.async {
		_, err := r.c.WriteCtx(ctx, bucket, object, data)
		return err
	}
	_, err := r.c.Write(bucket, object, data)
	return err
}

func (r rust) Read(ctx context.Context, bucket, object string, buf []byte) (int, error) {
	if r.async {
		return r.c.ReadIntoCtx(ctx, bucket, object, 0, 0, buf)
	}
	return r.c.ReadInto(bucket, object, 0, 0, buf)
}

func (r rust) List(ctx context.Context, bucket, prefix string) (int, error) {
	names, err := r.c.List(bucket, prefix)
	return len(names), err
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "ffi" {
		const n = 2_000_000
		t := time.Now()
		for i := uint64(0); i < n; i++ {
			gcsrust.Noop(i)
		}
		fmt.Printf("cgo Go->Rust call: %v/call\n", time.Since(t)/n)
		t = time.Now()
		gcsrust.CallbackBench(n)
		fmt.Printf("Rust->Go callback (inside one cgo call): %v/call\n", time.Since(t)/n)
		return
	}
	t := time.Now()
	c, err := gcsrust.NewClient("")
	if err != nil {
		panic(err)
	}
	fmt.Printf("client init: %v\n", time.Since(t))

	if len(os.Args) > 1 && os.Args[1] == "cancel" {
		bucket := "cpriti-thickthin-poc-asse1"
		if len(os.Args) > 2 {
			bucket = os.Args[2]
		}
		obj := "cancel/128MiB"
		data := make([]byte, 128<<20)
		if _, err := c.WriteCtx(context.Background(), bucket, obj, data); err != nil {
			panic(err)
		}
		buf := make([]byte, 128<<20)
		for _, d := range []time.Duration{20 * time.Millisecond, 100 * time.Millisecond, 10 * time.Second} {
			ctx, cancel := context.WithTimeout(context.Background(), d)
			t := time.Now()
			n, err := c.ReadIntoCtx(ctx, bucket, obj, 0, 0, buf)
			fmt.Printf("ctx timeout=%v -> returned after %v, n=%d, err=%v\n", d, time.Since(t).Round(time.Millisecond), n, err)
			cancel()
		}
		return
	}
	bench.Run(rust{c, os.Getenv("RUST_ASYNC") == "1"})
}
