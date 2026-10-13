// bench2_rust: the same 1:1 workload against rustthin (Go facade -> Rust core).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"poc/gothin/bench2"
	"poc/gothin/gcsrust"
	"poc/gothin/rustthin"
)

func main() {
	t := time.Now()
	c, err := rustthin.NewClient(context.Background(), "")
	if err != nil {
		panic(err)
	}
	fmt.Printf("client init: %v\n", time.Since(t))
	if len(os.Args) > 1 && os.Args[1] == "check" {
		check(c)
		return
	}
	bench2.Run[*rustthin.Client, *rustthin.BucketHandle, *rustthin.ObjectHandle, *rustthin.Reader, *rustthin.Writer, *rustthin.ObjectIterator]("go-rustthin", c, &gcsrust.FFICalls)
}

// check exercises the storage error/ctx contracts through the facade.
func check(c *rustthin.Client) {
	ctx := context.Background()
	b := c.Bucket("cpriti-thickthin-poc-asse1")

	_, err := b.Object("does/not/exist").NewReader(ctx)
	fmt.Printf("missing object -> errors.Is(ErrObjectNotExist)=%v (%v)\n", errors.Is(err, storage.ErrObjectNotExist), err)

	w := b.Object("check/exists").NewWriter(ctx)
	w.Write([]byte("x"))
	fmt.Printf("create -> err=%v attrs=%+v\n", w.Close(), w.Attrs())

	w = b.Object("check/exists").If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	w.Write([]byte("y"))
	err = w.Close()
	var ge *googleapi.Error
	fmt.Printf("DoesNotExist on existing -> googleapi.Error=%v code=%d (%v)\n", errors.As(err, &ge), func() int {
		if ge != nil {
			return ge.Code
		}
		return 0
	}(), err)

	r, err := b.Object("check/exists").NewRangeReader(ctx, 0, 1)
	if err == nil {
		data, _ := io.ReadAll(r)
		fmt.Printf("range read -> attrs=%+v remain=%d data=%q\n", r.Attrs, r.Remain(), data)
		r.Close()
	}

	// ctx cancellation mid-stream on a big object
	w = b.Object("check/big").NewWriter(ctx)
	w.Write(make([]byte, 64<<20))
	w.Close()
	cctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	t := time.Now()
	r, err = b.Object("check/big").NewReader(cctx)
	var n int64
	if err == nil {
		n, err = io.Copy(io.Discard, r)
		r.Close()
	}
	fmt.Printf("ctx timeout 30ms mid-stream -> after %v copied=%d err=%v (errors.Is(ctx.Err))=%v\n", time.Since(t).Round(time.Millisecond), n, err, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}
