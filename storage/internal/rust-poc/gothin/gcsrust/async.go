package gcsrust

/*
#include <stdlib.h>
#include <stdint.h>
#include "gcs_core.h"

extern void goDoneCb(void*, GcsStatus, uint64_t);
static uint64_t read_async(const GcsClient *c, const char *b, const char *o, uint64_t off,
                           uint64_t len, uint8_t *buf, size_t cap, void *user) {
	return gcs_read_object_async(c, b, o, off, len, buf, cap, goDoneCb, user);
}
static uint64_t write_async(const GcsClient *c, const char *b, const char *o,
                            const uint8_t *data, size_t len, void *user) {
	return gcs_write_object_async(c, b, o, data, len, goDoneCb, user);
}
*/
import "C"

import (
	"context"
	"errors"
	"runtime"
	"runtime/cgo"
	"unsafe"
)

type doneResult struct {
	err error
	n   uint64
}

//export goDoneCb
func goDoneCb(user unsafe.Pointer, st C.GcsStatus, n C.uint64_t) {
	// Runs on a tokio worker thread (C-created, non-Go thread).
	h := cgo.Handle(uintptr(user))
	ch := h.Value().(chan doneResult)
	ch <- doneResult{err: toErr(st), n: uint64(n)} // buffered(1): never blocks tokio
}

// ErrCancelled is returned when ctx is done before the op completes.
var ErrCancelled = errors.New("gcs_core: cancelled")

// await parks the goroutine (no OS thread pinned) until the Rust task
// completes, or cancels it when ctx is done. It always waits for the
// completion callback so that pinned buffers are safe to release.
func await(ctx context.Context, opID uint64, ch chan doneResult) (uint64, error) {
	if opID == 0 {
		return 0, errors.New("gcs_core: bad arguments")
	}
	select {
	case r := <-ch:
		return r.n, r.err
	case <-ctx.Done():
		C.gcs_cancel(C.uint64_t(opID))
		<-ch // wait for the callback; buffer no longer referenced by Rust
		return 0, ctx.Err()
	}
}

// ReadIntoCtx is the async-FFI version of ReadInto with ctx cancellation.
func (c *Client) ReadIntoCtx(ctx context.Context, bucket, object string, offset, length int64, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, errors.New("empty buffer")
	}
	cb, co := C.CString(bucket), C.CString(object)
	defer C.free(unsafe.Pointer(cb))
	defer C.free(unsafe.Pointer(co))

	// Rust retains buf after this call returns, so it must be pinned
	// (Go 1.21+). Without this it is a cgo pointer-rule violation.
	var pin runtime.Pinner
	pin.Pin(&buf[0])
	defer pin.Unpin()

	ch := make(chan doneResult, 1)
	h := cgo.NewHandle(ch)
	defer h.Delete()
	id := C.read_async(c.ptr, cb, co, C.uint64_t(offset), C.uint64_t(length),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), unsafe.Pointer(uintptr(h)))
	n, err := await(ctx, uint64(id), ch)
	return int(n), err
}

// WriteCtx is the async-FFI version of Write with ctx cancellation.
func (c *Client) WriteCtx(ctx context.Context, bucket, object string, data []byte) (int64, error) {
	cb, co := C.CString(bucket), C.CString(object)
	defer C.free(unsafe.Pointer(cb))
	defer C.free(unsafe.Pointer(co))
	var p *C.uint8_t
	if len(data) > 0 {
		p = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}
	ch := make(chan doneResult, 1)
	h := cgo.NewHandle(ch)
	defer h.Delete()
	// data is copied inside the call (before return), so no pin needed.
	id := C.write_async(c.ptr, cb, co, p, C.size_t(len(data)), unsafe.Pointer(uintptr(h)))
	gen, err := await(ctx, uint64(id), ch)
	return int64(gen), err
}
