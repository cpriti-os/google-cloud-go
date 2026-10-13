// Package gcsrust is a thin Go wrapper over the Rust gcs_core staticlib (POC).
package gcsrust

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: ${SRCDIR}/../../gcs_core/target/release/libgcs_core.a -lm -ldl -lpthread
#include <stdlib.h>
#include <stdint.h>
#include "gcs_core.h"

// Trampoline so Rust can call back into Go.
extern uint64_t goCallbackTrampoline(void*, uint64_t);
static uint64_t call_callback_bench(uint64_t n, void* user) {
	return gcs_callback_bench(n, goCallbackTrampoline, user);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unsafe"
)

// Error carries the flattened (code, message) that crosses the FFI boundary.
// NOTE: compared to the native Go SDK, callers lose *googleapi.Error,
// status.Code(), errors.As(..) etc.
type Error struct {
	Code     int // 1 generic, 2 panic, 3 bad arg, 4 cancelled
	HTTPCode int // HTTP status from the service, 0 if none
	Msg      string
}

func (e *Error) Error() string {
	return fmt.Sprintf("gcs_core: code=%d http=%d: %s", e.Code, e.HTTPCode, e.Msg)
}

func toErr(st C.GcsStatus) error {
	if st.code == 0 {
		return nil
	}
	msg := C.GoString(st.message)
	C.gcs_string_free(st.message)
	return &Error{Code: int(st.code), HTTPCode: int(st.http_code), Msg: msg}
}

type Client struct {
	ptr *C.GcsClient
}

func NewClient(endpoint string) (*Client, error) {
	var p *C.GcsClient
	var cend *C.char
	if endpoint != "" {
		cend = C.CString(endpoint)
		defer C.free(unsafe.Pointer(cend))
	}
	if err := toErr(C.gcs_client_new(cend, &p)); err != nil {
		return nil, err
	}
	c := &Client{ptr: p}
	runtime.SetFinalizer(c, func(c *Client) { c.Close() })
	return c, nil
}

func (c *Client) Close() {
	if c.ptr != nil {
		C.gcs_client_free(c.ptr)
		c.ptr = nil
	}
}

// ReadInto downloads the object (or [offset, offset+length)) into buf and
// returns the number of bytes written.
func (c *Client) ReadInto(bucket, object string, offset, length int64, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, errors.New("empty buffer")
	}
	cb, co := C.CString(bucket), C.CString(object)
	defer C.free(unsafe.Pointer(cb))
	defer C.free(unsafe.Pointer(co))
	var n C.size_t
	st := C.gcs_read_object(c.ptr, cb, co, C.uint64_t(offset), C.uint64_t(length),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &n)
	if err := toErr(st); err != nil {
		return 0, err
	}
	return int(n), nil
}

// Write uploads data as object and returns the new generation.
func (c *Client) Write(bucket, object string, data []byte) (int64, error) {
	cb, co := C.CString(bucket), C.CString(object)
	defer C.free(unsafe.Pointer(cb))
	defer C.free(unsafe.Pointer(co))
	var gen C.int64_t
	var p *C.uint8_t
	if len(data) > 0 {
		p = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}
	st := C.gcs_write_object(c.ptr, cb, co, p, C.size_t(len(data)), &gen)
	if err := toErr(st); err != nil {
		return 0, err
	}
	return int64(gen), nil
}

// List returns all object names under prefix.
func (c *Client) List(bucket, prefix string) ([]string, error) {
	cb := C.CString(bucket)
	defer C.free(unsafe.Pointer(cb))
	var cp *C.char
	if prefix != "" {
		cp = C.CString(prefix)
		defer C.free(unsafe.Pointer(cp))
	}
	var names *C.char
	var count C.size_t
	if err := toErr(C.gcs_list_objects(c.ptr, cb, cp, &names, &count)); err != nil {
		return nil, err
	}
	s := C.GoString(names)
	C.gcs_string_free(names)
	out := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	if len(out) == 1 && out[0] == "" {
		out = nil
	}
	return out, nil
}

// Noop is used to measure raw cgo call overhead.
func Noop(x uint64) uint64 { return uint64(C.gcs_noop(C.uint64_t(x))) }

// CallbackBench asks Rust to call back into Go n times.
func CallbackBench(n uint64) uint64 { return uint64(C.call_callback_bench(C.uint64_t(n), nil)) }

//export goCallbackTrampoline
func goCallbackTrampoline(_ unsafe.Pointer, i C.uint64_t) C.uint64_t { return i + 1 }
