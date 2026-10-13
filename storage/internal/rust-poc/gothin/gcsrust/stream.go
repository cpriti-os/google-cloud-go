package gcsrust

/*
#include <stdlib.h>
#include <stdint.h>
#include "gcs_core.h"

extern void goDoneCb(void*, GcsStatus, uint64_t);
static uint64_t reader_open(const GcsClient *c, const char *b, const char *o, int64_t off, int64_t len,
                            int64_t gen, int64_t ifgen, void *user) {
	return gcs_reader_open_async(c, b, o, off, len, gen, ifgen, goDoneCb, user);
}
static uint64_t reader_read(GcsReader *r, uint8_t *buf, size_t cap, void *user) {
	return gcs_reader_read_async(r, buf, cap, goDoneCb, user);
}
static uint64_t writer_write(GcsWriter *w, const uint8_t *data, size_t len, void *user) {
	return gcs_writer_write_async(w, data, len, goDoneCb, user);
}
static uint64_t writer_close(GcsWriter *w, void *user) {
	return gcs_writer_close_async(w, goDoneCb, user);
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"runtime/cgo"
	"sync/atomic"
	"unsafe"
)

// FFICalls counts boundary crossings (for the report).
var FFICalls atomic.Int64

func cstr(s string) *C.char {
	if s == "" {
		return nil
	}
	return C.CString(s)
}
func cfree(p *C.char) {
	if p != nil {
		C.free(unsafe.Pointer(p))
	}
}

// ---- Reader ---------------------------------------------------------------

type Reader struct {
	ptr *C.GcsReader
}

type ObjectAttrs struct {
	Size, Generation, Metageneration int64
	ContentType, ContentEncoding     string
}

// OpenReader mirrors ObjectHandle.NewRangeReader(ctx, offset, length).
func (c *Client) OpenReader(ctx context.Context, bucket, object string, offset, length, generation, ifGenMatch int64) (*Reader, error) {
	cb, co := C.CString(bucket), C.CString(object)
	defer C.free(unsafe.Pointer(cb))
	defer C.free(unsafe.Pointer(co))
	ch := make(chan doneResult, 1)
	h := cgo.NewHandle(ch)
	defer h.Delete()
	FFICalls.Add(1)
	id := C.reader_open(c.ptr, cb, co, C.int64_t(offset), C.int64_t(length), C.int64_t(generation), C.int64_t(ifGenMatch), unsafe.Pointer(uintptr(h)))
	n, err := await(ctx, uint64(id), ch)
	if err != nil {
		return nil, err
	}
	return &Reader{ptr: (*C.GcsReader)(unsafe.Pointer(uintptr(n)))}, nil
}

func (r *Reader) Attrs() ObjectAttrs {
	var a C.GcsObjectAttrs
	FFICalls.Add(1)
	C.gcs_reader_attrs(r.ptr, &a)
	return ObjectAttrs{
		Size: int64(a.size), Generation: int64(a.generation), Metageneration: int64(a.metageneration),
		ContentType: C.GoString(a.content_type), ContentEncoding: C.GoString(a.content_encoding),
	}
}

// Read fills p with the next available bytes (io.Reader contract); returns 0
// at EOF. p is pinned for the duration of the async Rust task.
func (r *Reader) Read(ctx context.Context, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var pin runtime.Pinner
	pin.Pin(&p[0])
	defer pin.Unpin()
	ch := make(chan doneResult, 1)
	h := cgo.NewHandle(ch)
	defer h.Delete()
	FFICalls.Add(1)
	id := C.reader_read(r.ptr, (*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p)), unsafe.Pointer(uintptr(h)))
	n, err := await(ctx, uint64(id), ch)
	return int(n), err
}

func (r *Reader) Close() {
	if r.ptr != nil {
		FFICalls.Add(1)
		C.gcs_reader_close(r.ptr)
		r.ptr = nil
	}
}

// ---- Writer ---------------------------------------------------------------

type Writer struct {
	ptr *C.GcsWriter
}

// OpenWriter mirrors ObjectHandle.NewWriter + Writer fields. ifGenMatch<0 = unset.
// knownSize >= 0 lets the crate choose a single-shot upload (like storage.Writer
// does when the content ends within the first ChunkSize).
func (c *Client) OpenWriter(bucket, object, contentType string, ifGenMatch int64, chunkSize int, knownSize int64) (*Writer, error) {
	cb, co, ct := C.CString(bucket), C.CString(object), cstr(contentType)
	defer C.free(unsafe.Pointer(cb))
	defer C.free(unsafe.Pointer(co))
	defer cfree(ct)
	var w *C.GcsWriter
	FFICalls.Add(1)
	if err := toErr(C.gcs_writer_open(c.ptr, cb, co, ct, C.int64_t(ifGenMatch), C.size_t(chunkSize), C.int64_t(knownSize), &w)); err != nil {
		return nil, err
	}
	return &Writer{ptr: w}, nil
}

// Write copies p into Rust and returns once the chunk is queued (backpressure
// applies). Same contract as storage.Writer.Write.
func (w *Writer) Write(ctx context.Context, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	ch := make(chan doneResult, 1)
	h := cgo.NewHandle(ch)
	defer h.Delete()
	FFICalls.Add(1)
	id := C.writer_write(w.ptr, (*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p)), unsafe.Pointer(uintptr(h)))
	n, err := await(ctx, uint64(id), ch)
	return int(n), err
}

// Close finishes the upload and returns (size, generation).
func (w *Writer) Close(ctx context.Context) (int64, int64, error) {
	ch := make(chan doneResult, 1)
	h := cgo.NewHandle(ch)
	defer h.Delete()
	FFICalls.Add(1)
	id := C.writer_close(w.ptr, unsafe.Pointer(uintptr(h)))
	_, err := await(ctx, uint64(id), ch)
	if err != nil {
		return 0, 0, err
	}
	var size, gen C.int64_t
	FFICalls.Add(1)
	if C.gcs_writer_result(w.ptr, &size, &gen) == 0 {
		return 0, 0, errors.New("gcs_core: no upload result")
	}
	return int64(size), int64(gen), nil
}

func (w *Writer) Free() {
	if w.ptr != nil {
		C.gcs_writer_free(w.ptr)
		w.ptr = nil
	}
}

// ---- List -----------------------------------------------------------------

type ListItem struct {
	Name           string `json:"name"`
	Size           int64  `json:"size"`
	Generation     int64  `json:"generation"`
	Metageneration int64  `json:"metageneration"`
	ContentType    string `json:"content_type"`
	StorageClass   string `json:"storage_class"`
	UpdatedSecs    int64  `json:"updated_secs"`
	CRC32C         uint32 `json:"crc32c"`
}

type ListPage struct {
	Items    []ListItem `json:"items"`
	Prefixes []string   `json:"prefixes"`
	Next     string     `json:"next"`
}

// ListPage fetches one page (blocking FFI; pins the thread for ~1 RPC).
func (c *Client) ListPage(bucket, prefix, delimiter, startOffset, endOffset, pageToken string, pageSize int, versions bool) (*ListPage, error) {
	cb := C.CString(bucket)
	defer C.free(unsafe.Pointer(cb))
	cp, cd, cs, ce, ct := cstr(prefix), cstr(delimiter), cstr(startOffset), cstr(endOffset), cstr(pageToken)
	defer cfree(cp)
	defer cfree(cd)
	defer cfree(cs)
	defer cfree(ce)
	defer cfree(ct)
	var out *C.char
	FFICalls.Add(1)
	if err := toErr(C.gcs_list_page(c.ptr, cb, cp, cd, cs, ce, ct, C.int32_t(pageSize), C.bool(versions), &out)); err != nil {
		return nil, err
	}
	js := C.GoString(out)
	C.gcs_string_free(out)
	var page ListPage
	if err := json.Unmarshal([]byte(js), &page); err != nil {
		return nil, err
	}
	return &page, nil
}
