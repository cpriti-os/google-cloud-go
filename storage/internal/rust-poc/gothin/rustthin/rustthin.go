// Package rustthin is a drop-in-shaped subset of cloud.google.com/go/storage
// backed by the Rust gcs_core. It reuses storage's public types (Query,
// ObjectAttrs, Conditions, ErrObjectNotExist) so that user code written for
// the native SDK compiles unchanged for the covered surface:
//
//	client.Bucket(b).Object(o).NewReader(ctx) / NewRangeReader(ctx, off, len)
//	client.Bucket(b).Object(o).If(storage.Conditions{...}).NewWriter(ctx)
//	client.Bucket(b).Objects(ctx, &storage.Query{...}) -> Next()/PageInfo()
package rustthin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"poc/gothin/gcsrust"
)

// ---- Client / handles ------------------------------------------------------

type Client struct{ c *gcsrust.Client }

// NewClient mirrors storage.NewClient(ctx, opts...). ClientOptions that carry
// Go objects (WithHTTPClient, WithTokenSource, WithGRPCConn, ...) cannot be
// honoured by the Rust core; only the endpoint is forwarded in this POC.
func NewClient(ctx context.Context, endpoint string) (*Client, error) {
	c, err := gcsrust.NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	return &Client{c: c}, nil
}

func (c *Client) Close() error { c.c.Close(); return nil }

type BucketHandle struct {
	c    *Client
	name string
}

func (c *Client) Bucket(name string) *BucketHandle { return &BucketHandle{c: c, name: name} }

type ObjectHandle struct {
	b         *BucketHandle
	object    string
	gen       int64
	conds     *storage.Conditions
	readCompr bool
}

func (b *BucketHandle) Object(name string) *ObjectHandle { return &ObjectHandle{b: b, object: name} }

func (o *ObjectHandle) Generation(gen int64) *ObjectHandle {
	o2 := *o
	o2.gen = gen
	return &o2
}

func (o *ObjectHandle) If(conds storage.Conditions) *ObjectHandle {
	o2 := *o
	o2.conds = &conds
	return &o2
}

func (o *ObjectHandle) ReadCompressed(compressed bool) *ObjectHandle {
	o2 := *o
	o2.readCompr = compressed
	return &o2
}

func (o *ObjectHandle) ifGenMatch() int64 {
	if o.conds == nil {
		return -1
	}
	if o.conds.DoesNotExist {
		return 0
	}
	if o.conds.GenerationMatch != 0 {
		return o.conds.GenerationMatch
	}
	return -1
}

// ---- Errors ----------------------------------------------------------------

func mapErr(err error) error {
	var e *gcsrust.Error
	if errors.As(err, &e) {
		switch {
		case e.Code == 4:
			return context.Canceled
		case e.HTTPCode == 404:
			return storage.ErrObjectNotExist
		case e.HTTPCode != 0:
			return &googleapi.Error{Code: e.HTTPCode, Message: e.Msg}
		}
	}
	return err
}

// ---- Reader ----------------------------------------------------------------

// Reader mirrors storage.Reader (io.ReadCloser + Attrs/Remain).
type Reader struct {
	ctx    context.Context
	r      *gcsrust.Reader
	Attrs  storage.ReaderObjectAttrs
	remain int64
	eof    bool
}

func (o *ObjectHandle) NewReader(ctx context.Context, opts ...storage.ReaderOption) (*Reader, error) {
	return o.NewRangeReader(ctx, 0, -1, opts...)
}

// ReaderOptions (ReadHandle etc.) are gRPC-only in the native SDK; accepted and ignored here.
func (o *ObjectHandle) NewRangeReader(ctx context.Context, offset, length int64, _ ...storage.ReaderOption) (*Reader, error) {
	if offset < 0 && length >= 0 {
		return nil, fmt.Errorf("storage: invalid offset %d < 0 requires negative length", offset)
	}
	r, err := o.b.c.c.OpenReader(ctx, o.b.name, o.object, offset, length, o.gen, o.ifGenMatch())
	if err != nil {
		return nil, mapErr(err)
	}
	a := r.Attrs()
	rd := &Reader{ctx: ctx, r: r}
	rd.Attrs = storage.ReaderObjectAttrs{
		Size:            a.Size,
		StartOffset:     max(offset, 0),
		ContentType:     a.ContentType,
		ContentEncoding: a.ContentEncoding,
		Generation:      a.Generation,
		Metageneration:  a.Metageneration,
	}
	if length >= 0 {
		rd.remain = min(length, a.Size-max(offset, 0))
	} else {
		rd.remain = a.Size - max(offset, 0)
	}
	return rd, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}
	n, err := r.r.Read(r.ctx, p)
	if err != nil {
		return n, mapErr(err)
	}
	if n == 0 {
		r.eof = true
		return 0, io.EOF
	}
	r.remain -= int64(n)
	return n, nil
}

func (r *Reader) Remain() int64 { return r.remain }

func (r *Reader) Close() error { r.r.Close(); return nil }

// ---- Writer ----------------------------------------------------------------

// Writer mirrors storage.Writer: embed ObjectAttrs, ChunkSize, Attrs() after Close.
// Like storage.Writer it buffers up to ChunkSize before starting the upload so
// that content ending within the first chunk goes as a single-shot upload.
type Writer struct {
	storage.ObjectAttrs
	ChunkSize int

	ctx   context.Context
	o     *ObjectHandle
	w     *gcsrust.Writer
	buf   []byte
	attrs *storage.ObjectAttrs
	err   error
}

func (o *ObjectHandle) NewWriter(ctx context.Context) *Writer {
	return &Writer{ctx: ctx, o: o, ChunkSize: googleapi.DefaultUploadChunkSize}
}

func (w *Writer) open(knownSize int64) error {
	if w.w != nil || w.err != nil {
		return w.err
	}
	ww, err := w.o.b.c.c.OpenWriter(w.o.b.name, w.o.object, w.ContentType, w.o.ifGenMatch(), w.ChunkSize, knownSize)
	if err != nil {
		w.err = mapErr(err)
		return w.err
	}
	w.w = ww
	return nil
}

func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.w.Write(w.ctx, w.buf)
	w.buf = w.buf[:0]
	if err != nil {
		w.err = mapErr(err)
	}
	return w.err
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.w == nil {
		// Still deciding single-shot vs resumable: buffer up to ChunkSize.
		if w.ChunkSize > 0 && len(w.buf)+len(p) < w.ChunkSize {
			w.buf = append(w.buf, p...)
			return len(p), nil
		}
		if err := w.open(-1); err != nil {
			return 0, err
		}
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	n, err := w.w.Write(w.ctx, p)
	if err != nil {
		w.err = mapErr(err)
	}
	return n, w.err
}

func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.w == nil {
		if err := w.open(int64(len(w.buf))); err != nil { // single-shot with exact size
			return err
		}
	}
	if err := w.flush(); err != nil {
		return err
	}
	size, gen, err := w.w.Close(w.ctx)
	w.w.Free()
	if err != nil {
		w.err = mapErr(err)
		return w.err
	}
	w.attrs = &storage.ObjectAttrs{Bucket: w.o.b.name, Name: w.o.object, Size: size, Generation: gen, ContentType: w.ContentType}
	return nil
}

func (w *Writer) Attrs() *storage.ObjectAttrs { return w.attrs }

// ---- Objects iterator ------------------------------------------------------

type ObjectIterator struct {
	b        *BucketHandle
	q        storage.Query
	pageInfo *iterator.PageInfo
	nextFunc func() error
	items    []*storage.ObjectAttrs
}

func (b *BucketHandle) Objects(ctx context.Context, q *storage.Query) *ObjectIterator {
	it := &ObjectIterator{b: b}
	if q != nil {
		it.q = *q
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.fetch,
		func() int { return len(it.items) },
		func() interface{} { b := it.items; it.items = nil; return b })
	return it
}

func (it *ObjectIterator) PageInfo() *iterator.PageInfo { return it.pageInfo }

func (it *ObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if err := it.nextFunc(); err != nil {
		return nil, err
	}
	item := it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ObjectIterator) fetch(pageSize int, pageToken string) (string, error) {
	page, err := it.b.c.c.ListPage(it.b.name, it.q.Prefix, it.q.Delimiter, it.q.StartOffset, it.q.EndOffset, pageToken, pageSize, it.q.Versions)
	if err != nil {
		return "", mapErr(err)
	}
	for _, p := range page.Prefixes {
		it.items = append(it.items, &storage.ObjectAttrs{Prefix: p})
	}
	for _, o := range page.Items {
		it.items = append(it.items, &storage.ObjectAttrs{
			Bucket: it.b.name, Name: o.Name, Size: o.Size, Generation: o.Generation, Metageneration: o.Metageneration,
			ContentType: o.ContentType, StorageClass: o.StorageClass, CRC32C: o.CRC32C,
			Updated: time.Unix(o.UpdatedSecs, 0),
		})
	}
	return page.Next, nil
}
