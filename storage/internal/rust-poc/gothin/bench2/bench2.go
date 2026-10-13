// Package bench2 is the 1:1 benchmark: the workload below is written against
// the *shape* of the cloud.google.com/go/storage API and instantiated once
// with the native SDK and once with rustthin. Nothing in Run knows which one
// it is talking to.
package bench2

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// --- the API shape (satisfied by both *storage.Client and *rustthin.Client) --

type Reader interface {
	io.ReadCloser
	Remain() int64
}
type Writer interface{ io.WriteCloser }
type Iter interface {
	Next() (*storage.ObjectAttrs, error)
}
type Object[R Reader, W Writer] interface {
	NewReader(ctx context.Context, opts ...storage.ReaderOption) (R, error)
	NewRangeReader(ctx context.Context, offset, length int64, opts ...storage.ReaderOption) (R, error)
	NewWriter(ctx context.Context) W
}
type Bucket[O Object[R, W], R Reader, W Writer, I Iter] interface {
	Object(name string) O
	Objects(ctx context.Context, q *storage.Query) I
}
type Client[B Bucket[O, R, W, I], O Object[R, W], R Reader, W Writer, I Iter] interface {
	Bucket(name string) B
}

// --- stats ----------------------------------------------------------------

type stats struct {
	durs []time.Duration
	size int64
	ffi  int64
}

func (s *stats) pct(p float64) time.Duration {
	if len(s.durs) == 0 {
		return 0
	}
	c := append([]time.Duration(nil), s.durs...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[int(float64(len(c)-1)*p)]
}
func (s *stats) mbps(p float64) float64 {
	d := s.pct(p)
	if d == 0 {
		return 0
	}
	return float64(s.size) / (1 << 20) / d.Seconds()
}
func (s *stats) mean() time.Duration {
	var sum time.Duration
	for _, d := range s.durs {
		sum += d
	}
	if len(s.durs) == 0 {
		return 0
	}
	return sum / time.Duration(len(s.durs))
}

func parseSizes(s string) []int64 {
	var out []int64
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		mult := int64(1)
		if strings.HasSuffix(p, "MiB") {
			mult, p = 1<<20, strings.TrimSuffix(p, "MiB")
		} else if strings.HasSuffix(p, "KiB") {
			mult, p = 1<<10, strings.TrimSuffix(p, "KiB")
		}
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			panic(err)
		}
		out = append(out, n*mult)
	}
	return out
}

// noWriterTo hides WriterTo/ReaderFrom fast paths so io.Copy really issues
// bufSize-sized Read/Write calls (what a user copying from a net.Conn gets).
type noWriterTo struct{ io.Reader }
type noReaderFrom struct{ io.Writer }

// --- the workload (identical for both backends) ------------------------------

// Run executes W1R3 in two styles per size:
//
//	"whole":  one Write(data) + io.ReadFull(r, buf)        (large calls)
//	"32k":    io.CopyBuffer 32 KiB writes + 32 KiB reads   (streaming calls)
//
// and a paged list. FFICalls (if non-nil) is sampled around each op.
func Run[C Client[B, O, R, W, I], B Bucket[O, R, W, I], O Object[R, W], R Reader, W Writer, I Iter](name string, c C, ffiCalls *atomic.Int64) {
	bucket := flag.String("bucket", "cpriti-thickthin-poc-asse1", "bucket")
	sizes := flag.String("sizes", "1MiB,16MiB,128MiB", "object sizes")
	iters := flag.Int("iters", 10, "iterations per size")
	par := flag.Int("parallel", 1, "concurrent workers")
	styles := flag.String("styles", "whole,32k", "whole,32k")
	listPrefix := flag.String("list-prefix", "list/", "prefix to list")
	listIters := flag.Int("list-iters", 5, "list iterations")
	flag.Parse()
	ctx := context.Background()

	ffi := func() int64 {
		if ffiCalls == nil {
			return 0
		}
		return ffiCalls.Load()
	}

	fmt.Printf("backend=%s bucket=%s parallel=%d\n", name, *bucket, *par)
	fmt.Printf("%-10s %-6s %-7s %-7s %9s %9s %9s %9s %10s %8s %9s %9s\n", "backend", "style", "op", "size", "p50", "p90", "max", "mean", "p50 MiB/s", "ffi/op", "cpu s/GiB", "peak MiB")

	for _, style := range strings.Split(*styles, ",") {
		for _, sz := range parseSizes(*sizes) {
			data := make([]byte, sz)
			rand.Read(data)
			ws := &stats{size: sz}
			rs := &stats{size: sz}
			// Per-workload resource accounting (whole process: Go + Rust threads).
			resetPeakRSS()
			sampler := startRSSSampler()
			cpu0 := cpuSeconds()
			var mu sync.Mutex
			var wg sync.WaitGroup
			work := make(chan int)
			for w := 0; w < *par; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					buf := make([]byte, sz)
					cbuf := make([]byte, 32<<10)
					for i := range work {
						obj := fmt.Sprintf("w1r3v2/%s/%s/%d/%d-%d", name, style, sz, w, i)
						h := c.Bucket(*bucket).Object(obj)

						// ---- write: exactly what a user writes ----
						f0 := ffi()
						t := time.Now()
						wr := h.NewWriter(ctx)
						var werr error
						if style == "whole" {
							_, werr = wr.Write(data)
						} else {
							_, werr = io.CopyBuffer(wr, noWriterTo{bytes.NewReader(data)}, cbuf)
						}
						if werr == nil {
							werr = wr.Close()
						}
						wd := time.Since(t)
						wf := ffi() - f0
						if werr != nil {
							fmt.Fprintln(os.Stderr, "write:", werr)
							continue
						}

						// ---- read x3 ----
						var rds []time.Duration
						var rf int64
						okr := true
						for k := 0; k < 3; k++ {
							f0 = ffi()
							t = time.Now()
							r, err := h.NewReader(ctx)
							if err != nil {
								fmt.Fprintln(os.Stderr, "open:", err)
								okr = false
								break
							}
							var n int64
							if style == "whole" {
								nn, e := io.ReadFull(r, buf)
								n, err = int64(nn), e
							} else {
								n, err = io.CopyBuffer(noReaderFrom{io.Discard}, noWriterTo{r}, cbuf)
							}
							r.Close()
							if err != nil || n != sz {
								fmt.Fprintln(os.Stderr, "read:", n, err)
								okr = false
								break
							}
							rds = append(rds, time.Since(t))
							rf += ffi() - f0
						}
						mu.Lock()
						ws.durs = append(ws.durs, wd)
						ws.ffi += wf
						if okr {
							rs.durs = append(rs.durs, rds...)
							rs.ffi += rf
						}
						mu.Unlock()
					}
				}(w)
			}
			for i := 0; i < *iters; i++ {
				work <- i
			}
			close(work)
			wg.Wait()
			cpu := cpuSeconds() - cpu0
			peak := peakRSSMiB()
			if sampled := sampler.stopMiB(); peak == 0 || sampled > peak {
				peak = sampled
			}
			// CPU is attributed to write+read bytes moved in this cell (1 write + 3 reads per iteration).
			gib := float64(sz) * float64(len(ws.durs)+len(rs.durs)) / (1 << 30)
			cpuPerGiB := 0.0
			if gib > 0 {
				cpuPerGiB = cpu / gib
			}
			pr := func(op string, s *stats) {
				n := max(1, len(s.durs))
				fmt.Printf("%-10s %-6s %-7s %-7s %9v %9v %9v %9v %10.1f %8.1f %9.2f %9.0f\n", name, style, op, human(sz),
					s.pct(.5).Round(time.Millisecond), s.pct(.9).Round(time.Millisecond), s.pct(1).Round(time.Millisecond), s.mean().Round(time.Millisecond), s.mbps(.5), float64(s.ffi)/float64(n), cpuPerGiB, peak)
			}
			pr("write", ws)
			pr("read", rs)
		}
	}

	ls := &stats{}
	var cnt int
	resetPeakRSS()
	lcpu0 := cpuSeconds()
	for i := 0; i < *listIters; i++ {
		f0 := ffi()
		t := time.Now()
		it := c.Bucket(*bucket).Objects(ctx, &storage.Query{Prefix: *listPrefix})
		cnt = 0
		for {
			_, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "list:", err)
				break
			}
			cnt++
		}
		ls.durs = append(ls.durs, time.Since(t))
		ls.ffi += ffi() - f0
	}
	lcpu := (cpuSeconds() - lcpu0) * 1000 / float64(max(1, len(ls.durs)))
	fmt.Printf("%-10s %-6s %-7s %-7s %9v %9v %9v %9v %10s %8.1f %6.2fms/call %6.0f  (objects=%d)\n", name, "-", "list", "-",
		ls.pct(.5).Round(time.Millisecond), ls.pct(.9).Round(time.Millisecond), ls.pct(1).Round(time.Millisecond), ls.mean().Round(time.Millisecond), "-", float64(ls.ffi)/float64(max(1, len(ls.durs))), lcpu, peakRSSMiB(), cnt)
}

func human(sz int64) string {
	if sz >= 1<<20 {
		return fmt.Sprintf("%dMiB", sz>>20)
	}
	return fmt.Sprintf("%dKiB", sz>>10)
}
