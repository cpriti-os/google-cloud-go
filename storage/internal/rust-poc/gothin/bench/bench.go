// Package bench is a tiny W1R3-style harness shared by the native and rust mains.
package bench

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Backend interface {
	Name() string
	Write(ctx context.Context, bucket, object string, data []byte) error
	Read(ctx context.Context, bucket, object string, buf []byte) (int, error)
	List(ctx context.Context, bucket, prefix string) (int, error)
}

type stats struct {
	durs []time.Duration
	size int64
}

func (s *stats) add(d time.Duration) { s.durs = append(s.durs, d) }
func (s *stats) pct(p float64) time.Duration {
	if len(s.durs) == 0 {
		return 0
	}
	c := append([]time.Duration(nil), s.durs...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	i := int(float64(len(c)-1) * p)
	return c[i]
}
func (s *stats) mbps(p float64) float64 {
	d := s.pct(p)
	if d == 0 {
		return 0
	}
	return float64(s.size) / (1024 * 1024) / d.Seconds()
}

func parseSizes(s string) []int64 {
	var out []int64
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		mult := int64(1)
		switch {
		case strings.HasSuffix(p, "MiB"):
			mult = 1 << 20
			p = strings.TrimSuffix(p, "MiB")
		case strings.HasSuffix(p, "KiB"):
			mult = 1 << 10
			p = strings.TrimSuffix(p, "KiB")
		}
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			panic(err)
		}
		out = append(out, n*mult)
	}
	return out
}

// Run executes the harness with flags parsed from os.Args.
func Run(b Backend) {
	bucket := flag.String("bucket", "cpriti-thickthin-poc-asse1", "bucket")
	sizes := flag.String("sizes", "1MiB,16MiB,128MiB", "object sizes")
	iters := flag.Int("iters", 10, "iterations per size")
	par := flag.Int("parallel", 1, "concurrent workers")
	listPrefix := flag.String("list-prefix", "list/", "prefix to list")
	listIters := flag.Int("list-iters", 5, "list iterations")
	skipData := flag.Bool("skip-data", false, "skip W1R3")
	flag.Parse()

	ctx := context.Background()
	fmt.Printf("backend=%s bucket=%s parallel=%d\n", b.Name(), *bucket, *par)
	fmt.Printf("%-8s %-10s %-7s %9s %9s %9s %10s %10s\n", "backend", "op", "size", "p50", "p90", "max", "p50 MiB/s", "mean MiB/s")

	if !*skipData {
		for _, sz := range parseSizes(*sizes) {
			data := make([]byte, sz)
			rand.Read(data)
			ws := &stats{size: sz}
			rs := &stats{size: sz}
			var mu sync.Mutex
			var wg sync.WaitGroup
			work := make(chan int)
			for w := 0; w < *par; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					buf := make([]byte, sz)
					for i := range work {
						obj := fmt.Sprintf("w1r3/%s/%d/%d-%d", b.Name(), sz, w, i)
						t := time.Now()
						if err := b.Write(ctx, *bucket, obj, data); err != nil {
							fmt.Fprintln(os.Stderr, "write:", err)
							continue
						}
						wd := time.Since(t)
						var rds []time.Duration
						okr := true
						for r := 0; r < 3; r++ {
							t = time.Now()
							n, err := b.Read(ctx, *bucket, obj, buf)
							if err != nil || int64(n) != sz {
								fmt.Fprintln(os.Stderr, "read:", n, err)
								okr = false
								break
							}
							rds = append(rds, time.Since(t))
						}
						mu.Lock()
						ws.add(wd)
						if okr {
							for _, d := range rds {
								rs.add(d)
							}
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
			var sum time.Duration
			for _, d := range ws.durs {
				sum += d
			}
			meanW := float64(sz) / (1 << 20) / (sum.Seconds() / float64(max(1, len(ws.durs))))
			sum = 0
			for _, d := range rs.durs {
				sum += d
			}
			meanR := float64(sz) / (1 << 20) / (sum.Seconds() / float64(max(1, len(rs.durs))))
			fmt.Printf("%-8s %-10s %-7s %9v %9v %9v %10.1f %10.1f\n", b.Name(), "write", human(sz), ws.pct(.5).Round(time.Millisecond), ws.pct(.9).Round(time.Millisecond), ws.pct(1).Round(time.Millisecond), ws.mbps(.5), meanW)
			fmt.Printf("%-8s %-10s %-7s %9v %9v %9v %10.1f %10.1f\n", b.Name(), "read", human(sz), rs.pct(.5).Round(time.Millisecond), rs.pct(.9).Round(time.Millisecond), rs.pct(1).Round(time.Millisecond), rs.mbps(.5), meanR)
		}
	}

	ls := &stats{}
	var n int
	for i := 0; i < *listIters; i++ {
		t := time.Now()
		var err error
		n, err = b.List(ctx, *bucket, *listPrefix)
		if err != nil {
			fmt.Fprintln(os.Stderr, "list:", err)
			break
		}
		ls.add(time.Since(t))
	}
	fmt.Printf("%-8s %-10s %-7s %9v %9v %9v  (objects=%d)\n", b.Name(), "list", "-", ls.pct(.5).Round(time.Millisecond), ls.pct(.9).Round(time.Millisecond), ls.pct(1).Round(time.Millisecond), n)
}

func human(sz int64) string {
	if sz >= 1<<20 {
		return fmt.Sprintf("%dMiB", sz>>20)
	}
	return fmt.Sprintf("%dKiB", sz>>10)
}
