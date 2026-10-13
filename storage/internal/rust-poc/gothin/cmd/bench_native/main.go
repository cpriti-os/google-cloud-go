// bench_native: pure-Go GCS SDK over JSON (CGO_ENABLED=0 friendly).
package main

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"poc/gothin/bench"
)

type native struct {
	c    *storage.Client
	name string
}

func (n native) Name() string { return n.name }

func (n native) Write(ctx context.Context, bucket, object string, data []byte) error {
	w := n.c.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

func (n native) Read(ctx context.Context, bucket, object string, buf []byte) (int, error) {
	r, err := n.c.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	return io.ReadFull(r, buf)
}

func (n native) List(ctx context.Context, bucket, prefix string) (int, error) {
	it := n.c.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	cnt := 0
	for {
		_, err := it.Next()
		if err == iterator.Done {
			return cnt, nil
		}
		if err != nil {
			return cnt, err
		}
		cnt++
	}
}

func main() {
	ctx := context.Background()
	var opts []option.ClientOption
	name := "go-json"
	if os.Getenv("NATIVE_HTTP1") == "1" {
		// Same connection model as the Rust core (reqwest without the `http2`
		// feature => HTTP/1.1, one TCP connection per in-flight request).
		// This is also what gcsfuse does by default (--client-protocol http1).
		base := http.DefaultTransport.(*http.Transport).Clone()
		base.ForceAttemptHTTP2 = false
		base.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		base.TLSClientConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
		base.MaxConnsPerHost = 0
		base.MaxIdleConnsPerHost = 256
		rt, err := htransport.NewTransport(ctx, base, option.WithScopes(storage.ScopeFullControl))
		if err != nil {
			panic(err)
		}
		opts = append(opts, option.WithHTTPClient(&http.Client{Transport: rt}))
		name = "go-json-h1"
	}
	c, err := storage.NewClient(ctx, opts...)
	if err != nil {
		panic(err)
	}
	bench.Run(native{c, name})
}
