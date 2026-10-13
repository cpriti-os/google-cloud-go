// bench2_native: the 1:1 workload against the real cloud.google.com/go/storage.
package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"poc/gothin/bench2"
)

func main() {
	ctx := context.Background()
	var opts []option.ClientOption
	name := "go-json"
	if os.Getenv("NATIVE_HTTP1") == "1" {
		base := http.DefaultTransport.(*http.Transport).Clone()
		base.ForceAttemptHTTP2 = false
		base.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		base.TLSClientConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
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
	bench2.Run[*storage.Client, *storage.BucketHandle, *storage.ObjectHandle, *storage.Reader, *storage.Writer, *storage.ObjectIterator](name, c, nil)
}
