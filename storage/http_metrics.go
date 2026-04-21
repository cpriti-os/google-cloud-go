package storage

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type httpMetricsRoundTripper struct {
	rt          http.RoundTripper
	instruments *metricInstruments
}

func (mrt *httpMetricsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if mrt.instruments == nil {
		return mrt.rt.RoundTrip(req)
	}
	ctx := req.Context()
	method := req.URL.Path
	host := req.URL.Host

	start := time.Now()
	var dnsStart, tcpStart, tlsStart time.Time

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			mrt.instruments.gcsStorageClientNetworkDnsLookupDuration.Record(ctx, time.Since(dnsStart).Seconds(), metric.WithAttributes(
				attribute.String("gcp.client.service", "storage"),
				attribute.String("server.address", host),
			))
		},
		ConnectStart: func(_, _ string) { tcpStart = time.Now() },
		ConnectDone: func(_, _ string, _ error) {
			mrt.instruments.gcsStorageClientNetworkTcpConnectDuration.Record(ctx, time.Since(tcpStart).Seconds(), metric.WithAttributes(
				attribute.String("gcp.client.service", "storage"),
				attribute.String("server.address", host),
			))
		},
		TLSHandshakeStart: func() { tlsStart = time.Now() },
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			mrt.instruments.gcsStorageClientNetworkTlsHandshakeDuration.Record(ctx, time.Since(tlsStart).Seconds(), metric.WithAttributes(
				attribute.String("gcp.client.service", "storage"),
				attribute.String("server.address", host),
			))
		},
		GotFirstResponseByte: func() {
			mrt.instruments.gcsStorageClientOperationFirstByteDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
				attribute.String("gcp.client.service", "storage"),
				attribute.String("rpc.method", method),
				attribute.String("server.address", host),
			))
		},
	}
	ctx = httptrace.WithClientTrace(ctx, trace)
	req = req.WithContext(ctx)

	// active requests
	mrt.instruments.gcsStorageClientActiveRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("gcp.client.service", "storage"),
		attribute.String("rpc.method", method),
		attribute.String("server.address", host),
	))
	defer mrt.instruments.gcsStorageClientActiveRequests.Add(ctx, -1, metric.WithAttributes(
		attribute.String("gcp.client.service", "storage"),
		attribute.String("rpc.method", method),
		attribute.String("server.address", host),
	))

	resp, err := mrt.rt.RoundTrip(req)
	duration := time.Since(start).Seconds()

	statusCode := "error"
	if resp != nil {
		statusCode = strconv.Itoa(resp.StatusCode)
	}

	mrt.instruments.httpClientRequestDuration.Record(ctx, duration, metric.WithAttributes(
		attribute.String("http.request.method", req.Method),
		attribute.String("http.response.status_code", statusCode),
		attribute.String("server.address", host),
		attribute.String("url.template", method),
	))

	return resp, err
}
