// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"go.opentelemetry.io/otel/metric"
)

type metricInstruments struct {
	rpcClientCallDuration                     metric.Float64Histogram
	httpClientRequestDuration                 metric.Float64Histogram
	gcsStorageClientOperationDuration         metric.Float64Histogram
	gcsStorageClientOperationAttemptDuration  metric.Float64Histogram
	gcsStorageClientOperations                metric.Int64Counter
	gcsStorageClientAttempts                  metric.Int64Counter
	gcsStorageClientErrors                    metric.Int64Counter
	gcsStorageClientRequestBodySize           metric.Int64Histogram
	gcsStorageClientResponseBodySize          metric.Int64Histogram
	gcsStorageClientOperationFirstByteDuration metric.Float64Histogram
	gcsStorageClientGfeDuration               metric.Float64Histogram
	gcsStorageClientStallDuration             metric.Float64Histogram
	gcsStorageClientOperationSlowStreamThroughput metric.Float64Histogram
	gcsStorageClientActiveRequests            metric.Int64UpDownCounter
	gcsStorageClientNetworkDnsLookupDuration  metric.Float64Histogram
	gcsStorageClientNetworkTcpConnectDuration metric.Float64Histogram
	gcsStorageClientNetworkTlsHandshakeDuration metric.Float64Histogram
	gcsStorageClientConnectionPoolUtilization metric.Float64Gauge
	gcsStorageClientChecksumMismatchCount     metric.Int64Counter
	gcsStorageClientAuthCredentialRefreshDuration metric.Float64Histogram
}

func initializeMetrics(meter metric.Meter) (*metricInstruments, error) {
	var err error
	var insts metricInstruments

	insts.rpcClientCallDuration, err = meter.Float64Histogram("rpc.client.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of one gRPC request."))
	if err != nil {
		return nil, err
	}

	insts.httpClientRequestDuration, err = meter.Float64Histogram("http.client.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of one HTTP client request."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientOperationDuration, err = meter.Float64Histogram("gcp.client.request.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of one GCS SDK API request including retries."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientOperationAttemptDuration, err = meter.Float64Histogram("gcp.storage.client.operation.attempt.duration",
		metric.WithUnit("s"), metric.WithDescription("Physical Latency: Per-attempt latency (excluding retry overhead)."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientOperations, err = meter.Int64Counter("gcp.storage.client.operations",
		metric.WithUnit("{call}"), metric.WithDescription("Measures overall application success/failure rate."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientAttempts, err = meter.Int64Counter("gcp.storage.client.attempts",
		metric.WithUnit("{attempt}"), metric.WithDescription("Direct visibility into retry volume."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientErrors, err = meter.Int64Counter("gcp.storage.client.errors",
		metric.WithUnit("{error}"), metric.WithDescription("Unified Error Tracker: Covers connectivity, timeouts, and API errors."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientRequestBodySize, err = meter.Int64Histogram("gcp.storage.client.request.body.size",
		metric.WithUnit("By"), metric.WithDescription("Application-level bytes uploaded."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientResponseBodySize, err = meter.Int64Histogram("gcp.storage.client.response.body.size",
		metric.WithUnit("By"), metric.WithDescription("Application-level bytes downloaded."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientOperationFirstByteDuration, err = meter.Float64Histogram("gcp.storage.client.operation.ttfb",
		metric.WithUnit("s"), metric.WithDescription("Time to first response byte; critical for streaming and AI/ML pipelines."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientGfeDuration, err = meter.Float64Histogram("gcp.storage.client.gfe.duration",
		metric.WithUnit("s"), metric.WithDescription("Latency from GFE receipt to first byte."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientStallDuration, err = meter.Float64Histogram("gcp.storage.client.stall.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of stalled timeouts when retries are triggered."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientOperationSlowStreamThroughput, err = meter.Float64Histogram("gcp.storage.client.operation.slow_stream_throughput",
		metric.WithUnit("By/s"), metric.WithDescription("Distribution of throughput levels triggering \"slow stream\" failure."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientActiveRequests, err = meter.Int64UpDownCounter("gcp.storage.client.active_requests",
		metric.WithUnit("{request}"), metric.WithDescription("In-flight requests at any time."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientNetworkDnsLookupDuration, err = meter.Float64Histogram("gcp.storage.client.network.dns.lookup.duration",
		metric.WithUnit("s"), metric.WithDescription("Time taken to perform a DNS lookup."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientNetworkTcpConnectDuration, err = meter.Float64Histogram("gcp.storage.client.network.tcp.connect.duration",
		metric.WithUnit("s"), metric.WithDescription("Time taken to establish a TCP connection."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientNetworkTlsHandshakeDuration, err = meter.Float64Histogram("gcp.storage.client.network.tls.handshake.duration",
		metric.WithUnit("s"), metric.WithDescription("Time taken to perform a TLS handshake."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientConnectionPoolUtilization, err = meter.Float64Gauge("gcp.storage.client.connection.pool.utilization",
		metric.WithUnit("1"), metric.WithDescription("Utilization of the connection pool."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientChecksumMismatchCount, err = meter.Int64Counter("gcp.storage.client.checksum.mismatch.count",
		metric.WithUnit("{error}"), metric.WithDescription("Number of checksum mismatches detected."))
	if err != nil {
		return nil, err
	}

	insts.gcsStorageClientAuthCredentialRefreshDuration, err = meter.Float64Histogram("gcp.storage.client.auth.credential_refresh.duration",
		metric.WithUnit("s"), metric.WithDescription("Time taken to refresh auth credentials."))
	if err != nil {
		return nil, err
	}

	return &insts, nil
}
