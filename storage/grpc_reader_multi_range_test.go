package storage

import (
	"bytes"
	"context"
	"testing"
	"time"

	"cloud.google.com/go/storage/internal/apiv2/storagepb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockBidiStreamSession struct {
	sendReqs   []*storagepb.BidiReadObjectRequest
	shutdownC  int
}

func (m *mockBidiStreamSession) SendRequest(req *storagepb.BidiReadObjectRequest) {
	m.sendReqs = append(m.sendReqs, req)
}

func (m *mockBidiStreamSession) Shutdown() {
	m.shutdownC++
}

func TestMultiRangeDownloaderManager_RetryCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxAttempts := 3
	s := &settings{
		retry: &retryConfig{
			maxAttempts: &maxAttempts,
			backoff: &gax.Backoff{
				Initial: time.Millisecond,
				Max:     time.Millisecond,
			},
			shouldRetry: func(err error, rc *RetryContext) bool {
				return status.Code(err) == codes.Unavailable
			},
		},
	}

	m := &multiRangeDownloaderManager{
		ctx:          ctx,
		cancel:       cancel,
		settings:     s,
		params:       &newMultiRangeDownloaderParams{},
		cmds:         make(chan mrdCommand, mrdCommandChannelSize),
		sessionResps: make(chan mrdSessionResult, mrdResponseChannelSize),
		addStreams:   make(chan mrdCommand, mrdAddStreamsChannelSize),
		streams:      make(map[int]*mrdStream),
		attrsReady:   make(chan struct{}),
	}

	m.readSpec = &storagepb.BidiReadObjectSpec{}

	// Setup a stream
	streamID := 1
	m.streams[streamID] = &mrdStream{
		id:            streamID,
		pendingRanges: make(map[int64]*rangeRequest),
	}

	var callbackErr error
	doneC := make(chan struct{})
	req := &rangeRequest{
		output: new(bytes.Buffer),
		offset: 0,
		length: 100,
		origOffset: 0,
		origLength: 100,
		readID: 1,
		attempts: 1, // first attempt
		callback: func(offset, length int64, err error) {
			callbackErr = err
			close(doneC)
		},
	}
	m.streams[streamID].pendingRanges[req.readID] = req

	// Simulate failures hitting the limit
	retryableErr := status.Error(codes.Unavailable, "try again")

	// Fail stream attempt 1
	m.handleStreamEnd(mrdSessionResult{err: retryableErr}, m.streams[streamID])
	if callbackErr != nil {
		t.Fatalf("Expected no callback error after attempt 1, got: %v", callbackErr)
	}
	if req.attempts != 2 {
		t.Fatalf("Expected attempts to be 2, got: %v", req.attempts)
	}

	// Fail stream attempt 2
	m.handleStreamEnd(mrdSessionResult{err: retryableErr}, m.streams[streamID])
	if callbackErr != nil {
		t.Fatalf("Expected no callback error after attempt 2, got: %v", callbackErr)
	}
	if req.attempts != 3 {
		t.Fatalf("Expected attempts to be 3, got: %v", req.attempts)
	}

	// Fail stream attempt 3 -> should trigger the cap
	m.handleStreamEnd(mrdSessionResult{err: retryableErr}, m.streams[streamID])

	select {
	case <-doneC:
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for callback error")
	}

	if callbackErr == nil {
		t.Fatalf("Expected callback error on attempt 3 due to maxAttempts cap, got nil. Request attempts is %v, req completed is %v", req.attempts, req.completed)
	}

	expectedErrSubstring := "retry failed after 3 attempts"
	if !bytes.Contains([]byte(callbackErr.Error()), []byte(expectedErrSubstring)) {
		t.Fatalf("Expected error to contain %q, got: %v", expectedErrSubstring, callbackErr)
	}
	if !req.completed {
		t.Fatalf("Expected req.completed to be true after failing the limit")
	}
}
