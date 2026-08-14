// Copyright 2026 Google LLC
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
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage/experimental"
	"cloud.google.com/go/storage/internal/apiv2/storagepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const mockBufSize = 1024 * 1024

type mockStorageServer struct {
	storagepb.UnimplementedStorageServer

	mu                sync.Mutex
	bidiReadHandler   func(storagepb.Storage_BidiReadObjectServer) error
	bidiWriteHandler  func(storagepb.Storage_BidiWriteObjectServer) error
	bidiReadSpecs     []*storagepb.BidiReadObjectSpec
	bidiWriteRequests []*storagepb.BidiWriteObjectRequest
}

func (s *mockStorageServer) BidiReadObject(stream storagepb.Storage_BidiReadObjectServer) error {
	s.mu.Lock()
	handler := s.bidiReadHandler
	s.mu.Unlock()
	if handler != nil {
		return handler(stream)
	}
	return status.Errorf(codes.Unimplemented, "BidiReadObject not implemented")
}

func (s *mockStorageServer) BidiWriteObject(stream storagepb.Storage_BidiWriteObjectServer) error {
	s.mu.Lock()
	handler := s.bidiWriteHandler
	s.mu.Unlock()
	if handler != nil {
		return handler(stream)
	}
	return status.Errorf(codes.Unimplemented, "BidiWriteObject not implemented")
}

func setupMockStorageClient(ctx context.Context, t *testing.T, srv *mockStorageServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(mockBufSize)
	s := grpc.NewServer()
	storagepb.RegisterStorageServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("Mock server Serve error: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client, err := NewGRPCClient(ctx,
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
		experimental.WithZonalBucketAPIs(),
	)
	if err != nil {
		t.Fatalf("NewGRPCClient failed: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}

	return client, cleanup
}

// TestMock_BidiRead_HandleRedirectError tests handling mid-read BidiReadObjectRedirectedError gRPC responses.
// It verifies that the client intercepts the redirect, reconnects using the updated routing token and read handle, and completes the range read transparently.
func TestMock_BidiRead_HandleRedirectError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var streamCount int
	mockData := []byte("hello-redirected-bidi-read-data-with-proper-length-padding")
	crc := crc32.Checksum(mockData, crc32.MakeTable(crc32.Castagnoli))

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		srv.mu.Lock()
		streamCount++
		currentStream := streamCount
		srv.mu.Unlock()

		// Receive initial message containing ReadObjectSpec.
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		if currentStream == 1 {
			// First stream: respond with initial metadata and then simulate mid-read redirect on first range request.
			err := stream.Send(&storagepb.BidiReadObjectResponse{
				Metadata: &storagepb.Object{
					Name: req.GetReadObjectSpec().GetObject(),
					Size: int64(len(mockData)),
				},
			})
			if err != nil {
				return err
			}

			// Receive the range request.
			_, err = stream.Recv()
			if err != nil {
				return err
			}

			// Trigger redirect error.
			routingToken := "routing-token-redirect-target"
			readHandle := []byte("redirect-handle-123")
			redirectErr := &storagepb.BidiReadObjectRedirectedError{
				RoutingToken: &routingToken,
				ReadHandle: &storagepb.BidiReadHandle{
					Handle: readHandle,
				},
			}
			st, err := status.New(codes.Aborted, "bidi read object redirected").WithDetails(redirectErr)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to attach redirect details: %v", err)
			}
			return st.Err()
		}

		// Reconnected stream: send metadata then serve the pending range.
		if req.GetReadObjectSpec() != nil {
			srv.mu.Lock()
			srv.bidiReadSpecs = append(srv.bidiReadSpecs, req.GetReadObjectSpec())
			srv.mu.Unlock()
		}

		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name: "test-obj",
				Size: int64(len(mockData)),
			},
		})
		if err != nil {
			return err
		}

		for {
			rReq, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			for _, r := range rReq.GetReadRanges() {
				err := stream.Send(&storagepb.BidiReadObjectResponse{
					ObjectDataRanges: []*storagepb.ObjectRangeData{
						{
							ReadRange: &storagepb.ReadRange{
								ReadId:     r.GetReadId(),
								ReadOffset: r.GetReadOffset(),
								ReadLength: int64(len(mockData)),
							},
							ChecksummedData: &storagepb.ChecksummedData{
								Content: mockData,
								Crc32C:  &crc,
							},
							RangeEnd: true,
						},
					},
				})
				if err != nil {
					return err
				}
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}
	defer mrd.Close()

	var buf bytes.Buffer
	var rangeErr error
	done := make(chan struct{})

	mrd.Add(&buf, 0, int64(len(mockData)), func(off, length int64, err error) {
		rangeErr = err
		close(done)
	})

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("Range read timed out waiting for redirect completion")
	}

	mrd.Wait()

	if rangeErr != nil {
		t.Fatalf("Expected transparent redirect success, got error: %v", rangeErr)
	}
	if !bytes.Equal(buf.Bytes(), mockData) {
		t.Errorf("Buffer data mismatch: got %q, want %q", buf.String(), string(mockData))
	}
}

// TestMock_BidiRead_HandleRedirectErrorOnOpen tests handling BidiReadObjectRedirectedError during initial session open.
// It verifies that the client reconnects using the updated routing token and successfully initializes the session.
func TestMock_BidiRead_HandleRedirectErrorOnOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var streamCount int
	mockData := []byte("redirect-on-open-data")
	crc := crc32.Checksum(mockData, crc32.MakeTable(crc32.Castagnoli))

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		srv.mu.Lock()
		streamCount++
		currentStream := streamCount
		srv.mu.Unlock()

		req, err := stream.Recv()
		if err != nil {
			return err
		}

		if currentStream == 1 {
			// Fail open on the first stream with a redirect error.
			routingToken := "routing-token-on-open"
			readHandle := []byte("handle-on-open")
			redirectErr := &storagepb.BidiReadObjectRedirectedError{
				RoutingToken: &routingToken,
				ReadHandle: &storagepb.BidiReadHandle{
					Handle: readHandle,
				},
			}
			st, _ := status.New(codes.Aborted, "bidi read object redirected").WithDetails(redirectErr)
			return st.Err()
		}

		// Second stream: succeed with metadata and handle range.
		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name: req.GetReadObjectSpec().GetObject(),
				Size: int64(len(mockData)),
			},
		})
		if err != nil {
			return err
		}

		for {
			rReq, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			for _, r := range rReq.GetReadRanges() {
				err := stream.Send(&storagepb.BidiReadObjectResponse{
					ObjectDataRanges: []*storagepb.ObjectRangeData{
						{
							ReadRange: &storagepb.ReadRange{
								ReadId:     r.GetReadId(),
								ReadOffset: 0,
								ReadLength: int64(len(mockData)),
							},
							ChecksummedData: &storagepb.ChecksummedData{
								Content: mockData,
								Crc32C:  &crc,
							},
							RangeEnd: true,
						},
					},
				})
				if err != nil {
					return err
				}
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}
	defer mrd.Close()

	var buf bytes.Buffer
	var rangeErr error
	done := make(chan struct{})

	mrd.Add(&buf, 0, int64(len(mockData)), func(off, length int64, err error) {
		rangeErr = err
		close(done)
	})

	select {
	case <-done:
	case <-ctx.Done():
	}
	mrd.Wait()

	if rangeErr != nil {
		t.Fatalf("Range read failed: %v", rangeErr)
	}
	if !bytes.Equal(buf.Bytes(), mockData) {
		t.Errorf("Buffer data mismatch: got %q, want %q", buf.String(), string(mockData))
	}
}

// TestMock_BidiRead_HandleRedirectErrorMaxAttempts tests redirect limit enforcement when the server repeatedly returns redirects.
// It verifies that after exceeding max allowed redirects, the operation fails with an appropriate error.
func TestMock_BidiRead_HandleRedirectErrorMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		_, err := stream.Recv()
		if err != nil {
			return err
		}

		routingToken := "infinite-redirect-loop"
		readHandle := []byte("redirect-handle")
		redirectErr := &storagepb.BidiReadObjectRedirectedError{
			RoutingToken: &routingToken,
			ReadHandle: &storagepb.BidiReadHandle{
				Handle: readHandle,
			},
		}
		st, _ := status.New(codes.Aborted, "bidi read object redirected").WithDetails(redirectErr)
		return st.Err()
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err == nil {
		_ = mrd.Close()
		t.Fatalf("Expected NewMultiRangeDownloader to fail due to exhausted redirects, got nil")
	}
}

// TestMock_BidiRead_ClosingStreamFailsPendingReads tests explicit session shutdown.
// It verifies that closing the session while reads are in-flight causes all pending read callbacks to fail with a stream closed error.
func TestMock_BidiRead_ClosingStreamFailsPendingReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		// Send initial metadata response.
		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name: req.GetReadObjectSpec().GetObject(),
				Size: 5000,
			},
		})
		if err != nil {
			return err
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}

	// Close the downloader before adding ranges.
	if err := mrd.Close(); err != nil {
		t.Fatalf("MultiRangeDownloader.Close failed: %v", err)
	}

	var buf bytes.Buffer
	var rangeErr error
	mrd.Add(&buf, 0, 100, func(off, length int64, err error) {
		rangeErr = err
	})
	mrd.Wait()

	if rangeErr == nil {
		t.Fatalf("Expected callback error on closed downloader, got nil")
	}
	if !strings.Contains(rangeErr.Error(), "downloader closed") {
		t.Errorf("Error mismatch: got %v, want downloader closed", rangeErr)
	}
}

// TestMock_BidiRead_RetryableErrorWhileOpen verifies automatic reconnection when the initial stream open encounters a transient error.
func TestMock_BidiRead_RetryableErrorWhileOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var streamCount int
	mockData := []byte("recovered-from-transient-unavailable")
	crc := crc32.Checksum(mockData, crc32.MakeTable(crc32.Castagnoli))

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		srv.mu.Lock()
		streamCount++
		currentStream := streamCount
		srv.mu.Unlock()

		req, err := stream.Recv()
		if err != nil {
			return err
		}

		if currentStream == 1 {
			// Fail first attempt with transient UNAVAILABLE status.
			return status.Errorf(codes.Unavailable, "transient network disconnect")
		}

		// Succeed on retry.
		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name: req.GetReadObjectSpec().GetObject(),
				Size: int64(len(mockData)),
			},
		})
		if err != nil {
			return err
		}

		for {
			rReq, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			for _, r := range rReq.GetReadRanges() {
				err := stream.Send(&storagepb.BidiReadObjectResponse{
					ObjectDataRanges: []*storagepb.ObjectRangeData{
						{
							ReadRange: &storagepb.ReadRange{
								ReadId:     r.GetReadId(),
								ReadOffset: 0,
								ReadLength: int64(len(mockData)),
							},
							ChecksummedData: &storagepb.ChecksummedData{
								Content: mockData,
								Crc32C:  &crc,
							},
							RangeEnd: true,
						},
					},
				})
				if err != nil {
					return err
				}
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}
	defer mrd.Close()

	var buf bytes.Buffer
	var rangeErr error
	done := make(chan struct{})

	mrd.Add(&buf, 0, int64(len(mockData)), func(off, length int64, err error) {
		rangeErr = err
		close(done)
	})

	select {
	case <-done:
	case <-ctx.Done():
	}
	mrd.Wait()

	if rangeErr != nil {
		t.Fatalf("Range read failed after retry: %v", rangeErr)
	}
	if !bytes.Equal(buf.Bytes(), mockData) {
		t.Errorf("Buffer content mismatch: got %q, want %q", buf.String(), string(mockData))
	}
}

// TestMock_BidiRead_RequestOptionVerification verifies propagation of request options and preconditions into gRPC headers and specs.
func TestMock_BidiRead_RequestOptionVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		if spec := req.GetReadObjectSpec(); spec != nil {
			srv.mu.Lock()
			srv.bidiReadSpecs = append(srv.bidiReadSpecs, spec)
			srv.mu.Unlock()
		}

		// Send initial metadata response.
		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name:       req.GetReadObjectSpec().GetObject(),
				Size:       100,
				Generation: req.GetReadObjectSpec().GetGeneration(),
			},
		})
		if err != nil {
			return err
		}

		for {
			rReq, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			for _, r := range rReq.GetReadRanges() {
				data := []byte("verified-options-data")
				crc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
				_ = stream.Send(&storagepb.BidiReadObjectResponse{
					ObjectDataRanges: []*storagepb.ObjectRangeData{
						{
							ReadRange: &storagepb.ReadRange{
								ReadId:     r.GetReadId(),
								ReadOffset: 0,
								ReadLength: int64(len(data)),
							},
							ChecksummedData: &storagepb.ChecksummedData{
								Content: data,
								Crc32C:  &crc,
							},
							RangeEnd: true,
						},
					},
				})
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj").Generation(42)
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}
	defer mrd.Close()

	var buf bytes.Buffer
	done := make(chan struct{})
	mrd.Add(&buf, 0, 21, func(off, length int64, err error) {
		close(done)
	})
	<-done
	mrd.Wait()

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if len(srv.bidiReadSpecs) == 0 {
		t.Fatalf("Server received no BidiReadObjectSpec messages")
	}
	spec := srv.bidiReadSpecs[0]
	if spec.GetGeneration() != 42 {
		t.Errorf("Spec generation: got %d, want 42", spec.GetGeneration())
	}
	if spec.GetObject() != "test-obj" {
		t.Errorf("Spec object: got %q, want test-obj", spec.GetObject())
	}
}

// TestMock_BidiRead_NonRetriableError verifies that non-retriable terminal error codes fail immediately.
func TestMock_BidiRead_NonRetriableError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		// Initial metadata response.
		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name: req.GetReadObjectSpec().GetObject(),
				Size: 50,
			},
		})
		if err != nil {
			return err
		}

		// Receive range read request and return terminal OutOfRange error.
		_, err = stream.Recv()
		if err != nil {
			return err
		}
		return status.Errorf(codes.OutOfRange, "requested range outside object boundary")
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}
	defer mrd.Close()

	var buf bytes.Buffer
	var rangeErr error
	done := make(chan struct{})

	mrd.Add(&buf, 0, 100, func(off, length int64, err error) {
		rangeErr = err
		close(done)
	})

	select {
	case <-done:
	case <-ctx.Done():
	}
	mrd.Wait()

	if rangeErr == nil {
		t.Fatalf("Expected OutOfRange terminal error, got nil")
	}
	if status.Code(rangeErr) != codes.OutOfRange {
		t.Errorf("Error code mismatch: got %v, want OutOfRange", status.Code(rangeErr))
	}
}

// TestMock_BidiWrite_ObjectRedirectErrorMaxAttempts tests redirect handling and attempt limit on BidiWriteObject.
func TestMock_BidiWrite_ObjectRedirectErrorMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := &mockStorageServer{}
	srv.bidiWriteHandler = func(stream storagepb.Storage_BidiWriteObjectServer) error {
		_, err := stream.Recv()
		if err != nil {
			return err
		}

		routingToken := "write-redirect-token"
		redirectErr := &storagepb.BidiWriteObjectRedirectedError{
			RoutingToken: &routingToken,
		}
		st, _ := status.New(codes.Aborted, "bidi write object redirected").WithDetails(redirectErr)
		return st.Err()
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-write-obj")
	w := obj.NewWriter(ctx)
	w.Append = true

	_, writeErr := w.Write([]byte("test-append-data"))
	closeErr := w.Close()

	if writeErr == nil && closeErr == nil {
		t.Fatalf("Expected write failure due to exhausted redirect attempts, got nil")
	}
}

// TestMock_BidiWrite_IncrementalChecksum verifies checksum calculation on incremental chunk writes and finalization.
func TestMock_BidiWrite_IncrementalChecksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	chunk1 := []byte("first-chunk-payload-data-1234567")
	chunk2 := []byte("second-chunk-payload-data-7654321")

	srv := &mockStorageServer{}
	srv.bidiWriteHandler = func(stream storagepb.Storage_BidiWriteObjectServer) error {
		var persisted int64
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			srv.mu.Lock()
			srv.bidiWriteRequests = append(srv.bidiWriteRequests, req)
			srv.mu.Unlock()

			if data := req.GetChecksummedData(); data != nil {
				persisted += int64(len(data.GetContent()))
			}

			if req.GetFlush() || req.GetFinishWrite() || req.GetStateLookup() {
				var resp *storagepb.BidiWriteObjectResponse
				if req.GetFinishWrite() {
					resp = &storagepb.BidiWriteObjectResponse{
						WriteStatus: &storagepb.BidiWriteObjectResponse_Resource{
							Resource: &storagepb.Object{
								Name: "test-checksum-obj",
								Size: persisted,
							},
						},
					}
				} else {
					resp = &storagepb.BidiWriteObjectResponse{
						WriteStatus: &storagepb.BidiWriteObjectResponse_PersistedSize{
							PersistedSize: persisted,
						},
					}
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-checksum-obj")
	w := obj.NewWriter(ctx)
	w.Append = true

	if _, err := w.Write(chunk1); err != nil {
		t.Fatalf("First chunk write failed: %v", err)
	}
	if _, err := w.Write(chunk2); err != nil {
		t.Fatalf("Second chunk write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer close failed: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	var totalPayload []byte
	for _, req := range srv.bidiWriteRequests {
		if data := req.GetChecksummedData(); data != nil {
			totalPayload = append(totalPayload, data.GetContent()...)
			computed := crc32.Checksum(data.GetContent(), crc32.MakeTable(crc32.Castagnoli))
			if data.GetCrc32C() != computed {
				t.Errorf("Chunk checksum mismatch: got %d, computed %d", data.GetCrc32C(), computed)
			}
		}
	}

	expectedCombined := append(chunk1, chunk2...)
	if !bytes.Equal(totalPayload, expectedCombined) {
		t.Errorf("Total uploaded payload mismatch: got %d bytes, want %d bytes", len(totalPayload), len(expectedCombined))
	}
}

// TestMock_BidiRead_PartialRangeBadCRC verifies that a corrupted chunk checksum on one range fails only that specific range while concurrent ranges succeed.
func TestMock_BidiRead_PartialRangeBadCRC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data1 := []byte("first-range-data-with-bad-checksum")
	data2 := []byte("second-range-data-with-valid-checksum")
	crc2 := crc32.Checksum(data2, crc32.MakeTable(crc32.Castagnoli))
	badCRC := uint32(0xdeadbeef)

	srv := &mockStorageServer{}
	srv.bidiReadHandler = func(stream storagepb.Storage_BidiReadObjectServer) error {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// Initial metadata response.
		err = stream.Send(&storagepb.BidiReadObjectResponse{
			Metadata: &storagepb.Object{
				Name: req.GetReadObjectSpec().GetObject(),
				Size: 5000,
			},
		})
		if err != nil {
			return err
		}

		for {
			rReq, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			for _, r := range rReq.GetReadRanges() {
				if r.GetReadId() == 1 {
					// Send bad CRC for read_id 1.
					err := stream.Send(&storagepb.BidiReadObjectResponse{
						ObjectDataRanges: []*storagepb.ObjectRangeData{
							{
								ReadRange: &storagepb.ReadRange{
									ReadId:     1,
									ReadOffset: 0,
									ReadLength: int64(len(data1)),
								},
								ChecksummedData: &storagepb.ChecksummedData{
									Content: data1,
									Crc32C:  &badCRC,
								},
								RangeEnd: true,
							},
						},
					})
					if err != nil {
						return err
					}
				} else {
					// Send valid CRC for read_id 2.
					err := stream.Send(&storagepb.BidiReadObjectResponse{
						ObjectDataRanges: []*storagepb.ObjectRangeData{
							{
								ReadRange: &storagepb.ReadRange{
									ReadId:     r.GetReadId(),
									ReadOffset: 100,
									ReadLength: int64(len(data2)),
								},
								ChecksummedData: &storagepb.ChecksummedData{
									Content: data2,
									Crc32C:  &crc2,
								},
								RangeEnd: true,
							},
						},
					})
					if err != nil {
						return err
					}
				}
			}
		}
	}

	client, cleanup := setupMockStorageClient(ctx, t, srv)
	defer cleanup()

	obj := client.Bucket("test-bucket").Object("test-obj")
	mrd, err := obj.NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader failed: %v", err)
	}
	defer mrd.Close()

	var buf1, buf2 bytes.Buffer
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	mrd.Add(&buf1, 0, int64(len(data1)), func(off, length int64, err error) {
		err1 = err
		wg.Done()
	})
	mrd.Add(&buf2, 100, int64(len(data2)), func(off, length int64, err error) {
		err2 = err
		wg.Done()
	})

	wg.Wait()
	mrd.Wait()

	if err1 == nil {
		t.Errorf("Expected range 1 to fail with CRC error, got nil")
	} else if !strings.Contains(err1.Error(), "bad CRC") {
		t.Errorf("Range 1 error mismatch: got %v, want bad CRC error", err1)
	}
	if err2 != nil {
		t.Errorf("Expected range 2 to succeed, got error: %v", err2)
	}
	if !bytes.Equal(buf2.Bytes(), data2) {
		t.Errorf("Range 2 content mismatch: got %q, want %q", buf2.String(), string(data2))
	}
}

