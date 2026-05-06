package storage

import (
	"net"
	"os"
	"testing"

	"cloud.google.com/go/storage/experimental"
)

func TestIOUring_OptionConfiguration(t *testing.T) {
	opt := experimental.WithLinuxIOUring()
	if opt == nil {
		t.Fatalf("experimental.WithLinuxIOUring returned nil")
	}

	conf := newStorageConfig(opt)
	if !conf.grpcLinuxIOUring {
		t.Errorf("storageConfig.grpcLinuxIOUring got false, want true after WithLinuxIOUring option")
	}
}

func TestIOUring_CapabilitySupported(t *testing.T) {
	// Backup initial states
	origEnv := os.Getenv("STORAGE_ENABLE_IO_URING")
	origEnable := enableLinuxIOUring
	defer func() {
		os.Setenv("STORAGE_ENABLE_IO_URING", origEnv)
		enableLinuxIOUring = origEnable
	}()

	// Test 1: Disabled by default
	os.Setenv("STORAGE_ENABLE_IO_URING", "")
	enableLinuxIOUring = false
	if isIOUringSupported() {
		t.Errorf("isIOUringSupported got true, want false by default")
	}

	// Test 2: Enabled by experimental option flag
	enableLinuxIOUring = true
	// This will run the setup syscall which might return false on non-Linux, so we only verify
	// that it doesn't panic and works properly
	_ = isIOUringSupported()
}

type mockConn struct {
	net.Conn
	writeCalled bool
	writeBytes  int
}

func (m *mockConn) Write(b []byte) (int, error) {
	m.writeCalled = true
	m.writeBytes = len(b)
	return len(b), nil
}

func (m *mockConn) Read(b []byte) (int, error) {
	return len(b), nil
}

func TestIOUring_AdaptiveSizeGate(t *testing.T) {
	baseConn := &mockConn{}
	c := &ioUringConn{
		Conn:  baseConn,
		ring:  nil, // Nil ring forces fallback or standard Conn routing
		rawFd: 0,
	}

	// Test 1: Tiny payload (100 bytes) should route directly to base connection Write
	payload := make([]byte, 100)
	n, err := c.Write(payload)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(payload) {
		t.Errorf("Write returned %d, want %d", n, len(payload))
	}
	if !baseConn.writeCalled {
		t.Errorf("Write did not route directly to standard Connection for tiny payload")
	}

	// Test 2: Large payload (300KB) should trigger io_uring path.
	// If ring is nil, it gracefully falls back to standard Connection Write.
	baseConn.writeCalled = false
	largePayload := make([]byte, 300*1024)
	n, err = c.Write(largePayload)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(largePayload) {
		t.Errorf("Write returned %d, want %d", n, len(largePayload))
	}
	if !baseConn.writeCalled {
		t.Errorf("Write failed to fallback gracefully to standard Connection when ring is nil")
	}
}
