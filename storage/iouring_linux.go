//go:build linux

package storage

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Constants for io_uring operations
const (
	sys_io_uring_setup = 425
	sys_io_uring_enter = 426

	ioring_op_read   = 22
	ioring_op_write  = 23
	ioring_enter_getevents = (1 << 0)
)

var enableLinuxIOUring bool

// Struct definitions for io_uring layout
type io_sqring_offsets struct {
	head         uint32
	tail         uint32
	ring_mask    uint32
	ring_entries uint32
	flags        uint32
	array        uint32
	dropped      uint32
	resv1        uint32
	resv2        uint64
}

type io_cqring_offsets struct {
	head         uint32
	tail         uint32
	ring_mask    uint32
	ring_entries uint32
	overflow     uint32
	cqes         uint32
	flags        uint32
	resv         [3]uint32
}

type io_uring_params struct {
	sq_entries     uint32
	cq_entries     uint32
	flags          uint32
	sq_thread_cpu  uint32
	sq_thread_idle uint32
	features       uint32
	wq_fd          uint32
	resv           [3]uint32
	sq_off         io_sqring_offsets
	cq_off         io_cqring_offsets
}

type io_uring_sqe struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	rw_flags    uint32
	user_data   uint64
	buf_index   uint16
	personality uint16
	pad_cgo_0   uint32
	pad         [2]uint64
}

type io_uring_cqe struct {
	user_data uint64
	res       int32
	flags     uint32
}

type ioUringRing struct {
	fd       int
	sqeMmap  []byte
	cqeMmap  []byte
	sqes     []io_uring_sqe
	cqes     []io_uring_cqe
	sqHead   *uint32
	sqTail   *uint32
	sqMask   uint32
	sqArray  []uint32
	cqHead   *uint32
	cqTail   *uint32
	cqMask   uint32
	mu       sync.Mutex

	// Task synchronization channels mapped by SQE idx (0-7)
	chans    [8]chan int32
	shutdown chan struct{}
}

// isIOUringSupported checks if io_uring is supported by the running Linux kernel.
func isIOUringSupported() bool {
	if !enableLinuxIOUring && os.Getenv("STORAGE_ENABLE_IO_URING") == "" {
		return false
	}
	var params io_uring_params
	fd, _, err := syscall.Syscall(sys_io_uring_setup, 8, uintptr(unsafe.Pointer(&params)), 0)
	if err != 0 {
		return false
	}
	syscall.Close(int(fd))
	return true
}

func newIOUringRing(entries uint32) (*ioUringRing, error) {
	var params io_uring_params
	fd, _, errno := syscall.Syscall(sys_io_uring_setup, uintptr(entries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup error: %w", errno)
	}

	// Map Submission Queue Entries (SQEs)
	sqeSize := uintptr(params.sq_entries) * unsafe.Sizeof(io_uring_sqe{})
	sqeMmap, err := unix.Mmap(int(fd), 0x10000000, int(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		syscall.Close(int(fd))
		return nil, fmt.Errorf("sqe mmap error: %w", err)
	}

	// Map Completion Queue Entries (CQEs) & arrays
	cqeSize := uintptr(params.cq_entries) * unsafe.Sizeof(io_uring_cqe{})
	cqeMmap, err := unix.Mmap(int(fd), 0, int(cqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		unix.Munmap(sqeMmap)
		syscall.Close(int(fd))
		return nil, fmt.Errorf("cqe mmap error: %w", err)
	}

	ring := &ioUringRing{
		fd:       int(fd),
		sqeMmap:  sqeMmap,
		cqeMmap:  cqeMmap,
		sqes:     unsafe.Slice((*io_uring_sqe)(unsafe.Pointer(&sqeMmap[0])), params.sq_entries),
		cqes:     unsafe.Slice((*io_uring_cqe)(unsafe.Pointer(&cqeMmap[0])), params.cq_entries),
		sqHead:   (*uint32)(unsafe.Pointer(&sqeMmap[params.sq_off.head])),
		sqTail:   (*uint32)(unsafe.Pointer(&sqeMmap[params.sq_off.tail])),
		sqMask:   *(*uint32)(unsafe.Pointer(&sqeMmap[params.sq_off.ring_mask])),
		cqHead:   (*uint32)(unsafe.Pointer(&cqeMmap[params.cq_off.head])),
		cqTail:   (*uint32)(unsafe.Pointer(&cqeMmap[params.cq_off.tail])),
		cqMask:   *(*uint32)(unsafe.Pointer(&cqeMmap[params.cq_off.ring_mask])),
		shutdown: make(chan struct{}),
	}

	// Map SQ Array
	sqArrayPtr := unsafe.Add(unsafe.Pointer(&sqeMmap[0]), params.sq_off.array)
	ring.sqArray = unsafe.Slice((*uint32)(sqArrayPtr), params.sq_entries)

	// Pre-allocate channels for the 8 entries
	for i := 0; i < len(ring.chans); i++ {
		ring.chans[i] = make(chan int32, 1)
	}

	// Spawn dedicated background CQ reaper thread
	go ring.cqReaper()

	return ring, nil
}

// cqReaper runs in a single background OS thread, waiting for kernel completions
// and waking up waiting read/write goroutines asynchronously.
func (r *ioUringRing) cqReaper() {
	for {
		select {
		case <-r.shutdown:
			return
		default:
			// Block until at least 1 event completes in the kernel
			_, _, errno := syscall.Syscall6(sys_io_uring_enter, uintptr(r.fd), 0, 1, ioring_enter_getevents, 0, 0)
			if errno != 0 && errno != syscall.EINTR {
				time.Sleep(time.Millisecond * 5)
				continue
			}

			r.mu.Lock()
			head := *r.cqHead
			tail := *r.cqTail
			for head != tail {
				idx := head & r.cqMask
				cqe := &r.cqes[idx]
				sqeIdx := cqe.user_data
				res := cqe.res

				// Propagate completion result to the parked goroutine channel
				if sqeIdx < uint64(len(r.chans)) {
					select {
					case r.chans[sqeIdx] <- res:
					default:
					}
				}
				head++
			}
			*r.cqHead = head
			r.mu.Unlock()
		}
	}
}

func (r *ioUringRing) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fd == -1 {
		return nil
	}
	close(r.shutdown)
	unix.Munmap(r.sqeMmap)
	unix.Munmap(r.cqeMmap)
	syscall.Close(r.fd)
	r.fd = -1
	return nil
}

// ioUringConn wraps a standard net.Conn to intercept Reads and Writes with io_uring
type ioUringConn struct {
	net.Conn
	ring   *ioUringRing
	rawFd  int
}

func newIOUringConn(c net.Conn) (net.Conn, error) {
	sysConn, ok := c.(syscall.Conn)
	if !ok {
		return c, nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return c, nil
	}
	var rawFd int
	controlErr := rawConn.Control(func(fd uintptr) {
		rawFd = int(fd)
	})
	if controlErr != nil || rawFd == 0 {
		return c, nil
	}

	ring, err := newIOUringRing(8)
	if err != nil {
		return c, nil // Fallback to standard net.Conn
	}

	return &ioUringConn{
		Conn:  c,
		ring:  ring,
		rawFd: rawFd,
	}, nil
}

func (c *ioUringConn) Read(b []byte) (int, error) {
	// Adaptive Size Gate: standard socket reads for tiny payloads
	if c.ring == nil || len(b) < 256*1024 {
		return c.Conn.Read(b)
	}

	c.ring.mu.Lock()
	tail := *c.ring.sqTail
	idx := tail & c.ring.sqMask
	sqe := &c.ring.sqes[idx]

	*sqe = io_uring_sqe{
		opcode:    ioring_op_read,
		fd:        int32(c.rawFd),
		addr:      uint64(uintptr(unsafe.Pointer(&b[0]))),
		len:       uint32(len(b)),
		user_data: uint64(idx),
	}

	c.ring.sqArray[idx] = idx
	*c.ring.sqTail = tail + 1

	// Drain the channel before submit
	select {
	case <-c.ring.chans[idx]:
	default:
	}
	c.ring.mu.Unlock()

	// Submit SQE asynchronously
	_, _, errno := syscall.Syscall6(sys_io_uring_enter, uintptr(c.ring.fd), 1, 0, 0, 0, 0)
	if errno != 0 && errno != syscall.EINTR {
		return c.Conn.Read(b) // Fallback
	}

	// Park the goroutine waiting for the background CQ reaper to signal us
	res := <-c.ring.chans[idx]

	if res < 0 {
		return 0, syscall.Errno(-res)
	}
	if res == 0 {
		return 0, syscall.EPIPE
	}

	return int(res), nil
}

func (c *ioUringConn) Write(b []byte) (int, error) {
	// Adaptive Size Gate: standard socket writes for tiny payloads
	if c.ring == nil || len(b) < 256*1024 {
		return c.Conn.Write(b)
	}

	c.ring.mu.Lock()
	tail := *c.ring.sqTail
	idx := tail & c.ring.sqMask
	sqe := &c.ring.sqes[idx]

	*sqe = io_uring_sqe{
		opcode:    ioring_op_write,
		fd:        int32(c.rawFd),
		addr:      uint64(uintptr(unsafe.Pointer(&b[0]))),
		len:       uint32(len(b)),
		user_data: uint64(idx),
	}

	c.ring.sqArray[idx] = idx
	*c.ring.sqTail = tail + 1

	// Drain the channel before submit
	select {
	case <-c.ring.chans[idx]:
	default:
	}
	c.ring.mu.Unlock()

	// Submit SQE asynchronously
	_, _, errno := syscall.Syscall6(sys_io_uring_enter, uintptr(c.ring.fd), 1, 0, 0, 0, 0)
	if errno != 0 && errno != syscall.EINTR {
		return c.Conn.Write(b) // Fallback
	}

	// Park the goroutine waiting for the background CQ reaper to signal us
	res := <-c.ring.chans[idx]

	if res < 0 {
		return 0, syscall.Errno(-res)
	}

	return int(res), nil
}

func (c *ioUringConn) Close() error {
	if c.ring != nil {
		c.ring.Close()
	}
	return c.Conn.Close()
}

// customDialer is a high-performance dialer that intercepts network connections
// and registers them with io_uring on Linux.
func customDialer(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if isIOUringSupported() {
		return newIOUringConn(conn)
	}
	return conn, nil
}
