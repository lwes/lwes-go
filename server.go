package lwes

import (
	// "bytes"
	"io"
	"sync"
	"time"
	"net"
)

// Server is the interface for servers that receive inbound span submissions from client.
type Server interface {
	Serve()          // start serving
	IsServing() bool // check if the server is still in serving mode
	Stop()           // stop the server
	Wait()           // wait till the server is stopped
	DataChan() <-chan *readBuf

	// Addr returns server's network address.
	Addr() net.Addr
	// DataRecd(ReadMsg) // must be called by consumer after reading data from the ReadBuf

	WaitLwesMode(num_workers int) <-chan *LwesEvent
	EnableMetricsReport(time.Duration, func(string, interface{}))
}

// ReadBuf is a structure that holds the bytes to read into as well as the number of bytes
// that was read. The slice is typically pre-allocated to the max packet size and the buffers
// themselves are polled to avoid memory allocations for every new inbound message.
type readBuf struct {
	buf  []byte
	n    int
	pool *sync.Pool
}

func (b *readBuf) Done() {
	b.n = 0
	b.pool.Put(b)
}

// overwrite the ReadFrom to read one packet only
func (b *readBuf) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(b.buf[b.n:])
	if err != nil {
		return 0, nil
	}
	b.n = b.n + n
	return int64(n), nil
}

func (b *readBuf) Write(p []byte) (n int, err error) {
	return copy(b.buf[b.n:], p), nil
}

func (b *readBuf) Bytes() []byte { return b.buf[:b.n] }

func NewFixedBuffer(pool *sync.Pool, size int) *readBuf {
	if x := pool.Get(); x != nil {
		return x.(*readBuf)
	} else {
		return &readBuf{
			buf:  make([]byte, size),
			pool: pool,
		}
	}
}
