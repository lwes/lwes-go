package lwes

import (
	"io"
	"sync"
)

// Server is the interface for servers that receive inbound span submissions from client.
type Server interface {
	Serve()
	IsServing() bool
	Stop()
	DataChan() <-chan ReadMsg
	// DataRecd(ReadMsg) // must be called by consumer after reading data from the ReadBuf
}

// ReadBuf is a structure that holds the bytes to read into as well as the number of bytes
// that was read. The slice is typically pre-allocated to the max packet size and the buffers
// themselves are polled to avoid memory allocations for every new inbound message.
type readBuf struct {
	buf []byte // contents are the bytes buf[off : len(buf)]
	n   int

	pool *sync.Pool
}

func (r *readBuf) Bytes() []byte {
	return r.buf[:r.n]
}

func (r *readBuf) ReadFrom(reader io.Reader) (n int64, err error) {
	var n_ int
	n_, err = reader.Read(r.buf)
	r.n = int(n_)
	return int64(n_), err
}

func (r *readBuf) Write(p []byte) (n int, err error) {
	// r.n = copy(p, r.buf)
	// return r.n, nil
	return 0, io.EOF
}

func (r *readBuf) Done() {
	r.pool.Put(r)
}

func newReadMsg(pool *sync.Pool) ReadMsg {
	readBuf := pool.Get().(*readBuf)
	readBuf.pool = pool
	return readBuf
}

type ReadMsg interface {
	io.ReaderFrom
	io.Writer
	Bytes() []byte
	Done() // return this msg (back to pool)
}
