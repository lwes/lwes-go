package lwes

import (
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

const (
	/* maximum datagram size for UDP is 65535 (0xffff) minus
	    transport layer overhead (20 bytes for IP header, and 8 bytes for UDP header), so this value
	   should be 65535 - 28 = 65507
	*/
	MAX_MSG_SIZE = 65535 - 20 - 8
	// MAX_QUEUED_ELEMENTS = 10000

	MAX_PACKET_SIZE = 64 * 1024 // max size of single UDP packet is 64KB - headersize
	SO_RCVBUF_SIZE  = 16 * 1024 * 1024
)

type bufferedServer struct {
	multi_addrport string
	dataChan       chan ReadMsg
	maxPacketSize  int
	maxQueueSize   int
	queueSize      int64
	serving        uint32
	transport      io.ReadCloser
	readBufPool    *sync.Pool
	// started        chan struct{}
	metrics struct {
		QueueSize        int
		PacketSize       int
		PacketsDropped   int
		PacketsProcessed int
		ReadError        int
	}
}

func Listen(multi_addrport string,
	maxQueueSize int, maxPacketSize int) (Server, error) {

	addr, err := net.ResolveUDPAddr("udp", multi_addrport)
	if err != nil {
		log.Println("failed to resolve:", multi_addrport)
		return nil, err
	}
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Println("failed to listen multicast udp:", addr)
		return nil, err
	}
	log.Println("start listening on:", multi_addrport)
	// s.transport = conn

	dataChan := make(chan ReadMsg, maxQueueSize)

	readBufPool := &sync.Pool{
		New: func() interface{} { return &readBuf{buf: make([]byte, maxPacketSize)} },
	}

	s := &bufferedServer{
		multi_addrport: multi_addrport,
		dataChan:       dataChan,
		transport:      conn,
		maxQueueSize:   maxQueueSize,
		maxPacketSize:  maxPacketSize,
		readBufPool:    readBufPool,
		// started:        make(chan struct{}),
	}

	go s.Serve()

	// wait it started before returning
	// <-s.started

	return s, nil
}

func (s *bufferedServer) Serve() {
	if atomic.SwapUint32(&s.serving, 1) != 0 {
		return
	}
	// s.started <- struct{}{}

	for s.IsServing() {
		readBuf := newReadMsg(s.readBufPool)
		_, err := io.Copy(readBuf, s.transport)

		if err != nil {
			readBuf.Done()
			continue
		}
		/*
			if nerr, ok := err.(net.Error); ok {
				log.Printf("network error from io.Copy: %d, %v %+v %#v\n", n, nerr, nerr, nerr, nerr.Timeout(), nerr.Temporary())
				continue
			}
			if err != nil {
				log.Println("from io.Copy:", n, err)
				s.metrics.ReadError++
				continue
			} */

		// s.metrics.PacketSize.Update(int64(n))
		select {
		case s.dataChan <- readBuf:
			s.metrics.PacketsProcessed++
			// s.updateQueueSize(1)
		default:
			s.metrics.PacketsDropped++
			// s.readBufPool.Put(readBuf)
			readBuf.Done()
		}
	}
}

// IsServing indicates whether the server is currently serving traffic
func (s *bufferedServer) IsServing() bool {
	return atomic.LoadUint32(&s.serving) == 1
}

// Stop stops the serving of traffic and waits until the queue is
// emptied by the readers
func (s *bufferedServer) Stop() {
	if atomic.SwapUint32(&s.serving, 0) == 1 {
		s.transport.Close()
		s.transport = nil
		close(s.dataChan)
		s.dataChan = nil
	}
}

// DataChan returns the data chan of the buffered server
func (s *bufferedServer) DataChan() <-chan ReadMsg {
	return s.dataChan
}

// DataRecd is called by the consumers every time they read a data item from DataChan
func (s *bufferedServer) DataRecd(buf ReadMsg) {
	// s.updateQueueSize(-1)
	s.readBufPool.Put(buf)
}

// wait in a mode of streaming decoded *LwesEvent
func (s *bufferedServer) WaitLwesMode(num_workers int) <-chan *LwesEvent {
	return nil
}

func (s *bufferedServer) lwesdecoder(out <-chan *LwesEvent) {
	for r := range s.dataChan {
		_ = r
	}
}
