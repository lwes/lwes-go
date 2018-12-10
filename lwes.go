package lwes

import (
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	/* maximum datagram size for UDP is 65535 (0xffff) minus
	    transport layer overhead (20 bytes for IP header, and 8 bytes for UDP header), so this value
	   should be 65535 - 28 = 65507
	*/
	MAX_MSG_SIZE       = 65535 - 20 - 8
	DEFAULT_QUEUE_SIZE = 100 * 1000

	MAX_PACKET_SIZE = 64 * 1024 // max size of single UDP packet is 64KB - headersize
	SO_RCVBUF_SIZE  = 16 * 1024 * 1024

	defaultQueueSize     = 1000
	defaultMaxPacketSize = 65000
	defaultServerWorkers = 10
)

type bufferedServer struct {
	multi_addrport string

	dataChan      chan *readBuf
	datawait      sync.WaitGroup
	lwesChan      chan *LwesEvent
	waitworkers   sync.WaitGroup
	maxPacketSize int
	maxQueueSize  int
	serving       uint32
	transport     *net.UDPConn
	readBufPool   sync.Pool
	startstop     chan struct{}
	waitstop      chan struct{}
	tick          *time.Ticker

	reportIntv time.Duration
	reportFunc func(string, interface{})

	metricsLock sync.RWMutex
	metrics     struct {
		QueueSize             int64 `mondemand_stat:"queue_size,gauge"`
		PacketSize            int64 `mondemand_stat:"packet_size,gauge"`
		BytesReceived         int64 `mondemand_stat:"bytes_received"`
		BytesDropped          int64 `mondemand_stat:"bytes_dropped"`
		PacketsReceived       int64 `mondemand_stat:"packets_received"`
		PacketsDropped        int64 `mondemand_stat:"packets_dropped"`
		PacketsProcessed      int64 `mondemand_stat:"packets_processed"`
		PacketsInvalid        int64 `mondemand_stat:"packets_invalid"`
		PacketsDecoded        int64 `mondemand_stat:"packets_decoded"`
		PacketsDecodedPassed  int64 `mondemand_stat:"packets_decoded_passed"`
		PacketsDroppedDecoded int64 `mondemand_stat:"packets_dropped_decoded"`
		ReadError             int64 `mondemand_stat:"packets_read_error"`
		ReadTimeout           int64 `mondemand_stat:"packets_read_timeout"`
	}
}

// listen on the multicast "addr:port" form
// return a server with the Server interface methods
func Listen(multi_addrport string) (Server, error) {

	addr, err := net.ResolveUDPAddr("udp", multi_addrport)
	if err != nil {
		log.Println("failed to resolve:", multi_addrport)
		return nil, err
	}
	var conn *net.UDPConn
	if addr.IP.IsMulticast() {
		conn, err = net.ListenMulticastUDP("udp", nil, addr)
	} else {
		conn, err = net.ListenUDP("udp", addr)
	}
	if err != nil {
		log.Println("failed to listen:", addr)
		return nil, err
	}
	log.Println("start listening on:", multi_addrport)
	// s.transport = conn

	var bufsize int = defaultRcvBuf
	if err = conn.SetReadBuffer(bufsize); err != nil {
		log.Printf("unable to set recv buffer size: err %v:%#v\n", err, err)
	}
	// read it back to verify
	log.Printf("set conn:<%v> read buffer size to: %d\n", conn, bufsize)

	dataChan := make(chan *readBuf, DEFAULT_QUEUE_SIZE)

	// readBufPool := &sync.Pool{}
	// New: func() interface{} { return &readBuf{buf: make([]byte, MAX_PACKET_SIZE)} },

	s := &bufferedServer{
		multi_addrport: multi_addrport,
		dataChan:       dataChan,
		transport:      conn,
		maxQueueSize:   DEFAULT_QUEUE_SIZE,
		maxPacketSize:  MAX_PACKET_SIZE,
		// readBufPool:    readBufPool,
		startstop: make(chan struct{}),
		waitstop:  make(chan struct{}),
	}

	go s.Serve()

	// wait it started before returning
	<-s.startstop

	return s, nil
}

func (s *bufferedServer) Serve() {
	if atomic.SwapUint32(&s.serving, 1) != 0 {
		return
	}
	s.startstop <- struct{}{}

	runtime.LockOSThread()

	readBuf := NewFixedBuffer(&s.readBufPool, s.maxPacketSize)
	for s.IsServing() {
		s.transport.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := io.Copy(readBuf, s.transport)

		s.metricsLock.Lock()
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			s.metrics.ReadTimeout++
			s.metricsLock.Unlock()
			continue
		}

		if err != nil {
			s.metrics.ReadError++
			s.metricsLock.Unlock()
			continue
		}

		// atomic.AddInt64(&s.metrics.BytesReceived, n)
		s.metrics.BytesReceived += n
		// atomic.AddInt64(&s.metrics.PacketsReceived, 1)
		s.metrics.PacketsReceived++
		// atomic.StoreInt64(&s.metrics.PacketSize, int64(readBuf.Len()))
		s.metrics.PacketSize = n
		s.metricsLock.Unlock()

		// must increase the counter before enqueue
		s.datawait.Add(1)

		s.metricsLock.Lock()
		select {
		case s.dataChan <- readBuf:
			// atomic.AddInt64(&s.metrics.PacketsProcessed, 1)
			s.metrics.PacketsProcessed++
			// atomic.StoreInt64(&s.metrics.QueueSize, int64(len(s.dataChan)))
			s.metrics.QueueSize = int64(len(s.dataChan))
			s.metricsLock.Unlock()

			// s.updateQueueSize(1)
			readBuf = NewFixedBuffer(&s.readBufPool, s.maxPacketSize)
		default:
			// atomic.AddInt64(&s.metrics.BytesDropped, n)
			s.metrics.BytesDropped += n
			// atomic.AddInt64(&s.metrics.PacketsDropped, 1)
			s.metrics.PacketsDropped++
			s.metricsLock.Unlock()

			// drop one if didn't enqueue
			s.datawait.Done()
		}
	}
	runtime.UnlockOSThread()

	readBuf.Done()

	s.startstop <- struct{}{}

	if s.tick != nil {
		// if MetricsReport ever enabled
		s.tick.Stop()
	}
}

// IsServing indicates whether the server is currently serving traffic
func (s *bufferedServer) IsServing() bool {
	return atomic.LoadUint32(&s.serving) == 1
}

// Stop stops the serving of traffic and waits until the queue is
// emptied by the readers
func (s *bufferedServer) Stop() {
	if atomic.SwapUint32(&s.serving, 0) == 0 {
		// already stopped
		return
	}

	<-s.startstop

	s.transport.Close()
	s.transport = nil

	// log.Printf("in stopping: waiting for dataChan stopping: %d:%d, %v\n", len(s.dataChan), cap(s.dataChan), s.metrics)

	// for len(s.dataChan) > 0 {}
	// log.Printf("after waiting for dataChan stopping: %d:%d\n", len(s.dataChan), cap(s.dataChan))
	s.datawait.Wait()
	log.Printf("no more lwes events.")

	close(s.dataChan)
	s.dataChan = nil

	// log.Println("in stopping")

	if s.lwesChan != nil {
		s.waitworkers.Wait()
		log.Printf("no more lwes decoder workers.")

		close(s.lwesChan)
		s.lwesChan = nil
	}

	// non-blocking send msg
	select {
	case s.waitstop <- struct{}{}: // send an empty marker
	default: // if no one waiting
	}

	log.Printf("lwes serving is done.")
}

func (s *bufferedServer) Wait() {
	<-s.waitstop
}

// DataChan returns the data chan of the buffered server
func (s *bufferedServer) DataChan() <-chan *readBuf {
	if s.lwesChan != nil {
		// do not use data chan in lwes decoding mode
		return nil
	}

	return s.dataChan
}

// wait in a mode of streaming decoded *LwesEvent
func (s *bufferedServer) WaitLwesMode(num_workers int) <-chan *LwesEvent {
	ch := make(chan *LwesEvent, s.maxQueueSize)
	s.lwesChan = ch

	// check
	if num_workers == 0 {
		num_workers = runtime.NumCPU()
	}

	for i := 0; i < num_workers; i++ {
		s.waitworkers.Add(1)
		go s.lwesdecoder(i, s.dataChan)
	}

	return ch
}

func (s *bufferedServer) lwesdecoder(idx int, dataChan <-chan *readBuf) {
	lwe := new(LwesEvent)
	for rbuf := range dataChan {
		// decrement the data wait counters
		s.datawait.Done()

		err := lwe.UnmarshalBinary(rbuf.Bytes())

		s.metricsLock.Lock()
		if err != nil {
			// update some counters
			// atomic.AddInt64(&s.metrics.PacketsInvalid, 1)
			s.metrics.PacketsInvalid++
			s.metricsLock.Unlock()
			continue
		}
		s.metricsLock.Unlock()

		rbuf.Done()

		s.metricsLock.Lock()
		// atomic.AddInt64(&s.metrics.PacketsDecoded, 1)
		s.metrics.PacketsDecoded++

		select {
		case s.lwesChan <- lwe:
			// atomic.AddInt64(&s.metrics.PacketsDecodedPassed, 1)
			s.metrics.PacketsDecodedPassed++
			s.metricsLock.Unlock()
			lwe = new(LwesEvent)
		default:
			// atomic.AddInt64(&s.metrics.PacketsDroppedDecoded, 1)
			s.metrics.PacketsDroppedDecoded++
			s.metricsLock.Unlock()
		}
	}
	// log.Printf("worker%d end\n", idx)

	s.waitworkers.Done()
}

func (s *bufferedServer) EnableMetricsReport(interval time.Duration, reportFunc func(string, interface{})) {
	s.reportIntv = interval
	s.reportFunc = reportFunc

	if interval != 0 {
		s.tick = time.NewTicker(interval)
		go func() {
			for range s.tick.C {
				// log.Println("reporting metrics")
				s.metricsLock.RLock()
				metrics := s.metrics
				s.metricsLock.RUnlock()

				reportFunc("lwes-events", metrics)
			}
		}()
	}
}

func (s *bufferedServer) Addr() net.Addr {
	return s.transport.LocalAddr()
}