package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"go.openx.org/lwes"
	"go.openx.org/lwes/examples/pkg/multicast_group"
)

var (
	noop       bool
	lookupaddr bool
)

func init() {
	const local_prefix = "local."
	flag.BoolVar(&noop, local_prefix+"noop", false, "no operation")
	flag.BoolVar(&lookupaddr, local_prefix+"lookup", false, "print hostname for each addr")
}

func printLwesEvent(lwe *lwes.LwesEvent) {
	fmt.Printf("%s[%d]\n", lwe.Name, len(lwe.Attrs))
	fmt.Println("{")
	lwe.Enumerate(func(key string, value interface{}) bool {
		fmt.Printf("\t%s = %v;\n", key, value)
		return true
	})
	fmt.Println("}")
}

func main() {
	opts := multicast_group.NewOptions()
	opts.Bind(flag.CommandLine)
	flag.Parse()

	var waitTimeout <-chan time.Time
	/* if max_wait != "" {
		if dura, err := time.ParseDuration(max_wait); err == nil {
			waitTimeout = time.After(dura)
			fmt.Printf("wait running for %q\n", dura)
		}
	} */
	waitTimeout = time.After(max_wait)

	runtime.GOMAXPROCS(runtime.NumCPU())

	// var once sync.Once
	// packets := 0

	// begin := time.Now()
	// lbufs, quitCh, done, err := lwes.Listen(opts.Multi_addr, opts.Multi_port, 100*time.Millisecond)

	// var server lwes.Server
	// var err error
	server, err := lwes.Listen(
		fmt.Sprintf("%s:%d", opts.Multi_addr, opts.Multi_port),
		100*1000, 65536)
	if err != nil {
		log.Fatalln("failed to start server")
	}

	// started by Listen
	// go server.Serve()

	out := make(chan *lwes.LwesEvent, 100)
	processing := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		processing.Add(1)
		go func() {
			for readBuf := range server.DataChan() {
				lwe := new(lwes.LwesEvent)
				// err := lwes.Unmarshal(readBuf.GetBytes(), &lwe)
				// err := lwes.Unmarshal(readBuf.GetBytes(), &lwe)
				err := lwe.UnmarshalBinary(readBuf.Bytes())
				// server.DataRecd(readBuf)
				readBuf.Done()

				if err != nil {
					// TODO: save last Unmarshal error and break if too many
					log.Printf("Unmarshal failed:", err)
					continue
				}

				select {
				case out <- lwe:
					// well consumed
				default:
					// consumer too slow
					// counters
				}
			}
			processing.Done()
		}()
	}

OUTER_LOOP:
	for {
		select {
		case <-waitTimeout:
			// fmt.Println("timeout to break", max_wait)
			server.Stop()
			break OUTER_LOOP

		case lwe := <-out:
			// printLwesEvent(lwe)
			msg := DecodePerfMsg(lwe)
			msg.Print()
		}
	}

	processing.Wait()
}
