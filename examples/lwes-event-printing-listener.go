package main

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.openx.org/lwes"
	"go.openx.org/lwes/examples/pkg/multicast_group"
)

func main() {
	opts := multicast_group.NewOptions()
	opts.Bind(flag.CommandLine)
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var waitTimeout <-chan time.Time
	if max_wait > 0 {
		waitTimeout = time.After(max_wait)
	}

	var once sync.Once
	packets := 0

	server, err := lwes.Listen(
		fmt.Sprintf("%s:%d", opts.Multi_addr, opts.Multi_port),
		100*1000, 65536)

	// fmt.Println(server, out, server.IsServing())
	for {
		// fmt.Println("start serving:", server, out, server.IsServing())

		select {
		// if waitTimeout is not initialized, it's nil, never settle
		case <-waitTimeout:
			once.Do(func() {
				server.Stop()
				fmt.Println("wait time satisfied, send signal to close multicast listener.")
			})

		case lb, ok := <-server.DataChan():
			if !ok {
				fmt.Println("lwes channel closed?")
				break
			}
			lwe := new(lwes.LwesEvent)
			// lwe, err := lwes.Decode(lb.Bytes())
			err := lwe.UnmarshalBinary(lb.Bytes())
			lb.Done()

			if err != nil {
				continue // ignore for now
			}

			// fmt.Println(lwe, err)
			fmt.Printf("%s[%d]\n", lwe.Name, len(lwe.Attrs))
			fmt.Println("{")
			lwe.Enumerate(func(key string, value interface{}) bool {
				fmt.Printf("\t%s = %v;\n", key, value)
				return true
			})
			fmt.Println("}")

			packets++
		}

		if max_count > 0 && packets >= max_count {
			// once.Do(func() {
			server.Stop()
			// server.Stop()
			// fmt.Println("count satisfied, send signal to close multicast listener.")
			break
			// })
		}

		if !server.IsServing() {
			break
		}
	}

	fmt.Println(err, packets)
}
