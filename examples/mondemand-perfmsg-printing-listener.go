package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	// "sync"
	"strings"
	"syscall"
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

	runtime.GOMAXPROCS(runtime.NumCPU())

	server, err := lwes.Listen(
		fmt.Sprintf("%s:%d", opts.Multi_addr, opts.Multi_port),
		100*1000, 65536)
	if err != nil {
		log.Fatalln("failed to start server")
	}

	sc := NewStatsClient("go-lwes-data-pipeline", 60*time.Second, map[string]interface{}{
		"ip":   "239.5.1.1",
		"port": uint16(10201),
	})
	host, _ := os.Hostname()
	sc.AddContext("host", host)

	server.EnableMetricsReport(
		5*time.Second,
		func(v interface{}) {
			// get a copy of a struct containing metrics
			// TODO: need a cache for same structures, like "encoding/json"
			t := reflect.TypeOf(v)
			if t.Kind() != reflect.Struct {
				// log.Println("accept a struct with int64 only")
				return
			}
			s := reflect.ValueOf(v)
			for i := 0; i < t.NumField(); i++ {
				tf := t.Field(i)
				vf := s.Field(i)
				/* fmt.Printf("%d: %s<%v,%v> %q %s = %v\n", i, tf.Name,
				   tf.Type.Kind(), tf.Type.Kind() == reflect.Int64,
				   tf.Tag.Get("mondemand_stat"), vf.Type(), vf.Interface()) */
				if tag := tf.Tag.Get("mondemand_stat"); tag != "" && tf.Type.Kind() == reflect.Int64 {
					words := strings.SplitN(tag, ",", 2)
					name := words[0]
					tag_m := "counter"
					if len(words) == 2 {
						tag_m = words[1]
					}
					switch tag_m {
					case "counter":
						sc.SetCounter(name, vf.Interface().(int64))
					case "gauge":
						sc.SetGauge(name, vf.Interface().(int64))
					}
				}
			}
			fmt.Println("reporting metrics", reflect.ValueOf(v))
			sc.Increment("metrics_reported", 1)
		},
	)

	time.AfterFunc(max_wait, func() {
		fmt.Printf("timeout to stop server at %s\n", max_wait)
		server.Stop()
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for s := range c {
			// Block until a signal is received.
			fmt.Println("Got signal:", s)
			noop = !noop
		}
	}()

	out := server.WaitLwesMode(12)
	for lwe := range out {
		if !server.IsServing() {
			continue
		}

		// printLwesEvent(lwe)
		msg := DecodePerfMsg(lwe)
		if !noop {
			msg.Print()
		}
		sc.Increment("msgs_printed", 1)
	}

	signal.Stop(c)
	close(c)
}
