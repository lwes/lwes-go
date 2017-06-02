package main

import (
	"os"
	"time"
)

func main() {
	sc := NewStatsClient("mondemand-performance", 60*time.Second, map[string]interface{}{
		"ip":   "239.5.1.1",
		"port": uint16(10201),
	})
	host, _ := os.Hostname()

	sc.AddContext("host", host)
	sc.Set("text_to_text_seconds", 53)
	// sc.Flush()

	tc := time.Tick(30 * time.Second)
	tc2 := time.Tick(10 * time.Second)
	t2 := time.After(15 * time.Second)
	for {
		select {
		case <-tc:
			sc.Set("text_to_text_seconds", 23)
		case <-tc2:
			sc.Increment("text_to_text_done", 1)
		case <-t2:
			sc.Flush()
		}
	}
}
