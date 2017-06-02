package main

import (
	"flag"
	"time"
)

var (
	max_count int
	max_wait  time.Duration
)

func init() {
	flag.IntVar(&max_count, "listen.max_count", 0, "to print max count, use -1 for indefinitely")
	flag.DurationVar(&max_wait, "listen.max_wait", 0, "to listen and wait for max duration")
}
