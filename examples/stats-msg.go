package main

import (
	"fmt"
	// "log"
	"time"

	"github.com/lwes/lwes-go"
)

const (
	precachedSize = 1024
)

var (
	cached map[string][]string
)

func init() {
	cached = make(map[string][]string)
	for _, key := range []string{"k", "v", "t", "ctxt_k", "ctxt_v"} {
		cached[key] = make([]string, precachedSize)
		for i := 0; i < precachedSize; i++ {
			cached[key][i] = fmt.Sprint(key, i)
		}
	}
}

func getK(key string, idx int) string {
	if idx >= precachedSize {
		return fmt.Sprint(key, idx)
	}
	if strs, ok := cached[key]; ok {
		return strs[idx]
	}
	return fmt.Sprint(key, idx)
}

type metric struct {
	tag   string
	key   string
	value int64
}

type StatsMsg struct {
	prog_id      string
	metrics      []*metric
	context      map[string]string
	context_keys []string
}

func NewStatsMsg(prog_id string) *StatsMsg {
	return &StatsMsg{prog_id: prog_id,
		metrics: make([]*metric, 0, 10),
		context: make(map[string]string),
	}
}

func (st *StatsMsg) AddContext(key, value string) {
	st.context_keys = append(st.context_keys, key)
	st.context[key] = value
}

func (st *StatsMsg) AddMetric(tag, key string, value int64) {
	st.metrics = append(st.metrics, &metric{
		tag: tag, key: key, value: value})
}

func (st *StatsMsg) ToLwes() *lwes.LwesEvent {
	lwe := lwes.NewLwesEvent("MonDemand::StatsMsg")
	lwe.Set("prog_id", st.prog_id)

	lwe.Set("ctxt_num", uint16(len(st.context)))
	for idx, key := range st.context_keys {
		value := st.context[key]
		lwe.Set(getK("ctxt_k", idx), key)
		lwe.Set(getK("ctxt_v", idx), value)
	}

	lwe.Set("num", uint16(len(st.metrics)))
	for idx, metric := range st.metrics {
		lwe.Set(getK("t", idx), metric.tag)
		lwe.Set(getK("k", idx), metric.key)
		lwe.Set(getK("v", idx), metric.value)
	}

	return lwe
}

type metrickey struct {
	tag, key string
	// encoded context
}

type StatsClient struct {
	*lwes.Emitter
	prog_id  string
	interval time.Duration
	metrics  chan *metric
	context  [][2]string
	contextc chan [2]string
	control  chan string
	statsdb  map[metrickey]*metric

	serverstarted chan struct{}
}

func NewStatsClient(prog_id string, interval time.Duration, transports ...map[string]interface{}) *StatsClient {
	em := lwes.Open(transports...)
	if em == nil {
		return nil
	}
	sc := &StatsClient{
		Emitter:  em,
		prog_id:  prog_id,
		interval: interval,
		metrics:  make(chan *metric, 100),
		contextc: make(chan [2]string, 10),
		control:  make(chan string),
		statsdb:  make(map[metrickey]*metric),

		serverstarted: make(chan struct{}),
	}

	go sc.serve()
	<-sc.serverstarted

	return sc
}

func (sc *StatsClient) flush() {
	st := NewStatsMsg(sc.prog_id)
	for _, keyval := range sc.context {
		st.AddContext(keyval[0], keyval[1])
	}
	for key, me := range sc.statsdb {
		st.metrics = append(st.metrics, me)

		// TODO: need a LRU strategy to delete too old not-in-use keys
		_ = key
		// delete(sc.statsdb, key)
	}
	sc.Emit(st.ToLwes())

	// log.Print("tick")
}

func (sc *StatsClient) serve() {
	sc.serverstarted <- struct{}{}

	ticker := time.Tick(sc.interval)
	for {
		// go one metric request
		select {
		case me := <-sc.metrics:
			orig_tag := me.tag
			if orig_tag == "setcounter" {
				me.tag = "counter"
			}
			mek := metrickey{me.tag, me.key}
			if m, ok := sc.statsdb[mek]; ok {
				switch orig_tag {
				case "counter":
					m.value += me.value
				case "setcounter", "gauge":
					m.value = me.value
				}
			} else {
				sc.statsdb[mek] = me
			}
		case keyval := <-sc.contextc:
			sc.context = append(sc.context, keyval)
		case ctrl := <-sc.control:
			switch ctrl {
			case "flush":
				sc.flush()
			}
		case <-ticker:
			sc.flush()
		}
	}
}

// counters
func (sc *StatsClient) Increment(key string, value int64) {
	sc.metrics <- &metric{"counter", key, value}
}

func (sc *StatsClient) SetCounter(key string, value int64) {
	sc.metrics <- &metric{"setcounter", key, value}
}

// for gauges
func (sc *StatsClient) SetGauge(key string, value int64) {
	sc.metrics <- &metric{"gauge", key, value}
}

func (sc *StatsClient) AddContext(key, value string) {
	sc.contextc <- [2]string{key, value}
}

func (sc *StatsClient) Flush() {
	sc.control <- "flush"
}

func (sc *StatsClient) Close() {
}
