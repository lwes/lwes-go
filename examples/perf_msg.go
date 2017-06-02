package main

import (
	"fmt"
	"net"
	"time"

	"go.openx.org/lwes"
)

type Timeline struct {
	Label      string
	Start, End int64
}

type PerfMsg struct {
	Context      map[string]string
	Timelines    []*Timeline
	Caller_label string
	Perf_id      string
	ReceiptTime  int64
	SenderIP     net.IP // lwes.IPAddr
	SenderPort   uint16

	context_keys []string
}

func DecodePerfMsg(lwe *lwes.LwesEvent) *PerfMsg {
	var num_ctxt, num_timelines int

	if val, ok := lwe.Attrs["ctxt_num"].(uint16); ok {
		num_ctxt = int(val)
	} else {
		// some lwes events come with no context, it's ok
	}

	num_timelines = int(lwe.Attrs["num"].(uint16))

	msg := &PerfMsg{
		Context:      make(map[string]string, num_ctxt),
		Timelines:    make([]*Timeline, 0, num_timelines),
		context_keys: make([]string, 0, num_ctxt),
	}

	for i := 0; i < num_ctxt; i++ {
		k := lwe.Attrs[fmt.Sprint("ctxt_k", i)].(string)
		v := lwe.Attrs[fmt.Sprint("ctxt_v", i)].(string)
		msg.Context[k] = v
		msg.context_keys = append(msg.context_keys, k)
	}

	for i := 0; i < num_timelines; i++ {
		label := lwe.Attrs[fmt.Sprint("label", i)].(string)
		start := lwe.Attrs[fmt.Sprint("start", i)].(int64)
		end := lwe.Attrs[fmt.Sprint("end", i)].(int64)
		msg.Timelines = append(msg.Timelines, &Timeline{label, start, end})
	}

	msg.Caller_label = lwe.Attrs["caller_label"].(string)
	msg.Perf_id = lwe.Attrs["id"].(string)

	msg.ReceiptTime = lwe.Attrs["ReceiptTime"].(int64)
	msg.SenderIP = lwe.Attrs["SenderIP"].(net.IP) // lwes.IPAddr)
	msg.SenderPort = lwe.Attrs["SenderPort"].(uint16)

	// fmt.Printf("msg: %v\n", msg)
	// fmt.Printf("msg#: %#v\n", msg)
	// fmt.Printf("msg+: %+v\n", msg)

	return msg
}

func (msg *PerfMsg) Noop() {}

func toTime(msec int64) time.Time {
	return time.Unix(msec/1000, (msec%1000)*1000000)
}

func (msg *PerfMsg) Print() {
	/* var names []string
	var err error
	 if lookupaddr {
		names, err = net.LookupAddr(msg.SenderIP.String())
	} */
	fmt.Printf("PerfMsg[%s] (at %s, from %s:%d)\n",
		msg.Perf_id,
		toTime(msg.ReceiptTime).Format("2006-01-02T15:04:05.000Z07:00"),
		msg.SenderIP, msg.SenderPort,
		// names, err,
	)
	fmt.Println("{")
	fmt.Printf("\t%v\n", msg.Caller_label)
	// fmt.Printf("\t%v\n", msg.context)
	for _, k := range msg.context_keys {
		fmt.Printf("\t |%s:\t%q|\n", k, msg.Context[k])
	}
	for _, tl := range msg.Timelines {
		start, end := toTime(tl.Start), toTime(tl.End)
		fmt.Printf("\t%s\t%s\n\t |%s %s|\n", tl.Label, end.Sub(start),
			start.Format("2006-01-02T15:04:05.000Z07:00"),
			end.Format("2006-01-02T15:04:05.000Z07:00"),
		)
	}
	fmt.Println("}")
}
