package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	// "time"

	"github.com/lwes/lwes-go"
)

var (
	prog_id    string
	transports arrayFlags
	context    arrayFlags
	stats      arrayFlags
)

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "lwes:<iface>:<ip>:<port> | lwes:<iface>:<ip>:<port>:<ttl> | stderr"
}
func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func init() {
	flag.StringVar(&prog_id, "p", "mondemand-performance", "program identifier")
	flag.Var(&transports, "o", "Transport Options")
	flag.Var(&context, "c", "Context Options")
	flag.Var(&stats, "s", "Stats Options")
}

func main() {
	flag.Parse()
	// fmt.Println(transports)

	st := NewStatsMsg(prog_id)

	for _, ctx := range context {
		words := strings.SplitN(ctx, ":", 2)
		if len(words) != 2 {
			continue
		}
		st.AddContext(words[0], words[1])
	}

	for _, stat := range stats {
		words := strings.SplitN(stat, ":", 3)
		if len(words) != 3 {
			continue
		}
		switch words[0] {
		case "gauge", "counter":
		default:
			continue
		}
		val, err := strconv.ParseFloat(words[2], 64)
		if err != nil {
			fmt.Printf("cannot parse: %q, err %v:%#v\n", words[2], err, err)
			continue
		}
		st.AddMetric(words[0], words[1], int64(val))
	}

	if len(transports) == 0 {
		transports = append(transports, "239.5.1.1:10201")
	}

	validated := make([]map[string]interface{}, 0, len(transports))
	for _, trans := range transports {
		if v := validate_transport(trans); v != nil {
			validated = append(validated, v)
		}
	}

	fmt.Println(validated)

	em := lwes.Open(validated...)
	if em == nil {
		log.Fatal("failed to open lwes channel.\n")
	}

	em.Emit(st.ToLwes())
	em.Close()
}

func validate_transport(str string) map[string]interface{} {
	words := strings.Split(str, ":")
	if len(words) == 1 && words[0] == "stderr" {
		// TODO: support stderr
		return nil
	}
	if !(4 <= len(words) && len(words) <= 5) {
		// invalid
		return nil
	}
	if words[0] != "lwes" {
		return nil
	}

	// TODO: strconv to check port and ttl are in valid ranges
	port, err := strconv.ParseUint(words[3], 10, 16)
	if err != nil {
		return nil
	}
	v := map[string]interface{}{
		"iface": words[1],
		"ip":    words[2],
		"port":  uint16(port),
	}
	if len(words) == 5 {
		ttl, err := strconv.ParseUint(words[4], 10, 8)
		if err != nil {
			return nil
		}
		v["ttl"] = uint8(ttl)
	}
	return v
}
