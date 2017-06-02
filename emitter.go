package lwes

import (
	"encoding"
	"fmt"
	"log"
	"net"

	"golang.org/x/net/ipv4"
)

type conn struct {
	*net.UDPConn
}

type Emitter struct {
	conns []*conn
}

func Open(transports ...map[string]interface{}) *Emitter {
	// lwes:<iface>:<ip>:<port>:<ttl>

	conns := make([]*conn, 0, len(transports))
	for _, trans := range transports {
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprint(trans["ip"], ":", trans["port"]))
		if err != nil {
			log.Printf("failed to resolve %q:%v:%#v, ignored\n", trans, err, err)
			continue
		}

		c, err := net.DialUDP("udp", nil, addr)
		// conn, err := net.ListenUDP("udp", nil)
		if err != nil {
			log.Printf("failed to dail %q:%v:%#v, ignored\n", addr, err, err)
			continue
		}

		var writebuffer int = 256 * 1024 * 1024
		if sndbuf, ok := trans["sndbuf"]; ok {
			writebuffer = sndbuf.(int)
		}
		if err = c.SetWriteBuffer(writebuffer); err != nil {
			log.Printf("unable to set send buffer size: err %v:%#v\n", err, err)
		}

		p := ipv4.NewPacketConn(c)
		if iface, ok := trans["iface"]; ok {
			ifstr := iface.(string)
			if ifstr != "" {
				intf, err := net.InterfaceByName(ifstr)
				if err != nil {
					log.Printf("unable to get intf: %q err %v:%#v\n", iface, err, err)
					continue
				}
				p.SetMulticastInterface(intf)
			}
		}

		var ttl int = 3
		if v, ok := trans["ttl"]; ok {
			ttl = int(v.(uint8))
		}
		p.SetMulticastTTL(ttl)
		p.SetMulticastLoopback(false)

		conns = append(conns, &conn{c})
	}
	if len(conns) == 0 {
		log.Println("no connections made.")
		return nil
	}
	return &Emitter{conns}
}

func (em *Emitter) Emit(lwe encoding.BinaryMarshaler) error {
	buf, err := Marshal(lwe)
	if err != nil {
		return nil
	}
	for _, conn := range em.conns {
		// n, err := conn.WriteToUDP(buf, conn.UDPAddr)
		n, err := conn.Write(buf)
		if err != nil {
			log.Printf("failed to write to conn, err %v:%#v\n", err, err)
		}
		log.Printf("written %d:%d bytes.\n", n, len(buf))
	}

	return nil
}

func (em *Emitter) Close() {
	for _, conn := range em.conns {
		conn.Close()
	}
}
