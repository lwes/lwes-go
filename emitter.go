package lwes

import (
	"encoding"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/ipv4"
)

var (
	defaultTTL    = 3
	defaultSndBuf = 256 * 1024 * 1024
	defaultRcvBuf = 256 * 1024 * 1024
)

type conn struct {
	*net.UDPConn
}

type Emitter struct {
	mutex sync.RWMutex
	conns []*conn
}

type EmitterConfig struct {
	Servers []struct {
		iface    string
		addrport string
		sndbuf   int
		ttl      uint8
	}
	msend int
}

// each transport param is lwes::ip:port:<ttl> form
func (sc *EmitterConfig) ParseFromString(param string) (err error) {
	words := strings.Split(param, ":")
	if !(4 <= len(words) && len(words) <= 5) {
		return fmt.Errorf("needs format lwes:<iface>:ip:port, but got %q", param)
	}
	if words[0] != "lwes" {
		return fmt.Errorf("lwes is the only supported, but got %q", param)
	}
	iface := ""
	if words[1] != "" {
		iface = words[1]
	}
	ip := words[2]
	port := words[3]

	var ttl uint64
	if len(words) == 5 && words[5] != "" {
		ttl, err = strconv.ParseUint(words[5], 10, 8)
		if err != nil {
			return fmt.Errorf("ttl is not valid: %q, %v", param, err)
		}
	}

	sc.Servers = append(sc.Servers,
		struct {
			iface    string
			addrport string
			sndbuf   int
			ttl      uint8
		}{
			iface: iface, addrport: ip + ":" + port, ttl: uint8(ttl),
		})

	return nil
}

// each transport is lwes::ip:port:<ttl> form
func Open(cfg EmitterConfig) *Emitter {
	// lwes:<iface>:<ip>:<port>:<ttl>

	conns := make([]*conn, 0, len(cfg.Servers))
	for _, scfg := range cfg.Servers {
		addr, err := net.ResolveUDPAddr("udp", scfg.addrport)
		if err != nil {
			log.Printf("failed to resolve %q:%v:%#v, ignored\n", scfg, err, err)
			continue
		}

		c, err := net.DialUDP("udp", nil, addr)
		// conn, err := net.ListenUDP("udp", nil)
		if err != nil {
			log.Printf("failed to dail %q:%v:%#v, ignored\n", addr, err, err)
			continue
		}

		var writebuffer int = defaultSndBuf
		if scfg.sndbuf != 0 {
			writebuffer = scfg.sndbuf
		}
		if err = c.SetWriteBuffer(writebuffer); err != nil {
			log.Printf("unable to set send buffer size: err %v:%#v\n", err, err)
		}

		p := ipv4.NewPacketConn(c)
		if scfg.iface != "" {
			intf, err := net.InterfaceByName(scfg.iface)
			if err != nil {
				log.Printf("unable to get intf: %q err %v:%#v\n", scfg.iface, err, err)
				continue
			}
			p.SetMulticastInterface(intf)
		}

		var ttl int = defaultTTL
		if scfg.ttl != 0 {
			ttl = int(scfg.ttl)
		}
		p.SetMulticastTTL(ttl)
		p.SetMulticastLoopback(false)

		conns = append(conns, &conn{c})
	}

	if len(conns) == 0 {
		log.Println("no connections made.")
		return nil
	}

	return &Emitter{conns: conns}
}

func (em *Emitter) Emit(lwe encoding.BinaryMarshaler) error {
	buf, err := Marshal(lwe)
	if err != nil {
		return nil
	}

	em.mutex.RLock()
	defer em.mutex.RUnlock()

	for _, conn := range em.conns {
		// n, err := conn.WriteToUDP(buf, conn.UDPAddr)
		n, err := conn.Write(buf)
		if err != nil {
			log.Printf("failed to write to conn, err %v:%#v\n", err, err)
		}
		_ = n
		// log.Printf("written %d:%d bytes.\n", n, len(buf))
	}

	return nil
}

func (em *Emitter) Close() {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	for _, conn := range em.conns {
		conn.Close()
	}
	em.conns = nil
}
