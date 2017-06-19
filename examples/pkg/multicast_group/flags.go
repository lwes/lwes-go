package multicast_group

import "flag"

type Options struct {
	Multi_addr string
	Multi_port int
}

func NewOptions() *Options {
	return &Options{}
}

func (opts *Options) Bind(flags *flag.FlagSet) {
	flags.StringVar(&opts.Multi_addr, "multicast.addr", "239.5.1.100", "the multicast address to listen on")
	flags.IntVar(&opts.Multi_port, "multicast.port", 11311, "the multicast port to listen on")
}
