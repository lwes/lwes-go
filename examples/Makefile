
all: mondemand-perfmsg-printing-listener lwes-event-printing-listener

mondemand-perfmsg-printing-listener: mondemand-perfmsg-printing-listener.go counters.go perf_msg.go stats-msg.go
	go build -v -- $^

lwes-event-printing-listener: lwes-event-printing-listener.go counters.go
	go build -v -- $^

mondemand-tool: mondemand-tool.go stats-msg.go
	go build -v -- $^

mondemand-set: mondemand-set.go stats-msg.go
	go build -v -- $^

clean:
	@-rm mondemand-perfmsg-printing-listener lwes-event-printing-listener