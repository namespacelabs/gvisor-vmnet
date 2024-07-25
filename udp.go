package vmnet

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

func (nt *Network) setUDPForwarder(ctx context.Context) {
	udpForwarder := udp.NewForwarder(nt.stack, func(fr *udp.ForwarderRequest) {
		id := fr.ID()

		relay := fmt.Sprintf(
			"%s:%d <-> %s:%d",
			id.LocalAddress.String(), id.LocalPort,
			id.RemoteAddress.String(), id.RemotePort,
		)
		nt.logger.Info(
			"start UDP relay",
			slog.String("between", relay),
		)

		proxyConn, err := nt.dialUDP(ctx, id.LocalAddress, id.LocalPort)
		if err != nil {
			nt.logger.Warn(
				"failed to bind local port",
				"err", err,
				"between", relay)
			return
		}

		var wq waiter.Queue
		ep, tcpipErr := fr.CreateEndpoint(&wq)
		if tcpipErr != nil {
			nt.logger.Info(
				"failed to create TCP end",
				slog.Any("tcpiperr", tcpipErr.String()),
				slog.String("between", relay),
			)
			return
		}

		client := gonet.NewUDPConn(&wq, ep)

		ctx, cancel := context.WithCancel(ctx)

		idleTimeout := time.Minute
		timer := time.AfterFunc(idleTimeout, func() {
			cancel()
		})
		go func() {
			<-ctx.Done()
			client.Close()
			proxyConn.Close()
		}()

		extend := func() { timer.Reset(idleTimeout) }

		go func() {
			defer cancel()
			nt.pool.udpRelay(ctx, nt.logger, client, proxyConn, cancel, extend) // loc <- remote
		}()
		go func() {
			defer cancel()
			nt.pool.udpRelay(ctx, nt.logger, proxyConn, client, cancel, extend) // remote <- loc
		}()
	})
	nt.stack.SetTransportProtocolHandler(udp.ProtocolNumber, udpForwarder.HandlePacket)
}

func (nt *Network) dialUDP(ctx context.Context, addr tcpip.Address, port uint16) (net.Conn, error) {
	if nt.subnet.Contains(addr) {
		return gonet.DialUDP(nt.stack, nil, &tcpip.FullAddress{
			Addr: addr,
			Port: port,
		}, ipv4.ProtocolNumber)
	}

	remoteAddr := fmt.Sprintf("%s:%d", addr, port)
	conn, err := nt.dialOut(ctx, "udp", remoteAddr)
	if err != nil {
		return nil, err
	}
	return conn, err
}

func dialUDPConn(
	s *stack.Stack,
	laddr *tcpip.FullAddress,
	network tcpip.NetworkProtocolNumber,
	opts ...func(*tcpip.SocketOptions),
) (*gonet.UDPConn, error) {
	var wq waiter.Queue
	ep, err := s.NewEndpoint(udp.ProtocolNumber, network, &wq)
	if err != nil {
		return nil, fmt.Errorf(err.String())
	}

	for _, opt := range opts {
		opt(ep.SocketOptions())
	}

	if err := ep.Bind(*laddr); err != nil {
		ep.Close()
		return nil, &net.OpError{
			Op:   "bind",
			Net:  "udp",
			Addr: fullToUDPAddr(*laddr),
			Err:  fmt.Errorf(err.String()),
		}
	}

	return gonet.NewUDPConn(&wq, ep), nil
}

func fullToUDPAddr(addr tcpip.FullAddress) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IP(addr.Addr.AsSlice()), Port: int(addr.Port)}
}
