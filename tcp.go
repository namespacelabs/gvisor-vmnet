package vmnet

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

func (nt *Network) setTCPForwarder(ctx context.Context) {
	tcpForwarder := tcp.NewForwarder(
		nt.stack,
		nt.tcpReceiveBufferSize,
		nt.tcpMaxInFlight,
		func(fr *tcp.ForwarderRequest) {
			id := fr.ID()

			relay := fmt.Sprintf(
				"%s:%d <-> %s:%d",
				id.LocalAddress.String(), id.LocalPort,
				id.RemoteAddress.String(), id.RemotePort,
			)

			remoteAddr := fmt.Sprintf("%s:%d", id.LocalAddress, id.LocalPort)
			conn, err := nt.dialTCP(ctx, id.LocalAddress, id.LocalPort)
			if err != nil {
				nt.logger.Error(
					"failed to dial TCP",
					"err", err,
					"target", remoteAddr,
					"between", relay,
				)
				fr.Complete(true)
				return
			}

			go func() {
				<-ctx.Done()
				conn.Close()
			}()

			var wq waiter.Queue
			ep, tcpipErr := fr.CreateEndpoint(&wq)
			if tcpipErr != nil {
				nt.logger.Info(
					"failed to create TCP end",
					slog.Any("tcpiperr", tcpipErr.String()),
					slog.String("between", relay),
				)
				fr.Complete(true)
				return
			}

			fr.Complete(false)
			ep.SocketOptions().SetKeepAlive(true)

			nt.logger.Info("start TCP relay", "between", relay)
			err = nt.pool.tcpRelay(conn, gonet.NewTCPConn(&wq, ep))
			if err != nil {
				nt.logger.Error("failed TCP relay", "err", err, "between", relay)
			}
		},
	)
	nt.stack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpForwarder.HandlePacket)
}

func (nt *Network) dialTCP(ctx context.Context, addr tcpip.Address, port uint16) (io.ReadWriteCloser, error) {
	if nt.subnet.Contains(addr) {
		return gonet.DialContextTCP(ctx, nt.stack, tcpip.FullAddress{
			NIC:  nicID,
			Addr: addr,
			Port: port,
		}, ipv4.ProtocolNumber)
	}
	remoteAddr := fmt.Sprintf("%s:%d", addr, port)

	return nt.dialOut(ctx, "tcp", remoteAddr)
}
