package vmnet

import (
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/insomniacslk/dhcp/dhcpv4"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/arp"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

type endpoint struct {
	// conn is the set of connection each identifying one inbound/outbound
	// channel.
	conns syncmap[tcpip.Address, net.Conn]

	arpTable syncmap[tcpip.Address, tcpip.LinkAddress]

	// mtu (maximum transmission unit) is the maximum size of a packet.
	mtu uint32

	// addr is the MAC address of the endpoint.
	addr tcpip.LinkAddress

	subnet tcpip.Subnet

	dispatcher stack.NetworkDispatcher

	// wg keeps track of running goroutines.
	wg sync.WaitGroup

	// closed is a function to be called when the FD's peer (if any) closes
	// its end of the communication pipe.
	closed func(tcpip.Address, error)

	writer  pcapWriter
	snapLen int

	pool *bytePool

	logger *slog.Logger

	dhcpv4Handler *dhcpHandler
}

type gatewayEndpointOption struct {
	MTU           uint32
	Address       tcpip.LinkAddress
	Subnet        tcpip.Subnet
	Writer        *os.File
	ClosedFunc    func(tcpip.Address, error)
	Pool          *bytePool
	Logger        *slog.Logger
	DHCPv4Handler *dhcpHandler
}

func newGatewayEndpoint(opts gatewayEndpointOption) (*endpoint, error) {
	ep := &endpoint{
		conns:         newSyncmap[tcpip.Address, net.Conn](),
		arpTable:      newSyncmap[tcpip.Address, tcpip.LinkAddress](),
		mtu:           opts.MTU,
		closed:        opts.ClosedFunc,
		addr:          opts.Address,
		subnet:        opts.Subnet,
		pool:          opts.Pool,
		logger:        opts.Logger,
		dhcpv4Handler: opts.DHCPv4Handler,
	}
	if opts.Writer != nil {
		ep.writer = &syncPCAPWriter{w: pcapgo.NewWriter(opts.Writer)}
		ep.snapLen = 65536
		if err := ep.writer.WriteFileHeader(uint32(ep.snapLen), layers.LinkTypeEthernet); err != nil {
			return nil, err
		}
	}
	return ep, nil
}

func (e *endpoint) RegisterConn(ipAddr tcpip.Address, hwAddr tcpip.LinkAddress, conn net.Conn) {
	e.conns.Store(ipAddr, conn)
	e.arpTable.Store(ipAddr, hwAddr)

	// Link endpoints are not savable. When transportation endpoints are
	// saved, they stop sending outgoing packets and all incoming packets
	// are rejected.
	if e.dispatcher != nil {
		e.wg.Add(1)
		go func() {
			e.dispatchLoop(ipAddr, conn)
			e.wg.Done()
		}()
	}
}

// IsAttached implements stack.LinkEndpoint.IsAttached.
func (e *endpoint) IsAttached() bool {
	return e.dispatcher != nil
}

// MTU implements stack.LinkEndpoint.MTU. It returns the value initialized
// during construction.
func (e *endpoint) MTU() uint32 {
	return e.mtu
}

// Capabilities implements stack.LinkEndpoint.Capabilities.
func (e *endpoint) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityResolutionRequired
}

// MaxHeaderLength returns the maximum size of the link-layer header.
func (e *endpoint) MaxHeaderLength() uint16 {
	return header.EthernetMinimumSize
}

// LinkAddress returns the link address of this endpoint.
func (e *endpoint) LinkAddress() tcpip.LinkAddress {
	return e.addr
}

// Wait implements stack.LinkEndpoint.Wait. It waits for the endpoint to stop
// reading from its FD.
func (e *endpoint) Wait() {
	e.wg.Wait()
}

// ARPHardwareType implements stack.LinkEndpoint.ARPHardwareType.
func (e *endpoint) ARPHardwareType() header.ARPHardwareType {
	return header.ARPHardwareEther
}

// AddHeader implements stack.LinkEndpoint.AddHeader.
func (e *endpoint) AddHeader(pkt stack.PacketBufferPtr) {
	// Add ethernet header if needed.
	eth := header.Ethernet(pkt.LinkHeader().Push(header.EthernetMinimumSize))
	eth.Encode(&header.EthernetFields{
		SrcAddr: pkt.EgressRoute.LocalLinkAddress,
		DstAddr: pkt.EgressRoute.RemoteLinkAddress,
		Type:    pkt.NetworkProtocolNumber,
	})
}

// Attach launches the goroutine that reads packets from the file descriptor and
// dispatches them via the provided dispatcher. If one is already attached,
// then nothing happens.
//
// Attach implements stack.LinkEndpoint.Attach.
func (e *endpoint) Attach(dispatcher stack.NetworkDispatcher) {
	// nil means the NIC is being removed.
	if dispatcher == nil && e.dispatcher != nil {
		e.Wait()
		e.dispatcher = nil
		return
	}
	if dispatcher != nil && e.dispatcher == nil {
		e.dispatcher = dispatcher
	}
}

// dispatchLoop reads packets from the file descriptor in a loop and dispatches
// them to the network stack.
func (e *endpoint) dispatchLoop(ipAddr tcpip.Address, conn net.Conn) {
	for {
		cont, err := e.inboundDispatch(ipAddr, conn)
		if err != nil || !cont {
			e.conns.Delete(ipAddr)
			if e.closed != nil {
				e.closed(ipAddr, err)
			}
			return
		}
	}
}

// writePacket writes outbound packets to the connection. If it is not
// currently writable, the packet is dropped.
func (e *endpoint) writePacket(pkt stack.PacketBufferPtr) tcpip.Error {
	data := pkt.ToView().AsSlice()

	if e.writer != nil {
		packetSize := pkt.Size()
		capLen := e.captureLength(packetSize)
		e.writer.WritePacket(gopacket.CaptureInfo{
			Timestamp:     time.Now(),
			CaptureLength: capLen,
			Length:        packetSize,
		}, data[0:capLen])
	}

	conn, ok := e.conns.Load(pkt.EgressRoute.RemoteAddress)
	if ok {
		if _, err := conn.Write(data); err != nil {
			e.logger.Warn("failed to write packet data in endpoint", err)
			return &tcpip.ErrInvalidEndpointState{}
		}
		return nil
	}

	e.conns.Range(func(_ tcpip.Address, v net.Conn) bool {
		v.Write(data)
		return true
	})
	return nil
}

// WritePackets writes outbound packets to the underlying connection. If
// one is not currently writable, the packet is dropped.
//
// Being a batch API, each packet in pkts should have the following
// fields populated:
//   - pkt.EgressRoute
//   - pkt.NetworkProtocolNumber
func (e *endpoint) WritePackets(pkts stack.PacketBufferList) (written int, err tcpip.Error) {
	for _, pkt := range pkts.AsSlice() {
		if err := e.writePacket(pkt); err != nil {
			break
		}
		written++
	}
	return written, err
}

func (e *endpoint) captureLength(packetSize int) int {
	if packetSize < e.snapLen {
		return packetSize
	}
	return e.snapLen
}

// dispatch reads one packet from the file descriptor and dispatches it.
func (e *endpoint) inboundDispatch(devAddr tcpip.Address, conn net.Conn) (bool, error) {
	data := e.pool.getBytes()
	defer e.pool.putBytes(data)

	n, err := conn.Read(data)
	if err != nil {
		return false, err
	}

	if e.writer != nil {
		e.writer.WritePacket(gopacket.CaptureInfo{
			Timestamp:     time.Now(),
			CaptureLength: e.captureLength(n),
			Length:        n,
		}, data[:n])
	}

	buf := buffer.MakeWithData(data[:n])
	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buf,
	})
	defer pkt.DecRef()

	return e.deliverOrConsumeNetworkPacket(pkt, conn)
}

func (e *endpoint) deliverOrConsumeNetworkPacket(
	pkt stack.PacketBufferPtr,
	conn net.Conn,
) (bool, error) {
	hdr, ok := pkt.LinkHeader().Consume(int(e.MaxHeaderLength()))
	if !ok {
		return false, nil
	}
	ethHdr := header.Ethernet(hdr)

	switch ethHdr.Type() {
	case arp.ProtocolNumber:
		return e.deliverOrConsumeARPPacket(ethHdr, pkt, conn)
	case ipv4.ProtocolNumber:
		return e.deliverOrConsumeIPv4Packet(ethHdr, pkt, conn)
	default:
		e.dispatcher.DeliverNetworkPacket(ethHdr.Type(), pkt)
		return true, nil
	}
}

func (e *endpoint) deliverOrConsumeARPPacket(
	ethHdr header.Ethernet,
	pkt stack.PacketBufferPtr,
	conn net.Conn,
) (bool, error) {
	data := pkt.ToView().AsSlice()
	req := header.ARP(data[header.EthernetMinimumSize:])
	if req.IsValid() && req.Op() == header.ARPRequest {
		target := req.ProtocolAddressTarget()

		linkAddr, ok := e.arpTable.Load(tcpip.AddrFromSlice(target))
		if ok {
			buf := make([]byte, header.EthernetMinimumSize+header.ARPSize)
			eth := header.Ethernet(buf)
			eth.Encode(&header.EthernetFields{
				SrcAddr: linkAddr,
				DstAddr: tcpip.LinkAddress(req.HardwareAddressSender()),
				Type:    header.ARPProtocolNumber,
			})
			res := header.ARP(buf[header.EthernetMinimumSize:])
			res.SetIPv4OverEthernet()
			res.SetOp(header.ARPReply)

			copy(res.HardwareAddressSender(), linkAddr)
			copy(res.ProtocolAddressSender(), req.ProtocolAddressTarget())
			copy(res.HardwareAddressTarget(), req.HardwareAddressSender())
			copy(res.ProtocolAddressTarget(), req.ProtocolAddressSender())

			conn.Write(buf)
			return true, nil
		}
	}
	e.dispatcher.DeliverNetworkPacket(ethHdr.Type(), pkt)
	return true, nil
}

func (e *endpoint) deliverOrConsumeIPv4Packet(
	ethHdr header.Ethernet,
	pkt stack.PacketBufferPtr,
	conn net.Conn,
) (bool, error) {
	data := pkt.ToView().AsSlice()
	ipv4 := header.IPv4(data[header.EthernetMinimumSize:])

	switch ipv4.TransportProtocol() {
	case header.ICMPv4ProtocolNumber:
		{
			// Deliver packets to the created network stack.
			if e.subnet.Contains(ipv4.DestinationAddress()) {
				e.dispatcher.DeliverNetworkPacket(ethHdr.Type(), pkt)
				return true, nil
			}

			// Only ICMPv4 echo request is emulated on the Go side.
			//
			// sudo privilege is required to use ICMP packets. So gvisor
			// cannot use ICMPv4 packets as they are. Therefore, gvisor
			// looks at the contents of the packets and converts them to
			// echo requests using UDP. The result is converted to ICMPv4
			// packet, which is then passed to the guest.
			icmpv4 := header.ICMPv4(data[header.EthernetMinimumSize+header.IPv4MinimumSize:])
			if icmpv4.Type() == header.ICMPv4Echo {
				tmp := icmpv4.Payload()
				payload := make([]byte, len(tmp))
				copy(payload, tmp)

				go pingv4(conn, pingPacket{
					srcIP:    ipv4.SourceAddress(),
					dstIP:    ipv4.DestinationAddress(),
					srcMAC:   ethHdr.SourceAddress(),
					dstMAC:   ethHdr.DestinationAddress(),
					payload:  payload,
					ident:    icmpv4.Ident(),
					sequence: icmpv4.Sequence(),
				})
				return true, nil
			}
		}
	case header.UDPProtocolNumber:
		{
			udpv4 := header.UDP(data[header.EthernetMinimumSize+header.IPv4MinimumSize:])
			srcPort := udpv4.SourcePort()
			dstPort := udpv4.DestinationPort()

			// In order to ensure IP address allocation here, DHCP broadcast responses
			// are made to the connection associated with each IP address.
			if dstPort == 67 && srcPort == 68 {
				msg, err := dhcpv4.FromBytes(udpv4.Payload())
				if err != nil {
					e.logger.Warn("failed to decode DHCPv4 packet data in endpoint", err)
					return true, nil
				}
				go e.dhcpv4Handler.handleDHCPv4(conn, dhcpv4Packet{
					srcIP:   ipv4.SourceAddress(),
					dstIP:   ipv4.DestinationAddress(),
					srcPort: srcPort,
					dstPort: dstPort,
					srcMAC:  ethHdr.SourceAddress(),
					dstMAC:  ethHdr.DestinationAddress(),
					msg:     msg,
				})

				return true, nil
			}
		}
	default:
	}

	e.dispatcher.DeliverNetworkPacket(ethHdr.Type(), pkt)

	return true, nil
}

func (e *endpoint) ParseHeader(stack.PacketBufferPtr) bool { return true }
