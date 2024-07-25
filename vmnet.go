package vmnet

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

type networkOpts struct {
	MTU                  uint32
	PcapFile             *os.File
	MACAddress           net.HardwareAddr
	DNSConfig            *DNSConfig
	TCPMaxInFlight       int
	TCPReceiveBufferSize int
	Logger               *slog.Logger
	DialOut              func(ctx context.Context, network string, addr string) (net.Conn, error)
}

// NetworkOpts is functional options.
type NetworkOpts func(*networkOpts)

// WithMTU is an option to set MTU (maximum transmission unit) between the gateway
// and some link devices.
//
// Default is 1500.
func WithMTU(mtu uint32) NetworkOpts {
	return func(n *networkOpts) {
		n.MTU = mtu
	}
}

// WithPcapFile is an option to create a pcap file based on the given file
// for writing packet data.
//
// Default is nil.
func WithPcapFile(pcapFile *os.File) NetworkOpts {
	return func(n *networkOpts) {
		n.PcapFile = pcapFile
	}
}

// WithGatewayMACAddress is an option to specify the gateway MAC address.
//
// Default is "7a:5b:10:21:90:e3"
func WithGatewayMACAddress(hwAddr net.HardwareAddr) NetworkOpts {
	return func(n *networkOpts) {
		n.MACAddress = hwAddr
	}
}

// WithDNSConfig is an option to configure DNS.
//
// Default value will be created from your /etc/resolv.conf file.
func WithDNSConfig(dnsConfig *DNSConfig) NetworkOpts {
	return func(n *networkOpts) {
		n.DNSConfig = dnsConfig
	}
}

// WithTCPMaxInFlight is an option initializes a new TCP forwarder with the given
// maximum number of in-flight connection attempts. Once the maximum is reached
// new incoming connection requests will be ignored.
//
// Default is 512.
func WithTCPMaxInFlight(maxInFlight int) NetworkOpts {
	return func(n *networkOpts) {
		n.TCPMaxInFlight = maxInFlight
	}
}

// WithTCPReceiveBufferSize is an option when use initialize a new TCP forwarder with
// the given buffer size of TCP Recieve window.
//
// Default is 1048576.
func WithTCPReceiveBufferSize(rcvWnd int) NetworkOpts {
	return func(n *networkOpts) {
		n.TCPReceiveBufferSize = rcvWnd
	}
}

// WithLogger is an option for debug logging which is using the given logger.
//
// Default is nil.
func WithLogger(logger *slog.Logger) NetworkOpts {
	return func(n *networkOpts) {
		n.Logger = logger
	}
}

func WithDialerOut(
	dialOut func(ctx context.Context, network string, addr string) (net.Conn, error),
) NetworkOpts {
	return func(n *networkOpts) {
		n.DialOut = dialOut
	}
}

// Network is network for any virtual machines.
type Network struct {
	stack                *stack.Stack
	pool                 *bytePool
	tcpMaxInFlight       int
	tcpReceiveBufferSize int
	gateway              *Gateway
	logger               *slog.Logger
	subnet               tcpip.Subnet
	shutdown             func()

	dialOut func(ctx context.Context, network string, addr string) (net.Conn, error)
}

// New initializes new network stack with a network gateway.
// The first IP in the specified cidr range is treated as the gateway IP address.
//
// For example, assume the value specified for cidr is "192.168.127.0/24". The
// first IP address in this range is "192.168.127.0" and last is "192.168.127.255.
// These IP addresses are not used for assignment. Because In general the first address
// is the network identification and the last one is the broadcast. Thus, the first IP
// address used for assignment here is "192.168.127.1". This is for the Gateway. Subsequent
// IP addresses will be assigned to the Link Device.
func New(cidr string, opts ...NetworkOpts) (*Network, error) {
	opt := &networkOpts{
		MTU: 1500,
		// "7a:5b:10:21:90:e3"
		// generated by https://go.dev/play/p/9XRn_wtY2go
		MACAddress: net.HardwareAddr{
			0x7a, 0x5b, 0x10, 0x21, 0x90, 0xe3,
		},
		TCPMaxInFlight:       512,
		TCPReceiveBufferSize: tcp.DefaultReceiveBufferSize,
		Logger:               slog.New(&nopHandler{}), // no output
		DialOut:              defaultDialOut,
	}
	for _, optFunc := range opts {
		optFunc(opt)
	}

	db, err := newLeaseDB(cidr)
	if err != nil {
		return nil, err
	}

	pool := newBytePool(int(opt.MTU + header.EthernetMaximumSize))

	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}

	gw, err := newGateway(opt.MACAddress, &gatewayOption{
		MTU:       opt.MTU,
		PcapFile:  opt.PcapFile,
		Pool:      pool,
		Logger:    opt.Logger,
		Leases:    db,
		DNSConfig: opt.DNSConfig,
		Subnet:    subnet,
	})
	if err != nil {
		return nil, err
	}

	s, err := createNetworkStack(gw.endpoint)
	if err != nil {
		return nil, err
	}

	gatewayIPv4 := tcpip.AddrFromSlice(gw.ipv4)
	addAddress(s, gatewayIPv4)

	ctx, cancel := context.WithCancel(context.Background())

	nt := &Network{
		stack:                s,
		pool:                 pool,
		tcpMaxInFlight:       opt.TCPMaxInFlight,
		tcpReceiveBufferSize: opt.TCPReceiveBufferSize,
		logger:               opt.Logger,
		gateway:              gw,
		subnet:               gw.endpoint.subnet,
		shutdown:             cancel,
		dialOut:              opt.DialOut,
	}

	err = gw.serveDNS4Server(ctx, s, &tcpip.FullAddress{
		NIC:  nicID,
		Addr: gatewayIPv4,
		Port: 53,
	})
	if err != nil {
		return nil, err
	}

	nt.setUDPForwarder(ctx)
	nt.setTCPForwarder(ctx)

	return nt, nil
}

func (nt *Network) Shutdown() {
	nt.stack.Destroy()
	nt.shutdown()
}

// Gateway returns default gateway in this network stack.
func (nt *Network) Gateway() *Gateway { return nt.gateway }

func (nt *Network) DialContextTCP(ctx context.Context, network string, laddr, raddr *net.TCPAddr) (net.Conn, error) {
	proto, err := protocolNumberFromNetwork(network)
	if err != nil {
		return nil, err
	}
	var laddrGonet tcpip.FullAddress
	if laddr != nil {
		laddrGonet = tcpip.FullAddress{Addr: tcpip.AddrFromSlice(laddr.IP), Port: uint16(laddr.Port)}
	}
	var raddrGonet tcpip.FullAddress
	if raddr != nil {
		raddrGonet = tcpip.FullAddress{Addr: tcpip.AddrFromSlice(raddr.IP), Port: uint16(raddr.Port)}
	}
	return gonet.DialTCPWithBind(ctx, nt.stack, laddrGonet, raddrGonet, proto)
}

func (nt *Network) DialUDP(ctx context.Context, network string, laddr, raddr *net.UDPAddr) (net.Conn, error) {
	proto, err := protocolNumberFromNetwork(network)
	if err != nil {
		return nil, err
	}
	var laddrGonet *tcpip.FullAddress
	if laddr != nil {
		laddrGonet = &tcpip.FullAddress{Addr: tcpip.AddrFromSlice(laddr.IP), Port: uint16(laddr.Port)}
	}
	var raddrGonet *tcpip.FullAddress
	if raddr != nil {
		raddrGonet = &tcpip.FullAddress{Addr: tcpip.AddrFromSlice(raddr.IP), Port: uint16(raddr.Port)}
	}
	return gonet.DialUDP(nt.stack, laddrGonet, raddrGonet, proto)
}

func (nt *Network) ListenTCP(network string, laddr *net.TCPAddr) (net.Listener, error) {
	proto, err := protocolNumberFromNetwork(network)
	if err != nil {
		return nil, err
	}
	var laddrGonet tcpip.FullAddress
	if laddr != nil {
		laddrGonet = tcpip.FullAddress{Addr: tcpip.AddrFromSlice(laddr.IP), Port: uint16(laddr.Port)}
	}
	return gonet.ListenTCP(nt.stack, laddrGonet, proto)
}

func (nt *Network) tcpIncomingForward(guestIPv4 net.IP, guestPort, hostPort int) (func() error, error) {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(hostPort))
	if err != nil {
		return nil, err
	}

	proxy := fmt.Sprintf(
		"127.0.0.1:%d <-> %s:%d",
		hostPort,
		guestIPv4.String(), guestPort,
	)
	nt.logger.Info(
		"start relay incoming TCP forward",
		slog.String("forward", proxy),
	)

	go func() {
		defer ln.Close()

		for {
			conn, err := ln.Accept()
			if err != nil {
				nt.logger.Error(
					"failed to accept connection in incoming TCP forward",
					"err", err,
					slog.String("forward", proxy),
				)
				return
			}
			nt.logger.Info(
				"forwarding host TCP connection",
				slog.Any("remote", conn.RemoteAddr()),
			)

			go func() {
				defer conn.Close()

				conn1, err := gonet.DialTCP(nt.stack, tcpip.FullAddress{
					NIC:  nicID,
					Addr: tcpip.AddrFromSlice(guestIPv4),
					Port: uint16(guestPort),
				}, ipv4.ProtocolNumber)
				if err != nil {
					nt.logger.Error(
						"failed to dial connection to upstream in incoming TCP forward",
						"err", err,
						slog.String("forward", proxy),
					)
					return
				}
				defer conn1.Close()

				if err := nt.pool.tcpRelay(conn.(*net.TCPConn), conn1); err != nil {
					nt.logger.Error(
						"failed to relay the connection in incoming TCP forward",
						"err", err,
						slog.String("forward", proxy),
					)
				}
			}()
		}
	}()

	return ln.Close, nil
}

func createNetworkStack(ep stack.LinkEndpoint) (*stack.Stack, error) {
	s, err := createBaseNetStack()
	if err != nil {
		return nil, err
	}

	if err := s.CreateNIC(nicID, ep); err != nil {
		return nil, fmt.Errorf("could not create netstack NIC: %v", err)
	}

	s.SetRouteTable([]tcpip.Route{
		{
			Destination: header.IPv4EmptySubnet,
			// Gateway:     gatewayIPv4,
			NIC: nicID,
		},
	})

	// Enable to forward transport layer data.
	s.SetPromiscuousMode(nicID, true)

	// Enable to allow endpoints to bind to any address in the NIC.
	s.SetSpoofing(nicID, true)

	return s, nil
}

// LinkDevice is a link device with vmnet network.
type LinkDevice struct {
	dev       *os.File
	ipv4      net.IP
	hwAddress net.HardwareAddr
	closeFunc func() error
	pool      *bytePool
}

type linkDeviceOpts struct {
	SendBufferSize     int
	TCPIncomingForward map[int]int
}

// LinkDeviceOpts is a optional type for NewLinkDevice.
type LinkDeviceOpts func(*linkDeviceOpts)

// WithSendBufferSize is an option sets SO_SNDBUF size between
// ethernet device and guest system. And sets SO_RCVBUF size
// four times of SO_SNDBUF. the default SO_SNDBUF is 131072.
func WithSendBufferSize(bufSize int) LinkDeviceOpts {
	return func(edo *linkDeviceOpts) {
		edo.SendBufferSize = bufSize
	}
}

// WithTCPIncomingForward is an option to set TCP forward from host machine to guest machine.
// For example, if you want to connect from the host machine to the guest machine via ssh,
// configure as follows:
//
// `WithTCPIncomingForward(8888, 22)` then you can ssh to the guest OS via 127.0.0.1:8888
//
// This option can be applied multiple times.
func WithTCPIncomingForward(hostPort, guestPort int) LinkDeviceOpts {
	return func(edo *linkDeviceOpts) {
		if edo.TCPIncomingForward == nil {
			edo.TCPIncomingForward = make(map[int]int)
		}
		edo.TCPIncomingForward[hostPort] = guestPort
	}
}

// NewLinkDevice creates a new link device which is connected with vmnet network.
func (nt *Network) NewLinkDevice(hwAddr net.HardwareAddr, opts ...LinkDeviceOpts) (*LinkDevice, error) {
	o := linkDeviceOpts{
		// net.inet.tcp.sendspace: 131072 (sysctl net.inet.tcp.sendspace)
		SendBufferSize: 128 * 1024,
	}
	for _, optFunc := range opts {
		optFunc(&o)
	}

	deviceIPv4, err := nt.gateway.leaseDB.LeaseIP(hwAddr)
	if err != nil {
		return nil, err
	}
	dev, network, err := socketPair(o.SendBufferSize, o.SendBufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create socket pair: %w", err)
	}
	closer := &wrappedConn{closers: []func() error{
		dev.Close,
		network.Close,
	}}

	ethConn, err := net.FileConn(network)
	if err != nil {
		closer.Close()
		return nil, fmt.Errorf("failed to make a connection: %w", err)
	}

	closer.Conn = ethConn
	closer.closers = append(closer.closers, ethConn.Close)

	deviceIPv4Addr := tcpip.AddrFromSlice(deviceIPv4.To4())
	nt.gateway.endpoint.RegisterConn(
		deviceIPv4Addr,
		tcpip.LinkAddress(hwAddr),
		// we pass the closer with associated ethConn, because then if the endpoint
		// is destroyed, everything is closed along with it
		closer,
	)

	for hostPort, guestPort := range o.TCPIncomingForward {
		close, err := nt.tcpIncomingForward(deviceIPv4, guestPort, hostPort)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to listen tcp forward proxy 127.0.0.1:%d <-> %s:%d: %w",
				hostPort,
				deviceIPv4.String(), guestPort,
				err,
			)
		}

		closer.closers = append(closer.closers, close)
	}

	return &LinkDevice{
		dev:       dev,
		ipv4:      deviceIPv4,
		closeFunc: closer.Close,
		pool:      nt.pool,
		hwAddress: hwAddr,
	}, nil
}

// File returns *os.File for this device.
func (l *LinkDevice) File() *os.File { return l.dev }

// IPv4 returns ipv4 address that you can use in the guest OS.
func (l *LinkDevice) IPv4() net.IP { return l.ipv4 }

// MACAddress returns MAC address.
func (l *LinkDevice) MACAddress() net.HardwareAddr { return l.hwAddress }

// Close closes this device and LinkDevice connection.
func (l *LinkDevice) Close() error { return l.closeFunc() }

type wrappedConn struct {
	net.Conn
	closers []func() error
}

func (mt *wrappedConn) Close() error {
	var err error
	for _, c := range mt.closers {
		if cerr := c(); cerr != nil && err == nil {
			err = cerr
		}
	}

	return err
}

func protocolNumberFromNetwork(network string) (tcpip.NetworkProtocolNumber, error) {
	switch network {
	case "tcp", "udp":
		return ipv4.ProtocolNumber, nil
	case "tcp6", "udp6":
		return ipv6.ProtocolNumber, nil
	default:
		return 0, fmt.Errorf("unknown network %s", network)
	}
}

func defaultDialOut(ctx context.Context, network string, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return dialer.DialContext(ctx, network, addr)
}
