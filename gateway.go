package vmnet

import (
	"fmt"
	"net"
	"net/netip"
	"os"

	"github.com/miekg/dns"
	"golang.org/x/exp/slog"
	"gvisor.dev/gvisor/pkg/tcpip"
)

type gatewayOption struct {
	MTU       uint32
	PcapFile  *os.File
	Pool      *bytePool
	Logger    *slog.Logger
	Leases    *leaseDB
	DNSConfig *DNSConfig
}

func (o *gatewayOption) dnsConfigTidy(gatewayIP net.IP) error {
	config, err := parseResolvConfFile(resolvConfPath)
	if err != nil {
		return err
	}
	if o.DNSConfig == nil {
		o.DNSConfig = config
	}
	if len(o.DNSConfig.Nameservers) == 0 {
		o.DNSConfig.Nameservers = config.Nameservers
	}

	if len(o.DNSConfig.SearchDomains) == 0 {
		o.DNSConfig.SearchDomains = config.SearchDomains
	}
	// overwrite to use fqdn format in keys.
	for mayfqdn, addr := range o.DNSConfig.StaticRecords {
		delete(o.DNSConfig.StaticRecords, mayfqdn)
		o.DNSConfig.StaticRecords[dns.Fqdn(mayfqdn)] = addr
	}
	return nil
}

// Gateway is a network gateway.
type Gateway struct {
	ipv4      net.IP
	hwAddress net.HardwareAddr
	endpoint  *endpoint
	leaseDB   *leaseDB
	dnsConfig *DNSConfig
	pool      *bytePool
	logger    *slog.Logger
}

func newGateway(hwAddr net.HardwareAddr, opt *gatewayOption) (*Gateway, error) {
	gatewayIP, err := opt.Leases.LeaseIP(hwAddr)
	if err != nil {
		return nil, err
	}
	if err := opt.dnsConfigTidy(gatewayIP); err != nil {
		return nil, err
	}

	ep, err := newGatewayEndpoint(gatewayEndpointOption{
		MTU:     opt.MTU,
		Address: tcpip.LinkAddress(hwAddr),
		ClosedFunc: func(ipAddr tcpip.Address, err error) {
			opt.Logger.Error(
				"closed ehternet endpoint", err,
				slog.String("IP Address", ipAddr.String()),
			)
		},
		Writer: opt.PcapFile,
		Pool:   opt.Pool,
		Logger: opt.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create an default gateway endpoint: %w", err)
	}
	return &Gateway{
		ipv4:      gatewayIP,
		endpoint:  ep,
		hwAddress: hwAddr,
		leaseDB:   opt.Leases,
		dnsConfig: opt.DNSConfig,
		pool:      opt.Pool,
		logger:    opt.Logger,
	}, nil
}

// IPv4 returns IPv4 address.
func (gw *Gateway) IPv4() net.IP { return gw.ipv4 }

// MACAddress returns MAC address.
func (gw *Gateway) MACAddress() net.HardwareAddr { return gw.hwAddress }

// DHCPLease is a lease info of DHCP.
type DHCPLease struct {
	net.HardwareAddr
	netip.Addr
}

// LeaseIP returns DHCP leases. If there is a HardwareAddr, it is leased.
func (gw *Gateway) LeaseIP() []DHCPLease {
	ret := make([]DHCPLease, len(gw.leaseDB.leases))
	for i, v := range gw.leaseDB.leases {
		ret[i] = DHCPLease{
			HardwareAddr: v.hwAddr,
			Addr:         v.ipAddr,
		}
	}
	return ret
}
