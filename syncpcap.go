package vmnet

import (
	"sync"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
)

type pcapWriter interface {
	WriteFileHeader(snaplen uint32, linktype layers.LinkType) error
	WritePacket(ci gopacket.CaptureInfo, data []byte) error
}

type syncPCAPWriter struct {
	mu sync.Mutex
	w  *pcapgo.Writer
}

func (w *syncPCAPWriter) WriteFileHeader(snaplen uint32, linktype layers.LinkType) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.WriteFileHeader(snaplen, linktype)
}

func (w *syncPCAPWriter) WritePacket(ci gopacket.CaptureInfo, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.WritePacket(ci, data)
}
