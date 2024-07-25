package vmnet

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"

	"golang.org/x/sync/errgroup"
)

type bytePool struct {
	pool sync.Pool
}

func newBytePool(mtu int) *bytePool {
	return &bytePool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, mtu)
			},
		},
	}
}

func (b *bytePool) getBytes() []byte    { return b.pool.Get().([]byte) }
func (b *bytePool) putBytes(raw []byte) { b.pool.Put(raw) }

func (b *bytePool) tcpRelay(rw1, rw2 io.ReadWriteCloser) error {
	defer rw1.Close()
	defer rw2.Close()

	copyBuffer := func(dst io.Writer, src io.Reader) error {
		buf := b.getBytes()
		defer b.putBytes(buf)

		defer func() {
			if v, ok := dst.(interface {
				CloseWrite() error
			}); ok {
				v.CloseWrite()
			}
			if v, ok := src.(interface {
				CloseRead() error
			}); ok {
				v.CloseRead()
			}
		}()

		_, err := io.CopyBuffer(dst, src, buf)
		return err
	}

	var eg errgroup.Group
	eg.Go(func() error {
		return copyBuffer(rw1, rw2)
	})
	eg.Go(func() error {
		return copyBuffer(rw2, rw1)
	})
	err := eg.Wait()

	var terr interface {
		error
		Timeout() bool
	}
	if errors.As(err, &terr) && terr.Timeout() {
		return nil
	}
	return err
}

func (b *bytePool) udpRelay(
	ctx context.Context,
	logger *slog.Logger,
	dst net.Conn,
	src net.Conn,
	cancel, extend func(),
) {
	defer cancel()

	buf := b.getBytes()
	defer b.putBytes(buf)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := src.Read(buf)
		if err == io.EOF {
			return
		}
		if err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Info(
				"failed to read packet",
				"err", err,
			)
			return
		}

		_, err = dst.Write(buf[:n])
		if err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Info(
				"failed to write packet",
				"err", err,
			)
			return
		}

		extend()
	}
}
