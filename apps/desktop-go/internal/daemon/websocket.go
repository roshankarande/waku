package daemon

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"unicode/utf8"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

type serverWebSocket struct {
	net.Conn
	reader *bufio.Reader
	mu     sync.Mutex
}

func (w *serverWebSocket) writeJSON(payload []byte) error { return w.writeFrame(1, payload) }

func (w *serverWebSocket) writeFrame(opcode byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(payload) > protocol.MaxWireMessageBytes {
		return errors.New("WebSocket payload exceeds protocol maximum")
	}
	header := []byte{0x80 | opcode}
	switch n := len(payload); {
	case n < 126:
		header = append(header, byte(n))
	case n <= 65535:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(n))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[len(header)-8:], uint64(n))
	}
	if err := writeAllServer(w.Conn, header); err != nil {
		return err
	}
	return writeAllServer(w.Conn, payload)
}

func writeAllServer(writer io.Writer, payload []byte) error {
	for len(payload) != 0 {
		n, err := writer.Write(payload)
		if n > 0 {
			payload = payload[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (w *serverWebSocket) readJSON(limit int) ([]byte, error) {
	var message bytes.Buffer
	started := false
	for {
		opcode, fin, payload, err := w.readFrame(limit - message.Len())
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 8:
			_ = w.writeFrame(8, payload)
			return nil, io.EOF
		case 9:
			if err := w.writeFrame(10, payload); err != nil {
				return nil, err
			}
			continue
		case 10:
			continue
		case 1:
			if started {
				return nil, errors.New("new text frame during fragmented message")
			}
			started = true
		case 0:
			if !started {
				return nil, errors.New("unexpected continuation frame")
			}
		default:
			return nil, errors.New("unsupported WebSocket opcode")
		}
		message.Write(payload)
		if message.Len() > limit {
			return nil, errors.New("WebSocket message exceeds protocol maximum")
		}
		if fin {
			if !utf8.Valid(message.Bytes()) {
				return nil, errors.New("WebSocket text is not UTF-8")
			}
			return append([]byte(nil), message.Bytes()...), nil
		}
	}
}

func (w *serverWebSocket) readFrame(limit int) (opcode byte, fin bool, payload []byte, err error) {
	var first [2]byte
	if _, err = io.ReadFull(w.reader, first[:]); err != nil {
		return
	}
	fin, opcode = first[0]&0x80 != 0, first[0]&0x0f
	if first[0]&0x70 != 0 {
		err = errors.New("WebSocket extensions were not negotiated")
		return
	}
	if first[1]&0x80 == 0 {
		err = errors.New("client WebSocket frame is not masked")
		return
	}
	n := uint64(first[1] & 0x7f)
	if n == 126 {
		var size [2]byte
		if _, err = io.ReadFull(w.reader, size[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(size[:]))
		if n < 126 {
			err = errors.New("non-minimal WebSocket length")
			return
		}
	} else if n == 127 {
		var size [8]byte
		if _, err = io.ReadFull(w.reader, size[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(size[:])
		if n < 65536 || n>>63 != 0 {
			err = errors.New("invalid WebSocket length")
			return
		}
	}
	if opcode&8 != 0 && (!fin || n > 125) {
		err = errors.New("invalid WebSocket control frame")
		return
	}
	if n > uint64(limit) || n > protocol.MaxWireMessageBytes {
		err = errors.New("WebSocket frame exceeds protocol maximum")
		return
	}
	var mask [4]byte
	if _, err = io.ReadFull(w.reader, mask[:]); err != nil {
		return
	}
	payload = make([]byte, int(n))
	if _, err = io.ReadFull(w.reader, payload); err != nil {
		return
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	if opcode == 8 && len(payload) == 1 {
		err = errors.New("invalid WebSocket close payload")
	}
	return
}
