package providers

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	maxWebSocketMessageBytes = 32 << 20
	maxWebSocketHeaderBytes  = 64 << 10
	maxWebSocketHeaderLine   = 8 << 10
)

type wsReader struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex
}

func openWebSocket(ctx context.Context, base, path string) (*wsReader, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" {
		return nil, errors.New("provider websocket requires a loopback HTTP endpoint")
	}
	hostName := u.Hostname()
	ip := net.ParseIP(hostName)
	if ip == nil || !ip.IsLoopback() {
		return nil, errors.New("provider websocket endpoint is not a loopback IP")
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	host := net.JoinHostPort(hostName, port)
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		_ = conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	request := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, u.Host, key)
	if _, err = io.WriteString(conn, request); err != nil {
		_ = conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	status, err := readWebSocketHeaderLine(reader)
	fields := strings.Fields(status)
	if err != nil || len(fields) < 2 || fields[0] != "HTTP/1.1" || fields[1] != "101" {
		_ = conn.Close()
		return nil, fmt.Errorf("websocket upgrade failed: %s", strings.TrimSpace(status))
	}
	headers := make(map[string]string)
	headerBytes := len(status) + 2
	for {
		line, readErr := readWebSocketHeaderLine(reader)
		if readErr != nil {
			_ = conn.Close()
			return nil, readErr
		}
		headerBytes += len(line) + 2
		if headerBytes > maxWebSocketHeaderBytes {
			_ = conn.Close()
			return nil, errors.New("websocket response headers exceed limit")
		}
		if line == "" {
			break
		}
		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			_ = conn.Close()
			return nil, errors.New("websocket response uses obsolete header folding")
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 || !validHTTPHeaderName(parts[0]) {
			_ = conn.Close()
			return nil, errors.New("websocket response has malformed header")
		}
		name, value := strings.ToLower(parts[0]), strings.TrimSpace(parts[1])
		if previous := headers[name]; previous != "" {
			headers[name] = previous + "," + value
		} else {
			headers[name] = value
		}
	}
	if !headerHasToken(headers["upgrade"], "websocket") || !headerHasToken(headers["connection"], "upgrade") {
		_ = conn.Close()
		return nil, errors.New("websocket response is missing upgrade headers")
	}
	if headers["sec-websocket-extensions"] != "" {
		_ = conn.Close()
		return nil, errors.New("websocket extensions were not requested")
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if headers["sec-websocket-accept"] != base64.StdEncoding.EncodeToString(sum[:]) {
		_ = conn.Close()
		return nil, errors.New("websocket accept mismatch")
	}
	w := &wsReader{conn: conn, reader: reader}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	return w, nil
}

func readWebSocketHeaderLine(reader *bufio.Reader) (string, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > maxWebSocketHeaderLine {
			return "", errors.New("websocket header line exceeds limit")
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return "", err
		}
		if len(line) < 2 || line[len(line)-2] != '\r' {
			return "", errors.New("websocket header uses invalid line ending")
		}
		return string(line[:len(line)-2]), nil
	}
}

func validHTTPHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range []byte(name) {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

func headerHasToken(value, token string) bool {
	for _, field := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(field), token) {
			return true
		}
	}
	return false
}

func (w *wsReader) readText() (jsonText []byte, err error) {
	var data []byte
	fragmented := false
	for {
		b1, e := w.reader.ReadByte()
		if e != nil {
			return nil, e
		}
		b2, e := w.reader.ReadByte()
		if e != nil {
			return nil, e
		}
		if b1&0x70 != 0 {
			return nil, errors.New("websocket frame uses unnegotiated RSV bits")
		}
		fin, opcode := b1&0x80 != 0, b1&0x0f
		if b2&0x80 != 0 {
			return nil, errors.New("server websocket frame is masked")
		}
		length := uint64(b2 & 0x7f)
		if length == 126 {
			var v uint16
			if e = binary.Read(w.reader, binary.BigEndian, &v); e != nil {
				return nil, e
			}
			length = uint64(v)
			if length < 126 {
				return nil, errors.New("websocket frame has non-minimal length")
			}
		} else if length == 127 {
			if e = binary.Read(w.reader, binary.BigEndian, &length); e != nil {
				return nil, e
			}
			if length&(uint64(1)<<63) != 0 || length < 65536 {
				return nil, errors.New("websocket frame has invalid 64-bit length")
			}
		}
		control := opcode >= 8
		if control && (!fin || length > 125) {
			return nil, errors.New("websocket control frame is fragmented or oversized")
		}
		if length > maxWebSocketMessageBytes || uint64(len(data))+length > maxWebSocketMessageBytes {
			return nil, errors.New("websocket message exceeds limit")
		}
		payload := make([]byte, int(length))
		if _, e = io.ReadFull(w.reader, payload); e != nil {
			return nil, e
		}
		switch opcode {
		case 0:
			if !fragmented {
				return nil, errors.New("unexpected websocket continuation frame")
			}
			data = append(data, payload...)
			if fin {
				if !utf8.Valid(data) {
					return nil, errors.New("websocket text is not UTF-8")
				}
				return data, nil
			}
		case 1:
			if fragmented {
				return nil, errors.New("websocket text frame interrupted fragmented message")
			}
			data = append(data, payload...)
			if fin {
				if !utf8.Valid(data) {
					return nil, errors.New("websocket text is not UTF-8")
				}
				return data, nil
			}
			fragmented = true
		case 2:
			return nil, errors.New("unexpected websocket binary frame")
		case 8:
			if len(payload) == 1 {
				return nil, errors.New("websocket close frame has invalid payload")
			}
			if len(payload) >= 2 {
				code := binary.BigEndian.Uint16(payload[:2])
				if !validWebSocketCloseCode(code) || !utf8.Valid(payload[2:]) {
					return nil, errors.New("websocket close frame is invalid")
				}
			}
			_ = w.writeControl(8, payload)
			return nil, io.EOF
		case 9:
			_ = w.writeControl(10, payload)
		case 10:
		default:
			return nil, fmt.Errorf("unsupported websocket opcode %d", opcode)
		}
	}
}

func validWebSocketCloseCode(code uint16) bool {
	if code >= 3000 && code <= 4999 {
		return true
	}
	switch code {
	case 1000, 1001, 1002, 1003, 1007, 1008, 1009, 1011:
		return true
	default:
		return false
	}
}
func (w *wsReader) writeControl(opcode byte, payload []byte) error {
	if len(payload) > 125 {
		payload = payload[:125]
	}
	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	frame := []byte{0x80 | opcode, 0x80 | byte(len(payload))}
	frame = append(frame, mask...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(frame) > 0 {
		n, err := w.conn.Write(frame)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(frame) {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

func (w *wsReader) Close() error { return w.conn.Close() }
