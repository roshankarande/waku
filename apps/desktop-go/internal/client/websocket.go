package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsConn struct {
	net.Conn
	r       *bufio.Reader
	writeMu sync.Mutex
}

func daemonURL(address string) (*url.URL, error) {
	if !strings.Contains(address, "://") {
		address = "ws://" + address
	}
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("invalid Waku daemon address: %w", err)
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, fmt.Errorf("unsupported daemon URL scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("daemon URL has no host")
	}
	u.Path, u.RawPath, u.RawQuery, u.Fragment = "/v1", "", "", ""
	return u, nil
}

func dialWebSocket(ctx context.Context, address, origin string) (*wsConn, error) {
	u, err := daemonURL(address)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		if u.Scheme == "wss" {
			host = net.JoinHostPort(u.Hostname(), "443")
		} else {
			host = net.JoinHostPort(u.Hostname(), "80")
		}
	}
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, fmt.Errorf("connect to Waku daemon: %w", err)
	}
	if u.Scheme == "wss" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("secure Waku daemon handshake: %w", err)
		}
		conn = tlsConn
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce[:])
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{
		"Upgrade": {"websocket"}, "Connection": {"Upgrade"}, "Sec-Websocket-Key": {key},
		"Sec-Websocket-Version": {"13"}, "X-Waku-Client": {"native"},
	}}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Waku WebSocket handshake: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("Waku WebSocket handshake: %s", resp.Status)
	}
	if !strings.EqualFold(strings.TrimSpace(resp.Header.Get("Upgrade")), "websocket") || !headerHasToken(resp.Header.Values("Connection"), "upgrade") {
		conn.Close()
		return nil, errors.New("Waku WebSocket handshake omitted required Upgrade/Connection headers")
	}
	if _, present := resp.Header[http.CanonicalHeaderKey("Sec-WebSocket-Extensions")]; present {
		conn.Close()
		return nil, errors.New("Waku WebSocket handshake returned unsolicited extensions")
	}
	if _, present := resp.Header[http.CanonicalHeaderKey("Sec-WebSocket-Protocol")]; present {
		conn.Close()
		return nil, errors.New("Waku WebSocket handshake returned an unsolicited subprotocol")
	}
	want := sha1.Sum([]byte(key + websocketGUID))
	if resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(want[:]) {
		conn.Close()
		return nil, errors.New("Waku WebSocket handshake returned an invalid accept key")
	}
	conn.SetDeadline(time.Time{})
	return &wsConn{Conn: conn, r: reader}, nil
}

func (c *wsConn) writeJSON(payload []byte) error { return c.writeFrame(1, payload) }

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if len(payload) > protocol.MaxWireMessageBytes {
		return errors.New("WebSocket payload exceeds protocol maximum")
	}
	if opcode&0x8 != 0 && len(payload) > 125 {
		return errors.New("WebSocket control frame exceeds 125 bytes")
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	header := []byte{0x80 | opcode}
	switch n := len(payload); {
	case n < 126:
		header = append(header, 0x80|byte(n))
	case n <= 65535:
		header = append(header, 0x80|126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(n))
	default:
		header = append(header, 0x80|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[len(header)-8:], uint64(n))
	}
	header = append(header, mask[:]...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	if err := writeAll(c.Conn, header); err != nil {
		return err
	}
	return writeAll(c.Conn, masked)
}

func writeAll(w io.Writer, payload []byte) error {
	for len(payload) != 0 {
		n, err := w.Write(payload)
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

func (c *wsConn) readJSON(limit int) ([]byte, error) {
	var message bytes.Buffer
	started := false
	for {
		remaining := limit - message.Len()
		if remaining < 0 {
			return nil, errors.New("WebSocket message exceeds protocol maximum")
		}
		opcode, fin, payload, err := c.readFrame(remaining)
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 8:
			if !validClosePayload(payload) {
				return nil, errors.New("invalid WebSocket close payload")
			}
			// RFC 6455 requires an endpoint that receives Close to echo Close.
			if err := c.writeFrame(8, payload); err != nil {
				return nil, err
			}
			return nil, io.EOF
		case 9:
			if err := c.writeFrame(10, payload); err != nil {
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
			return nil, fmt.Errorf("unsupported WebSocket opcode %d", opcode)
		}
		message.Write(payload)
		if message.Len() > limit {
			return nil, errors.New("WebSocket message exceeds protocol maximum")
		}
		if fin {
			if !utf8.Valid(message.Bytes()) {
				return nil, errors.New("WebSocket text message is not valid UTF-8")
			}
			return message.Bytes(), nil
		}
	}
}

func (c *wsConn) readFrame(limit int) (opcode byte, fin bool, payload []byte, err error) {
	var first [2]byte
	if _, err = io.ReadFull(c.r, first[:]); err != nil {
		return
	}
	fin, opcode = first[0]&0x80 != 0, first[0]&0x0f
	if first[0]&0x70 != 0 {
		err = errors.New("WebSocket extensions were not negotiated")
		return
	}
	if first[1]&0x80 != 0 {
		err = errors.New("server sent a masked WebSocket frame")
		return
	}
	n := uint64(first[1] & 0x7f)
	if n == 126 {
		var b [2]byte
		if _, err = io.ReadFull(c.r, b[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
		if n < 126 {
			err = errors.New("non-minimal WebSocket frame length")
			return
		}
	}
	if n == 127 {
		var b [8]byte
		if _, err = io.ReadFull(c.r, b[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(b[:])
		if n < 65536 || n>>63 != 0 {
			err = errors.New("invalid WebSocket 64-bit frame length")
			return
		}
	}
	if opcode&0x8 != 0 && (!fin || n > 125) {
		err = errors.New("invalid fragmented or oversized WebSocket control frame")
		return
	}
	if opcode&0x8 != 0 && opcode != 8 && opcode != 9 && opcode != 10 {
		err = errors.New("reserved WebSocket control opcode")
		return
	}
	if opcode == 8 && n == 1 {
		err = errors.New("invalid one-byte WebSocket close payload")
		return
	}
	payloadLimit := limit
	if opcode&0x8 != 0 {
		payloadLimit = 125
	}
	if n > uint64(payloadLimit) || n > protocol.MaxWireMessageBytes {
		err = errors.New("WebSocket frame exceeds protocol maximum")
		return
	}
	payload = make([]byte, int(n))
	_, err = io.ReadFull(c.r, payload)
	return
}

func headerHasToken(values []string, want string) bool {
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func validClosePayload(payload []byte) bool {
	if len(payload) == 0 {
		return true
	}
	if len(payload) < 2 {
		return false
	}
	code := binary.BigEndian.Uint16(payload[:2])
	if code < 1000 || code >= 5000 || code == 1004 || code == 1005 || code == 1006 || code == 1015 || (code >= 1016 && code < 3000) {
		return false
	}
	return utf8.Valid(payload[2:])
}
