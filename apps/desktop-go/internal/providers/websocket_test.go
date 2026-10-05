package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
)

func wsFixture(first byte, payload []byte) []byte {
	frame := []byte{first}
	switch {
	case len(payload) < 126:
		frame = append(frame, byte(len(payload)))
	case len(payload) <= 65535:
		frame = append(frame, 126, 0, 0)
		binary.BigEndian.PutUint16(frame[len(frame)-2:], uint16(len(payload)))
	default:
		frame = append(frame, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(frame[len(frame)-8:], uint64(len(payload)))
	}
	return append(frame, payload...)
}

func wsFixtureReader(frame []byte) *wsReader {
	return &wsReader{reader: bufio.NewReader(bytes.NewReader(frame))}
}

func TestWebSocketRejectsProtocolViolations(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{"rsv", wsFixture(0xc1, []byte("x"))},
		{"orphan continuation", wsFixture(0x80, []byte("x"))},
		{"binary", wsFixture(0x82, []byte("x"))},
		{"fragmented control", wsFixture(0x09, []byte("x"))},
		{"invalid utf8", wsFixture(0x81, []byte{0xff})},
		{"invalid close", wsFixture(0x88, []byte{1})},
		{"server 1010 close", wsFixture(0x88, []byte{0x03, 0xf2})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := wsFixtureReader(test.frame).readText(); err == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}
}

func TestWebSocketRejectsMalformedHeaderLine(t *testing.T) {
	if _, err := readWebSocketHeaderLine(bufio.NewReader(strings.NewReader("Upgrade: websocket\n"))); err == nil {
		t.Fatal("bare LF header accepted")
	}
}

func TestWebSocketRejectsObsoleteFoldBeforeTrimming(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil || line == "\r\n" {
				break
			}
		}
		_, _ = fmt.Fprint(conn, "HTTP/1.1 101 Switching Protocols\r\n Upgrade: websocket\r\n\r\n")
	}()
	_, err = openWebSocket(context.Background(), "http://"+listener.Addr().String(), "/events")
	if err == nil || !strings.Contains(err.Error(), "obsolete header folding") {
		t.Fatalf("err=%v", err)
	}
}

func TestWebSocketFragmentedText(t *testing.T) {
	frames := append(wsFixture(0x01, []byte("hel")), wsFixture(0x80, []byte("lo"))...)
	got, err := wsFixtureReader(frames).readText()
	if err != nil || string(got) != "hello" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestWebSocketRejectsNonLoopbackBeforeDial(t *testing.T) {
	_, err := openWebSocket(context.Background(), "http://192.0.2.1:80", "/events")
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("err=%v", err)
	}
}
