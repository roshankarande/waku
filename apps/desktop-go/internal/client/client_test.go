package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

type testServer struct {
	listener net.Listener
	address  string
	received chan map[string]any
	once     sync.Once
}

func newTestServer(t *testing.T, handler func(net.Conn, *bufio.Reader, chan map[string]any)) *testServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &testServer{listener: listener, address: listener.Addr().String(), received: make(chan map[string]any, 16)}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		reader := bufio.NewReader(conn)
		req, err := http.ReadRequest(reader)
		if err != nil {
			conn.Close()
			return
		}
		key := req.Header.Get("Sec-WebSocket-Key")
		accept := sha1.Sum([]byte(key + websocketGUID))
		_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(accept[:])+"\r\n\r\n")
		handler(conn, reader, s.received)
	}()
	t.Cleanup(func() { s.Close() })
	return s
}
func (s *testServer) Close() { s.once.Do(func() { s.listener.Close() }) }

func readClientJSON(reader *bufio.Reader) (map[string]any, error) {
	first := make([]byte, 2)
	if _, err := io.ReadFull(reader, first); err != nil {
		return nil, err
	}
	n := uint64(first[1] & 0x7f)
	if n == 126 {
		b := make([]byte, 2)
		io.ReadFull(reader, b)
		n = uint64(binary.BigEndian.Uint16(b))
	} else if n == 127 {
		b := make([]byte, 8)
		io.ReadFull(reader, b)
		n = binary.BigEndian.Uint64(b)
	}
	mask := make([]byte, 4)
	if _, err := io.ReadFull(reader, mask); err != nil {
		return nil, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	var v map[string]any
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.UseNumber()
	err := dec.Decode(&v)
	return v, err
}
func writeServerJSON(conn net.Conn, text string) error {
	b := []byte(text)
	header := []byte{0x81}
	if len(b) < 126 {
		header = append(header, byte(len(b)))
	} else {
		header = append(header, 126, byte(len(b)>>8), byte(len(b)))
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(b)
	return err
}

func connectTest(t *testing.T, s *testServer) *Client {
	t.Helper()
	c, err := Connect(context.Background(), ConnectOptions{Address: s.address, Token: "secret", ClientID: "00000000-0000-4000-8000-000000000001"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseTransport() })
	return c
}

func TestRequestCorrelation(t *testing.T) {
	s := newTestServer(t, func(conn net.Conn, r *bufio.Reader, got chan map[string]any) {
		hello, _ := readClientJSON(r)
		got <- hello
		_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		request, _ := readClientJSON(r)
		got <- request
		id := request["requestId"].(string)
		_ = writeServerJSON(conn, `{"type":"response","requestId":"`+id+`","outcome":{"status":"ok","payload":{"type":"ack"}}}`)
		time.Sleep(time.Second)
	})
	c := connectTest(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload, err := c.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: "getSettings"})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"type":"ack"}` {
		t.Fatalf("payload=%s", payload)
	}
	hello := <-s.received
	if hello["type"] != "hello" || hello["protocolVersion"].(json.Number).String() != "7" {
		t.Fatalf("hello=%v", hello)
	}
}

func TestEventDedupeAndEpochReset(t *testing.T) {
	session := protocol.UUID("00000000-0000-4000-8000-000000000010")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000011")
	s := newTestServer(t, func(conn net.Conn, r *bufio.Reader, _ chan map[string]any) {
		readClientJSON(r)
		writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		for _, e := range []string{
			`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000010","runtimeId":"00000000-0000-4000-8000-000000000011","epoch":"00000000-0000-4000-8000-000000000012","sequence":9,"event":{"kind":"textDelta","payload":"a"}}`,
			`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000010","runtimeId":"00000000-0000-4000-8000-000000000011","epoch":"00000000-0000-4000-8000-000000000012","sequence":9,"event":{"kind":"textDelta","payload":"duplicate"}}`,
			`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000010","runtimeId":"00000000-0000-4000-8000-000000000011","epoch":"00000000-0000-4000-8000-000000000013","sequence":1,"event":{"kind":"reasoningDelta","payload":"new epoch"}}`} {
			writeServerJSON(conn, e)
		}
		time.Sleep(time.Second)
	})
	c := connectTest(t, s)
	events, _ := c.Subscribe(session, runtimeID)
	first := <-events
	second := <-events
	if first.Event.Kind != "textDelta" || second.Event.Kind != "reasoningDelta" {
		t.Fatalf("events=%q,%q", first.Event.Kind, second.Event.Kind)
	}
}

func TestExternalSupervisorNeverSendsShutdown(t *testing.T) {
	types := make(chan string, 4)
	s := newTestServer(t, func(conn net.Conn, r *bufio.Reader, _ chan map[string]any) {
		hello, _ := readClientJSON(r)
		types <- hello["type"].(string)
		writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		for {
			message, err := readClientJSON(r)
			if err != nil {
				return
			}
			types <- message["type"].(string)
		}
	})
	sup, err := ConnectExternal(context.Background(), SupervisorOptions{Address: s.address, Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if sup.Ownership() != ExternalDaemon {
		t.Fatal("wrong ownership")
	}
	sup.Close()
	if got := <-types; got != "hello" {
		t.Fatalf("first=%s", got)
	}
	select {
	case got := <-types:
		t.Fatalf("external close sent %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestExternalSupervisorReconnectsWithReplayCursor(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	secondHello := make(chan map[string]any, 1)
	go func() {
		for connection := 0; connection < 2; connection++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)
			req, err := http.ReadRequest(reader)
			if err != nil {
				conn.Close()
				return
			}
			key := req.Header.Get("Sec-WebSocket-Key")
			accept := sha1.Sum([]byte(key + websocketGUID))
			_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(accept[:])+"\r\n\r\n")
			hello, _ := readClientJSON(reader)
			_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
			if connection == 0 {
				_ = writeServerJSON(conn, `{"type":"event","sessionId":"00000000-0000-4000-8000-000000000020","runtimeId":"00000000-0000-4000-8000-000000000021","epoch":"00000000-0000-4000-8000-000000000022","sequence":9007199254740993,"event":{"kind":"turnStarted","payload":null}}`)
				time.Sleep(20 * time.Millisecond)
				conn.Close()
			} else {
				secondHello <- hello
				time.Sleep(time.Second)
				conn.Close()
			}
		}
	}()
	sup, err := ConnectExternal(context.Background(), SupervisorOptions{Address: listener.Addr().String(), Token: "secret", ReconnectMin: 10 * time.Millisecond, ReconnectMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer sup.Close()
	events, stopEvents := sup.Client().Subscribe("00000000-0000-4000-8000-000000000020", "00000000-0000-4000-8000-000000000021")
	defer stopEvents()
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("first event was not delivered before reconnect")
	}
	updates, unsubscribe := sup.SubscribeClients()
	defer unsubscribe()
	<-updates
	select {
	case <-updates:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not publish replacement client")
	}
	select {
	case hello := <-secondHello:
		resume := hello["resumeFrom"].([]any)
		if len(resume) != 1 {
			t.Fatalf("resume=%v", resume)
		}
		cursor := resume[0].(map[string]any)
		if cursor["sequence"].(json.Number).String() != "9007199254740993" {
			t.Fatalf("cursor=%v", cursor)
		}
	case <-time.After(time.Second):
		t.Fatal("second hello not received")
	}
}

func TestConnectRejectsEmptyTokenAndOversizedHelloBeforeDial(t *testing.T) {
	base := ConnectOptions{Address: "127.0.0.1:1", ClientID: "00000000-0000-4000-8000-000000000001"}
	if _, err := Connect(context.Background(), base); err == nil || !strings.Contains(err.Error(), "token is empty") {
		t.Fatalf("empty token err=%v", err)
	}
	base.Token = "secret"
	for i := 0; i < 700; i++ {
		base.ResumeFrom = append(base.ResumeFrom, protocol.ReplayCursor{SessionID: "00000000-0000-4000-8000-000000000010", RuntimeID: "00000000-0000-4000-8000-000000000011", Epoch: "00000000-0000-4000-8000-000000000012", Sequence: uint64(i)})
	}
	if _, err := Connect(context.Background(), base); err == nil || !strings.Contains(err.Error(), "handshake maximum") {
		t.Fatalf("oversized hello err=%v", err)
	}
}

func TestReplacingSubscriptionClosesOld(t *testing.T) {
	c := &Client{subs: make(map[runtimeKey]*eventSubscription), buffered: make(map[runtimeKey][]protocol.SequencedEvent), bufferedBytes: make(map[runtimeKey]int)}
	old, _ := c.Subscribe(protocol.NilUUID, protocol.NilUUID)
	newer, stop := c.Subscribe(protocol.NilUUID, protocol.NilUUID)
	defer stop()
	select {
	case _, ok := <-old:
		if ok {
			t.Fatal("old subscription remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("old subscription was not closed")
	}
	select {
	case <-newer:
		t.Fatal("replacement closed unexpectedly")
	default:
	}
}

func TestMalformedMatchedResponseFailsRequest(t *testing.T) {
	s := newTestServer(t, func(conn net.Conn, r *bufio.Reader, _ chan map[string]any) {
		readClientJSON(r)
		writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		request, _ := readClientJSON(r)
		id := request["requestId"].(string)
		writeServerJSON(conn, `{"type":"response","requestId":"`+id+`"}`)
		time.Sleep(time.Second)
	})
	c := connectTest(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: "cancel"})
	if err == nil || !strings.Contains(err.Error(), "malformed daemon message") {
		t.Fatalf("err=%v", err)
	}
}

func TestDaemonURLAndDesktopAddress(t *testing.T) {
	u, err := daemonURL("wss://waku.example/old?q=1")
	if err != nil || u.String() != "wss://waku.example/v1" {
		t.Fatalf("url=%v err=%v", u, err)
	}
	for input, want := range map[string]string{"0.0.0.0:34123": "127.0.0.1:34123", "[::]:34123": "[::1]:34123", "[0:0::1]:34123": "[::1]:34123", "[::ffff:192.0.2.1]:34123": "[::ffff:192.0.2.1]:34123"} {
		got, err := desktopClientAddress(input)
		if err != nil || got != want {
			t.Fatalf("%s => %s, %v", input, got, err)
		}
	}
	for _, invalid := range []string{"localhost:34123", "example.test:34123", "[fe80::1%eth0]:34123"} {
		if _, err := desktopClientAddress(invalid); err == nil {
			t.Fatalf("accepted daemon address %q", invalid)
		}
	}
}

func TestLoopbackE2EHelperRejectsNonLoopback(t *testing.T) {
	for _, address := range []string{"192.0.2.1:1", "example.test:1", "ws://[2001:db8::1]:1"} {
		if _, _, err := ConnectLoopbackE2E(context.Background(), ConnectOptions{Address: address, Token: "token"}); err == nil {
			t.Fatalf("accepted non-loopback E2E address %q", address)
		}
	}
}

func TestRunLoopbackRequestExercisesRealTransport(t *testing.T) {
	s := newTestServer(t, func(conn net.Conn, r *bufio.Reader, got chan map[string]any) {
		_, _ = readClientJSON(r)
		_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"loopback"}`)
		request, _ := readClientJSON(r)
		requestID, _ := request["requestId"].(string)
		_ = writeServerJSON(conn, `{"type":"response","requestId":"`+requestID+`","outcome":{"status":"ok","payload":{"type":"ack"}}}`)
	})
	payload, err := RunLoopbackRequest(context.Background(), LoopbackE2EOptions{
		ConnectOptions: ConnectOptions{Address: s.address, Token: "secret", ClientID: "00000000-0000-4000-8000-000000000001"},
		SessionID:      "00000000-0000-4000-8000-000000000002",
		RuntimeID:      "00000000-0000-4000-8000-000000000003",
	}, protocol.Prompt("hello", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"type":"ack"}` {
		t.Fatalf("payload=%s", payload)
	}
}

func TestAllowedOriginsAreExactAndDeduplicated(t *testing.T) {
	got, err := ParseAllowedOrigins(" https://APP.waku.test:443, http://localhost:3001/, https://app.waku.test, https://[0:0::1]:443 ")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "https://app.waku.test,http://localhost:3001,https://[::1]" {
		t.Fatalf("origins=%v", got)
	}
	for _, invalid := range []string{"https://app.waku.test/path", "ws://app.waku.test", "https://user@app.waku.test", "https://app.waku.test?q=1", "https://[fe80::1%25eth0]", "https://tést.example"} {
		if _, err := ParseAllowedOrigins(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestDaemonReadinessIsBoundedStrictAndVersioned(t *testing.T) {
	ready, err := readDaemonReady(strings.NewReader(`{"address":"127.0.0.1:1","protocolVersion":7,"pid":42}`+"\n"), 42)
	if err != nil || ready.PID != 42 {
		t.Fatalf("ready=%#v err=%v", ready, err)
	}
	for _, raw := range []string{
		`{"address":"127.0.0.1:1","protocolVersion":6,"pid":42}` + "\n",
		`{"address":"127.0.0.1:1","protocolVersion":7,"pid":0}` + "\n",
		`{"address":"127.0.0.1:1","protocolVersion":7,"pid":42} {}` + "\n",
		`{"address":"localhost:1","protocolVersion":7,"pid":42}` + "\n",
		strings.Repeat(" ", protocol.MaxHandshakeBytes) + "\n",
	} {
		if _, err := readDaemonReady(strings.NewReader(raw), 42); err == nil {
			t.Fatalf("accepted readiness %q", raw[:min(len(raw), 80)])
		}
	}
	if _, err := readDaemonReady(strings.NewReader(`{"address":"127.0.0.1:1","protocolVersion":7,"pid":43}`+"\n"), 42); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("PID mismatch err=%v", err)
	}
}

func TestTaskStateSubscriptionKeepsLatestAndUnsubscribes(t *testing.T) {
	c := &Client{}
	updates, stop := c.SubscribeTaskState()
	if err := c.handle(protocol.ServerMessage{Type: "taskStateChanged", Revision: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.handle(protocol.ServerMessage{Type: "taskStateChanged", Revision: 2}); err != nil {
		t.Fatal(err)
	}
	if got := <-updates; got != 2 {
		t.Fatalf("latest=%d", got)
	}
	stop()
	if len(c.taskSubs) != 0 {
		t.Fatal("subscriber leaked")
	}
	if _, ok := <-updates; ok {
		t.Fatal("unsubscribed channel open")
	}
}

func TestEventSubscriptionHasByteAndCountLimits(t *testing.T) {
	s := newEventSubscription()
	defer s.close()
	s.mu.Lock()
	s.bytes = maxBufferedEventBytesPerRuntime
	s.mu.Unlock()
	event := protocol.SequencedEvent{SessionID: protocol.NilUUID, RuntimeID: protocol.NilUUID, Epoch: protocol.NilUUID, Sequence: 1, Event: protocol.WireDriverEvent{Kind: "x", Payload: json.RawMessage(`null`)}}
	if s.enqueue(event) {
		t.Fatal("byte limit not enforced")
	}
	s.mu.Lock()
	s.bytes = 0
	s.queue = make([]queuedEvent, maxBufferedEventsPerRuntime)
	s.mu.Unlock()
	if s.enqueue(event) {
		t.Fatal("count limit not enforced")
	}
}

func TestPreSubscriptionBufferHasByteAndCountLimits(t *testing.T) {
	makeEvent := func(sequence uint64, payload string) protocol.SequencedEvent {
		return protocol.SequencedEvent{SessionID: protocol.NilUUID, RuntimeID: protocol.NilUUID, Epoch: protocol.NilUUID, Sequence: sequence, Event: protocol.WireDriverEvent{Kind: "x", Payload: json.RawMessage(`{"text":"` + payload + `"}`)}}
	}
	var events []protocol.SequencedEvent
	bytes := 0
	for i := uint64(1); i <= 4; i++ {
		events, bytes = appendBoundedEvents(events, bytes, makeEvent(i, "x"), 3, 1<<20)
	}
	if len(events) != 3 || events[0].Sequence != 1 || events[2].Sequence != 3 {
		t.Fatalf("count buffer=%v", events)
	}
	size, _ := protocol.Marshal(makeEvent(5, "0123456789"))
	events, bytes = appendBoundedEvents(nil, 0, makeEvent(5, "0123456789"), 10, len(size)-1)
	if len(events) != 0 || bytes != 0 {
		t.Fatalf("byte buffer len=%d bytes=%d", len(events), bytes)
	}
}

func TestWebSocketRejectsInvalidUpgradeAndControlFrames(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, _ := listener.Accept()
		if conn == nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		req, _ := http.ReadRequest(reader)
		key := req.Header.Get("Sec-WebSocket-Key")
		accept := sha1.Sum([]byte(key + websocketGUID))
		io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(accept[:])+"\r\n\r\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := dialWebSocket(ctx, listener.Addr().String(), ""); err == nil || !strings.Contains(err.Error(), "Upgrade/Connection") {
		t.Fatalf("upgrade err=%v", err)
	}
	for name, frame := range map[string][]byte{"fragmented ping": {0x09, 0}, "reserved control": {0x8b, 0}, "one-byte close": {0x88, 1, 0}} {
		t.Run(name, func(t *testing.T) {
			ws := &wsConn{r: bufio.NewReader(strings.NewReader(string(frame)))}
			if _, _, _, err := ws.readFrame(10); err == nil {
				t.Fatal("invalid control frame accepted")
			}
		})
	}
}

func TestWebSocketRejectsUnsolicitedNegotiation(t *testing.T) {
	for name, header := range map[string]string{
		"extension":         "Sec-WebSocket-Extensions: permessage-deflate\r\n",
		"empty extension":   "Sec-WebSocket-Extensions:\r\n",
		"subprotocol":       "Sec-WebSocket-Protocol: waku-v7\r\n",
		"empty subprotocol": "Sec-WebSocket-Protocol:\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				conn, _ := listener.Accept()
				if conn == nil {
					return
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				req, _ := http.ReadRequest(reader)
				accept := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + websocketGUID))
				io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+header+"Sec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(accept[:])+"\r\n\r\n")
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := dialWebSocket(ctx, listener.Addr().String(), ""); err == nil || !strings.Contains(err.Error(), "unsolicited") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

type shortWriter struct {
	bytes.Buffer
	max int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	return w.Buffer.Write(p)
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	w := &shortWriter{max: 3}
	if err := writeAll(w, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if w.String() != "0123456789" {
		t.Fatalf("write=%q", w.String())
	}
}

func TestHandshakeReadRejectsDeclaredOversizeBeforePayload(t *testing.T) {
	var frame bytes.Buffer
	frame.Write([]byte{0x81, 127})
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(protocol.MaxHandshakeBytes+1))
	frame.Write(size[:])
	ws := &wsConn{r: bufio.NewReader(&frame), Conn: nopConn{Writer: io.Discard}}
	if _, err := ws.readJSON(protocol.MaxHandshakeBytes); err == nil {
		t.Fatal("oversized hello frame accepted")
	}
}

type nopConn struct{ io.Writer }

func (nopConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return nil }
func (nopConn) RemoteAddr() net.Addr             { return nil }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

func TestCloseCodesAndEcho(t *testing.T) {
	for _, code := range []uint16{1000, 1014, 3000, 4999} {
		var p [2]byte
		binary.BigEndian.PutUint16(p[:], code)
		if !validClosePayload(p[:]) {
			t.Fatalf("valid close code %d rejected", code)
		}
	}
	for _, code := range []uint16{999, 1004, 1005, 1006, 1015, 1016, 2000, 5000} {
		var p [2]byte
		binary.BigEndian.PutUint16(p[:], code)
		if validClosePayload(p[:]) {
			t.Fatalf("invalid close code %d accepted", code)
		}
	}
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()
	ws := &wsConn{Conn: clientSide, r: bufio.NewReader(clientSide)}
	done := make(chan error, 1)
	go func() { _, err := ws.readJSON(1024); done <- err }()
	payload := []byte{0x03, 0xe8}
	if _, err := serverSide.Write(append([]byte{0x88, byte(len(payload))}, payload...)); err != nil {
		t.Fatal(err)
	}
	serverSide.SetReadDeadline(time.Now().Add(time.Second))
	first := make([]byte, 2)
	if _, err := io.ReadFull(serverSide, first); err != nil {
		t.Fatal(err)
	}
	if first[0] != 0x88 || first[1]&0x7f != 2 || first[1]&0x80 == 0 {
		t.Fatalf("echo header=%x", first)
	}
	maskAndPayload := make([]byte, 6)
	if _, err := io.ReadFull(serverSide, maskAndPayload); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		maskAndPayload[4+i] ^= maskAndPayload[i%4]
	}
	if !bytes.Equal(maskAndPayload[4:], payload) {
		t.Fatalf("echo payload=%x", maskAndPayload[4:])
	}
	if err := <-done; err != io.EOF {
		t.Fatalf("read err=%v", err)
	}
}

func TestMalformedJSONFailsTransportAndPending(t *testing.T) {
	s := newTestServer(t, func(conn net.Conn, r *bufio.Reader, _ chan map[string]any) {
		readClientJSON(r)
		writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		readClientJSON(r)
		writeServerJSON(conn, `{"type":`)
	})
	c := connectTest(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: "cancel"})
	if err == nil || !strings.Contains(err.Error(), "malformed daemon message") {
		t.Fatalf("err=%v", err)
	}
}

func TestCursorAdvancesOnlyAfterDeliveryAndGapsFail(t *testing.T) {
	c := &Client{subs: make(map[runtimeKey]*eventSubscription), buffered: make(map[runtimeKey][]protocol.SequencedEvent), bufferedBytes: make(map[runtimeKey]int), last: make(map[runtimeKey]lastSequence), accepted: make(map[runtimeKey]lastSequence)}
	e := protocol.SequencedEvent{SessionID: "00000000-0000-4000-8000-000000000010", RuntimeID: "00000000-0000-4000-8000-000000000011", Epoch: "00000000-0000-4000-8000-000000000012", Sequence: 1, Event: protocol.WireDriverEvent{Kind: "x", Payload: json.RawMessage(`null`)}}
	if err := c.handle(protocol.ServerMessage{Type: "event", Event: &e}); err != nil {
		t.Fatal(err)
	}
	if len(c.LastSequences()) != 0 {
		t.Fatal("cursor advanced for merely retained event")
	}
	e.Sequence = 3
	if err := c.handle(protocol.ServerMessage{Type: "event", Event: &e}); err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("gap err=%v", err)
	}
	e.Sequence = 1
	events, stop := c.Subscribe(e.SessionID, e.RuntimeID)
	defer stop()
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("retained event not delivered")
	}
	deadline := time.Now().Add(time.Second)
	for len(c.LastSequences()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.LastSequences(); len(got) != 1 || got[0].Sequence != 1 {
		t.Fatalf("cursor=%v", got)
	}
}

func TestGlobalRetainedAndPendingAdmissionLimits(t *testing.T) {
	c := &Client{write: make(chan outboundMessage, maxOutboundMessages), done: make(chan struct{}), pending: make(map[protocol.UUID]pendingRequest), subs: make(map[runtimeKey]*eventSubscription), buffered: make(map[runtimeKey][]protocol.SequencedEvent), bufferedBytes: make(map[runtimeKey]int), last: make(map[runtimeKey]lastSequence), accepted: make(map[runtimeKey]lastSequence), retainedCount: maxRetainedEvents}
	e := protocol.SequencedEvent{SessionID: "00000000-0000-4000-8000-000000000010", RuntimeID: "00000000-0000-4000-8000-000000000011", Epoch: "00000000-0000-4000-8000-000000000012", Sequence: 1, Event: protocol.WireDriverEvent{Kind: "x", Payload: json.RawMessage(`null`)}}
	if err := c.handle(protocol.ServerMessage{Type: "event", Event: &e}); err == nil || !strings.Contains(err.Error(), "retained event limit") {
		t.Fatalf("retained err=%v", err)
	}
	c.retainedCount = 0
	for i := 0; i < maxPendingRequests; i++ {
		id, err := newUUID()
		if err != nil {
			t.Fatal(err)
		}
		c.pending[id] = pendingRequest{result: make(chan responseResult, 1), bytes: 1}
		c.pendingBytes++
	}
	_, err := c.Request(context.Background(), protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: "cancel"})
	if err == nil || !strings.Contains(err.Error(), "pending request limit") {
		t.Fatalf("pending err=%v", err)
	}
	if len(c.write) != 0 {
		t.Fatal("rejected pending request entered outbound queue")
	}
	c2 := &Client{write: make(chan outboundMessage, maxOutboundMessages), done: make(chan struct{}), pending: make(map[protocol.UUID]pendingRequest), outboundCount: maxOutboundMessages}
	if _, err := c2.Request(context.Background(), protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: "cancel"}); err == nil || !strings.Contains(err.Error(), "outbound queue limit") {
		t.Fatalf("outbound err=%v", err)
	}
	if len(c2.pending) != 0 || len(c2.write) != 0 {
		t.Fatal("outbound rejection installed pending correlation")
	}
	c3 := &Client{write: make(chan outboundMessage, maxOutboundMessages), done: make(chan struct{}), pending: make(map[protocol.UUID]pendingRequest), pendingBytes: maxPendingRequestBytes}
	if _, err := c3.Request(context.Background(), protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: "cancel"}); err == nil || !strings.Contains(err.Error(), "pending request limit") {
		t.Fatalf("pending byte err=%v", err)
	}
}
