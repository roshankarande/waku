package client

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

func TestTypedUISurfaceRequestsAndCorrelation(t *testing.T) {
	commands := make(chan map[string]any, 16)
	s := newTestServer(t, func(conn net.Conn, reader *bufio.Reader, _ chan map[string]any) {
		_, _ = readClientJSON(reader)
		_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		for i := 0; i < 12; i++ {
			request, err := readClientJSON(reader)
			if err != nil {
				return
			}
			command := request["command"].(map[string]any)
			commands <- command
			var payload string
			switch command["type"] {
			case "getSettings":
				payload = `{"type":"settings","settings":{"future_setting":{"kept":true}}}`
			case "fetchPlanUsage":
				payload = `{"type":"planUsage","usage":null}`
			case "loadUsageHistory":
				payload = `{"type":"usageHistory","history":{"window":"thisMonth","sinceDay":"2026-01-01","untilDay":"2026-01-31","totals":{"uncachedInput":9007199254740993,"cachedInput":0,"cacheCreation":0,"output":0,"reasoning":0},"totalTokens":9007199254740993,"costUsd":0,"records":0,"sessions":0,"providers":[],"models":[],"daily":[],"months":[],"projects":[],"quality":{"providerReportedShare":0,"modelPricedShare":0,"unpricedShare":1,"cacheSavingsUsd":0},"pricing":"fresh","scannedFiles":0,"skippedFiles":0,"errors":[],"scanDuration":{"secs":0,"nanos":0}}}`
			case "loadSkills":
				payload = `{"type":"skillsCatalog","catalog":{"skills":[]}}`
			case "workspace":
				payload = `{"type":"workspace","result":{"type":"textFile","content":"remote"}}`
			default:
				payload = `{"type":"ack"}`
			}
			_ = writeServerJSON(conn, `{"type":"response","requestId":"`+request["requestId"].(string)+`","outcome":{"status":"ok","payload":`+payload+`}}`)
		}
	})
	c := connectTest(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	settings, err := c.GetSettings(ctx)
	if err != nil || string(settings.Extra["future_setting"]) != `{"kept":true}` {
		t.Fatalf("settings=%+v err=%v", settings, err)
	}
	settings.ComputerUseAllowedApps = []protocol.ComputerAppGrant{{BundleID: "dev.waku", AppName: "Waku"}}
	if err := c.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	usage, err := c.FetchPlanUsage(ctx, "codex", nil, nil)
	if err != nil || usage != nil {
		t.Fatalf("plan usage=%+v err=%v", usage, err)
	}
	history, err := c.LoadUsageHistory(ctx, protocol.UsageWindowValue{Kind: "thisMonth"}, []string{"ssh://host/a/../b"})
	if err != nil || history.TotalTokens != 9007199254740993 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	catalog, err := c.LoadSkills(ctx, []protocol.SkillProject{{"remote", "ssh://host/a/../b"}})
	if err != nil || len(catalog.Skills) != 0 {
		t.Fatalf("catalog=%+v err=%v", catalog, err)
	}
	if err := c.SetSkillsEnabled(ctx, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.TrashSkills(ctx, nil); err != nil {
		t.Fatal(err)
	}
	result, err := c.Workspace(ctx, protocol.WorkspaceReadTextFile{Type: "readTextFile", Root: "ssh://host/a/../b", RelativePath: "../opaque"})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.(*protocol.WorkspaceTextFileResult).Content; got != "remote" {
		t.Fatalf("workspace content=%q", got)
	}
	session := protocol.UUID("00000000-0000-4000-8000-000000000010")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000011")
	if err := c.OpenTerminal(ctx, session, runtimeID, "ssh://host/a/../b", 120, 40); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteTerminal(ctx, session, runtimeID, []byte{0, 255}); err != nil {
		t.Fatal(err)
	}
	if err := c.ResizeTerminal(ctx, session, runtimeID, 80, 24); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseTerminal(ctx, session, runtimeID); err != nil {
		t.Fatal(err)
	}

	var got []map[string]any
	for i := 0; i < 12; i++ {
		got = append(got, <-commands)
	}
	if roots := got[3]["projectRoots"].([]any); roots[0] != "ssh://host/a/../b" {
		t.Fatalf("remote roots changed: %v", roots)
	}
	op := got[7]["operation"].(map[string]any)
	if op["root"] != "ssh://host/a/../b" || op["relative_path"] != "../opaque" {
		t.Fatalf("remote workspace paths changed: %v", op)
	}
	if got[9]["data"] != "AP8=" {
		t.Fatalf("terminal data=%v", got[9]["data"])
	}
}

func TestTypedHelperRejectsMismatchedCorrelatedPayload(t *testing.T) {
	s := newTestServer(t, func(conn net.Conn, reader *bufio.Reader, _ chan map[string]any) {
		_, _ = readClientJSON(reader)
		_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		request, _ := readClientJSON(reader)
		_ = writeServerJSON(conn, `{"type":"response","requestId":"`+request["requestId"].(string)+`","outcome":{"status":"ok","payload":{"type":"ack"}}}`)
	})
	c := connectTest(t, s)
	if _, err := c.GetSettings(context.Background()); err == nil {
		t.Fatal("GetSettings accepted an ack payload")
	}
}

func TestTypedHelperHonorsContextCancellation(t *testing.T) {
	s := newTestServer(t, func(conn net.Conn, reader *bufio.Reader, _ chan map[string]any) {
		_, _ = readClientJSON(reader)
		_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		_, _ = readClientJSON(reader)
		time.Sleep(time.Second)
	})
	c := connectTest(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.GetSettings(ctx); err != context.DeadlineExceeded {
		t.Fatalf("err=%v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) != 0 {
		t.Fatalf("cancelled request retained %d pending entries", len(c.pending))
	}
}

func TestTerminalEventDemuxAndCancellation(t *testing.T) {
	session := protocol.UUID("00000000-0000-4000-8000-000000000010")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000011")
	s := newTestServer(t, func(conn net.Conn, reader *bufio.Reader, _ chan map[string]any) {
		_, _ = readClientJSON(reader)
		_ = writeServerJSON(conn, `{"type":"hello","protocolVersion":7,"daemonVersion":"test"}`)
		for _, raw := range []string{
			`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000010","runtimeId":"00000000-0000-4000-8000-000000000011","epoch":"00000000-0000-4000-8000-000000000012","sequence":1,"event":{"kind":"terminalOutput","payload":{"data":"AP8="}}}`,
			`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000010","runtimeId":"00000000-0000-4000-8000-000000000011","epoch":"00000000-0000-4000-8000-000000000012","sequence":2,"event":{"kind":"terminalError","payload":"pty closed"}}`,
			`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000010","runtimeId":"00000000-0000-4000-8000-000000000011","epoch":"00000000-0000-4000-8000-000000000012","sequence":3,"event":{"kind":"terminalExited","payload":null}}`,
		} {
			_ = writeServerJSON(conn, raw)
		}
		time.Sleep(time.Second)
	})
	c := connectTest(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	events, stop := c.SubscribeTerminal(ctx, session, runtimeID)
	first, second, third := <-events, <-events, <-events
	if first.Kind != protocol.EventTerminalOutput || !reflect.DeepEqual(first.Data, []byte{0, 255}) {
		t.Fatalf("first=%+v", first)
	}
	if second.Kind != protocol.EventTerminalError || second.Message != "pty closed" {
		t.Fatalf("second=%+v", second)
	}
	if third.Kind != protocol.EventTerminalExited {
		t.Fatalf("third=%+v", third)
	}
	cancel()
	stop()
	if _, ok := <-events; ok {
		t.Fatal("terminal subscription remained open")
	}
}

func TestTerminalEventCopiesOutputBytes(t *testing.T) {
	raw := json.RawMessage(`{"data":"AP8="}`)
	event, ok := terminalEvent(protocol.WireDriverEvent{Kind: "terminalOutput", Payload: raw})
	if !ok || !reflect.DeepEqual(event.Data, []byte{0, 255}) {
		t.Fatalf("event=%+v ok=%v", event, ok)
	}
}

func TestTerminalCursorAdvancesOnlyAfterTypedConsumerReceives(t *testing.T) {
	c := &Client{
		subs:          make(map[runtimeKey]*eventSubscription),
		buffered:      make(map[runtimeKey][]protocol.SequencedEvent),
		bufferedBytes: make(map[runtimeKey]int),
		last:          make(map[runtimeKey]lastSequence),
		accepted:      make(map[runtimeKey]lastSequence),
	}
	session := protocol.UUID("00000000-0000-4000-8000-000000000010")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000011")
	events, stop := c.SubscribeTerminal(context.Background(), session, runtimeID)
	defer stop()
	event := protocol.SequencedEvent{
		SessionID: session,
		RuntimeID: runtimeID,
		Epoch:     "00000000-0000-4000-8000-000000000012",
		Sequence:  1,
		Event:     protocol.WireDriverEvent{Kind: "terminalOutput", Payload: json.RawMessage(`{"data":"YQ=="}`)},
	}
	if err := c.handle(protocol.ServerMessage{Type: "event", Event: &event}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if cursors := c.LastSequences(); len(cursors) != 0 {
		t.Fatalf("cursor advanced before terminal output consumption: %+v", cursors)
	}
	if got := <-events; string(got.Data) != "a" {
		t.Fatalf("event=%+v", got)
	}
	deadline := time.Now().Add(time.Second)
	for len(c.LastSequences()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if cursors := c.LastSequences(); len(cursors) != 1 || cursors[0].Sequence != 1 {
		t.Fatalf("cursor did not advance after terminal output consumption: %+v", cursors)
	}
}
