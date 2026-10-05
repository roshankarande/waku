package providers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

type deepSeekFake struct {
	mu             sync.Mutex
	methods        []string
	responds       int
	failReply      bool
	rejectReply    bool
	historyCalls   int
	recoveryEvents []any
}

func (f *deepSeekFake) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	var frame map[string]json.RawMessage
	if err := json.NewDecoder(request.Body).Decode(&frame); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.URL.Path == "/api/respond" {
		f.mu.Lock()
		f.responds++
		fail := f.failReply
		f.failReply = false
		reject := f.rejectReply
		f.rejectReply = false
		f.mu.Unlock()
		if fail {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": !reject, "reason": "fixture rejection"})
		return
	}
	method := rawString(frame["method"])
	f.mu.Lock()
	f.methods = append(f.methods, method)
	f.mu.Unlock()
	var payload map[string]json.RawMessage
	_ = decodeUseNumber(frame["payload"], &payload)
	value := any(map[string]any{})
	switch method {
	case "session.create":
		value = map[string]any{"sessionId": rawString(payload["sessionId"])}
	case "session.history":
		f.mu.Lock()
		f.historyCalls++
		calls := f.historyCalls
		recovery := append([]any(nil), f.recoveryEvents...)
		f.mu.Unlock()
		if calls > 1 {
			value = map[string]any{"events": recovery, "hasMore": false, "projections": map[string]any{"values": map[string]any{"title": "Recovered"}}}
		} else {
			value = map[string]any{"events": []any{}, "hasMore": false}
		}
	case "commands/list":
		value = []any{map[string]any{"name": "review", "description": "Review changes"}}
	case "host.info":
		value = map[string]any{"version": "0.9.1"}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "server-response", "rpcId": rawString(frame["rpcId"]), "result": map[string]any{"ok": true, "value": value}})
}

func TestDeepSeekHTTPDownlinkOrderedSession(t *testing.T) {
	fake := &deepSeekFake{}
	server := httptest.NewServer(fake)
	defer server.Close()
	downlink := make(chan json.RawMessage, 16)
	adapter := DeepSeekAdapter{BaseURL: server.URL, Client: server.Client(), eventSource: func(context.Context, string) (<-chan json.RawMessage, error) { return downlink, nil }}
	discovery, err := adapter.Discover(context.Background(), StartOptions{ProbeVersion: true})
	if err != nil || !discovery.Available || discovery.Version != "0.9.1" {
		t.Fatalf("discovery=%#v err=%v", discovery, err)
	}
	recorder := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir(), Access: AccessAsk}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	connected := recorder.wait(t, EventConnected)
	var cursor map[string]json.RawMessage
	if err := decodeUseNumber(connected.Cursor, &cursor); err != nil || rawString(cursor["provider"]) != "deepSeek" {
		t.Fatalf("cursor=%s err=%v", connected.Cursor, err)
	}
	sessionID := rawString(cursor["sessionId"])
	if err := session.Prompt(context.Background(), "ship it"); err != nil {
		t.Fatal(err)
	}
	for _, frame := range []string{
		`{"payload":{"type":"session/event","sessionId":"` + sessionID + `","event":{"seq":1,"type":"turn/start","data":{}}}}`,
		`{"payload":{"type":"session/event","sessionId":"` + sessionID + `","event":{"seq":2,"type":"assistant/chunk","data":{"chunk":{"type":"reasoning-delta","text":"think"}}}}}`,
		`{"payload":{"type":"session/event","sessionId":"` + sessionID + `","event":{"seq":3,"type":"assistant/chunk","data":{"chunk":{"type":"text-delta","text":"done"}}}}}`,
		`{"payload":{"type":"session/event","sessionId":"` + sessionID + `","event":{"seq":4,"type":"turn/end","data":{"reason":{"kind":"completed"}}}}}`,
	} {
		downlink <- json.RawMessage(frame)
	}
	recorder.wait(t, EventTurnFinished)
	recorder.mu.Lock()
	var kinds []EventKind
	for _, event := range recorder.events {
		if event.Kind != EventConnected && event.Kind != EventAvailableCommands {
			kinds = append(kinds, event.Kind)
		}
	}
	recorder.mu.Unlock()
	want := []EventKind{EventTurnStarted, EventReasoningDelta, EventTextDelta, EventTurnFinished}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("event order=%v want=%v", kinds, want)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventProcessExited)
}

func TestDeepSeekInteractionsRetainPendingOnFailedReply(t *testing.T) {
	fake := &deepSeekFake{rejectReply: true}
	server := httptest.NewServer(fake)
	defer server.Close()
	downlink := make(chan json.RawMessage, 8)
	recorder := newEventRecorder()
	session, err := (DeepSeekAdapter{BaseURL: server.URL, Client: server.Client(), eventSource: func(context.Context, string) (<-chan json.RawMessage, error) { return downlink, nil }}).Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir(), Access: AccessAsk}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	connected := recorder.wait(t, EventConnected)
	var cursor map[string]json.RawMessage
	_ = decodeUseNumber(connected.Cursor, &cursor)
	sessionID := rawString(cursor["sessionId"])
	downlink <- json.RawMessage(`{"rpcId":"rpc-approval","payload":{"type":"approval/requested","sessionId":"` + sessionID + `","approvalId":"approval-1","title":"Run command"}}`)
	permission := recorder.wait(t, EventPermission)
	if err := session.RespondPermission(context.Background(), permission.RequestID, "allow"); err == nil {
		t.Fatal("failed response unexpectedly succeeded")
	}
	if err := session.RespondPermission(context.Background(), permission.RequestID, "allow"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	downlink <- json.RawMessage(`{"rpcId":"rpc-question","payload":{"type":"question/requested","sessionId":"` + sessionID + `","questions":[{"id":"q1","header":"Choice","question":"Pick","options":[{"label":"A"}],"multiSelect":false}]}}`)
	question := recorder.wait(t, EventUserInputRequested)
	if err := session.RespondQuestions(context.Background(), question.RequestID, []UserInputAnswer{{QuestionID: "q1", Answers: []string{"A"}}}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	responds := fake.responds
	fake.mu.Unlock()
	if responds != 3 {
		t.Fatalf("respond calls=%d want=3", responds)
	}
	_ = session.Close()
}

func TestDeepSeekFiltersForeignSessionEvents(t *testing.T) {
	server := httptest.NewServer(&deepSeekFake{})
	defer server.Close()
	downlink := make(chan json.RawMessage, 4)
	recorder := newEventRecorder()
	session, err := (DeepSeekAdapter{BaseURL: server.URL, Client: server.Client(), eventSource: func(context.Context, string) (<-chan json.RawMessage, error) { return downlink, nil }}).Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir()}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventConnected)
	downlink <- json.RawMessage(`{"payload":{"type":"session/event","sessionId":"foreign","event":{"seq":99,"type":"assistant/chunk","data":{"chunk":{"type":"text-delta","text":"secret"}}}}}`)
	downlink <- json.RawMessage(`{"rpcId":"foreign-approval","payload":{"type":"approval/requested","sessionId":"foreign","approvalId":"p"}}`)
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventProcessExited)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	for _, event := range recorder.events {
		if event.Text == "secret" || event.Kind == EventPermission {
			t.Fatalf("foreign event leaked: %#v", event)
		}
	}
}

func TestDeepSeekSubscribedGapRecoversOrderedHistory(t *testing.T) {
	fake := &deepSeekFake{recoveryEvents: []any{
		map[string]any{"event": map[string]any{"seq": 2, "type": "turn/end", "data": map[string]any{"reason": map[string]any{"kind": "completed"}}}},
		map[string]any{"event": map[string]any{"seq": 1, "type": "assistant/chunk", "data": map[string]any{"chunk": map[string]any{"type": "text-delta", "text": "recovered"}}}},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()
	downlink := make(chan json.RawMessage, 2)
	recorder := newEventRecorder()
	session, err := (DeepSeekAdapter{BaseURL: server.URL, Client: server.Client(), eventSource: func(context.Context, string) (<-chan json.RawMessage, error) { return downlink, nil }}).Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir()}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	connected := recorder.wait(t, EventConnected)
	var cursor map[string]json.RawMessage
	_ = decodeUseNumber(connected.Cursor, &cursor)
	downlink <- json.RawMessage(`{"payload":{"type":"session/subscribed","sessionId":"` + rawString(cursor["sessionId"]) + `","lastSeq":2}}`)
	if got := recorder.wait(t, EventTextDelta); got.Text != "recovered" {
		t.Fatalf("text=%q", got.Text)
	}
	if got := recorder.wait(t, EventAutoTitleUpdated); got.Text != "Recovered" {
		t.Fatalf("title=%q", got.Text)
	}
	_ = session.Close()
}

func TestDeepSeekLiveSequenceJumpRecoversBeforeCurrent(t *testing.T) {
	fake := &deepSeekFake{recoveryEvents: []any{
		map[string]any{"event": map[string]any{"seq": 2, "type": "assistant/chunk", "data": map[string]any{"chunk": map[string]any{"type": "text-delta", "text": "recovered"}}}},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()
	downlink := make(chan json.RawMessage, 4)
	recorder := newEventRecorder()
	session, err := (DeepSeekAdapter{BaseURL: server.URL, Client: server.Client(), eventSource: func(context.Context, string) (<-chan json.RawMessage, error) { return downlink, nil }}).Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir()}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	connected := recorder.wait(t, EventConnected)
	var cursor map[string]json.RawMessage
	_ = decodeUseNumber(connected.Cursor, &cursor)
	id := rawString(cursor["sessionId"])
	downlink <- json.RawMessage(`{"payload":{"type":"session/event","sessionId":"` + id + `","event":{"seq":1,"type":"assistant/chunk","data":{"chunk":{"type":"text-delta","text":"first"}}}}}`)
	downlink <- json.RawMessage(`{"payload":{"type":"session/event","sessionId":"` + id + `","event":{"seq":3,"type":"assistant/chunk","data":{"chunk":{"type":"text-delta","text":"live"}}}}}`)
	downlink <- json.RawMessage(`{"payload":{"type":"session/event","sessionId":"` + id + `","event":{"seq":4,"type":"turn/start","data":{}}}}`)
	recorder.wait(t, EventTurnStarted)
	recorder.mu.Lock()
	var texts []string
	for _, event := range recorder.events {
		if event.Kind == EventTextDelta {
			texts = append(texts, event.Text)
		}
	}
	recorder.mu.Unlock()
	if !reflect.DeepEqual(texts, []string{"first", "recovered", "live"}) {
		t.Fatalf("texts=%v", texts)
	}
	_ = session.Close()
}

func TestDeepSeekRejectsUnsupportedStartupAndNoOpenProbe(t *testing.T) {
	if deepSeekSupportsNoOpen([]byte("Usage: dsh web")) {
		t.Fatal("unsupported --no-open accepted")
	}
	a := DeepSeekAdapter{BaseURL: "http://127.0.0.1:1"}
	for _, options := range []StartOptions{{WorkingDirectory: t.TempDir(), Model: "x"}, {WorkingDirectory: t.TempDir(), ReasoningEffort: "high"}, {WorkingDirectory: t.TempDir(), ServiceTier: "x"}, {WorkingDirectory: t.TempDir(), Access: AccessFull}, {WorkingDirectory: t.TempDir(), Access: AccessMode("mystery")}} {
		if _, err := a.Start(context.Background(), options, newEventRecorder()); err == nil {
			t.Fatalf("invalid options accepted: %#v", options)
		}
	}
	if _, err := (&deepSeekSession{}).ApplyOptions(context.Background(), SessionOptions{Access: AccessMode("mystery")}); err == nil {
		t.Fatal("unknown live access accepted")
	}
}

func TestDeepSeekRejectsWrongCursorAndMalformedDownlink(t *testing.T) {
	server := httptest.NewServer(&deepSeekFake{})
	defer server.Close()
	a := DeepSeekAdapter{BaseURL: server.URL, Client: server.Client(), eventSource: func(context.Context, string) (<-chan json.RawMessage, error) { return make(chan json.RawMessage), nil }}
	_, err := a.Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir(), ResumeCursor: json.RawMessage(`{"provider":"codex","sessionId":"x"}`)}, newEventRecorder())
	if err == nil {
		t.Fatal("wrong-provider cursor accepted")
	}
	downlink := make(chan json.RawMessage, 1)
	recorder := newEventRecorder()
	a.eventSource = func(context.Context, string) (<-chan json.RawMessage, error) { return downlink, nil }
	session, err := a.Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir()}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	connected := recorder.wait(t, EventConnected)
	var cursor map[string]json.RawMessage
	_ = decodeUseNumber(connected.Cursor, &cursor)
	downlink <- json.RawMessage(`{"payload":{"type":"session/event","sessionId":"` + rawString(cursor["sessionId"]) + `","event":{"type":"turn/start"}}}`)
	if got := recorder.wait(t, EventError); got.Text == "" {
		t.Fatal("malformed frame produced empty error")
	}
	recorder.wait(t, EventProcessExited)
	_ = session.Close()
}
