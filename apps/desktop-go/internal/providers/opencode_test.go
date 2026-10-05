package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenCodeHTTPAndSSEOrderedSession(t *testing.T) {
	events := make(chan string, 16)
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "opencode" || password != "secret" {
			http.Error(w, "unauthorized", 401)
			return
		}
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch {
		case r.URL.Path == "/api/info":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": "2.0.22", "pid": 42})
		case r.URL.Path == "/api/model":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]string{"providerID": "anthropic", "modelID": "sonnet", "name": "Sonnet"}}})
		case r.URL.Path == "/api/event":
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			w.WriteHeader(200)
			fl.Flush()
			for {
				select {
				case frame := <-events:
					fmt.Fprintf(w, "data: %s\n\n", frame)
					fl.Flush()
				case <-r.Context().Done():
					return
				}
			}
		case r.Method == "POST" && r.URL.Path == "/api/session":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "x"}})
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/message"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
		case strings.HasSuffix(r.URL.Path, "/prompt"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "inbox"}})
		case strings.Contains(r.URL.Path, "/permission/"):
			w.WriteHeader(204)
		default:
			http.Error(w, "missing", 404)
		}
	}))
	defer server.Close()
	a := OpenCodeAdapter{BaseURL: server.URL, Password: "secret", Client: server.Client()}
	d, err := a.Discover(context.Background(), StartOptions{DiscoverModels: true, ProbeVersion: true})
	if err != nil || !d.Available || d.Version != "2.0.22" || len(d.Models) != 1 {
		t.Fatalf("discover=%#v %v", d, err)
	}
	r := newEventRecorder()
	s, err := a.Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir(), Access: AccessAsk}, r)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	connected := r.wait(t, EventConnected)
	var cursor map[string]json.RawMessage
	if decodeUseNumber(connected.Cursor, &cursor) != nil || rawString(cursor["provider"]) != "openCode" {
		t.Fatalf("cursor=%s", connected.Cursor)
	}
	if err := s.Prompt(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	for _, frame := range []string{`{"event":{"type":"session.execution.started","data":{"sessionID":"` + rawString(cursor["sessionId"]) + `"}}}`, `{"event":{"type":"session.reasoning.delta","data":{"sessionID":"` + rawString(cursor["sessionId"]) + `","delta":"think"}}}`, `{"event":{"type":"session.text.delta","data":{"sessionID":"` + rawString(cursor["sessionId"]) + `","delta":"done"}}}`, `{"event":{"type":"session.execution.succeeded","data":{"sessionID":"` + rawString(cursor["sessionId"]) + `"}}}`} {
		events <- frame
	}
	r.wait(t, EventTurnFinished)
	r.mu.Lock()
	var kinds []EventKind
	for _, e := range r.events {
		if e.Kind != EventConnected {
			kinds = append(kinds, e.Kind)
		}
	}
	r.mu.Unlock()
	want := []EventKind{EventTurnStarted, EventReasoningDelta, EventTextDelta, EventTurnFinished}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("order=%v", kinds)
	}
	_ = s.Close()
	r.wait(t, EventProcessExited)
}

func TestOpenCodeFiltersForeignEventsAndRetriesPermissionWrite(t *testing.T) {
	events := make(chan string, 8)
	var replies int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/info":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": "2.0.22", "pid": 42})
		case r.URL.Path == "/api/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			for {
				select {
				case frame := <-events:
					fmt.Fprintf(w, "data: %s\n\n", frame)
					w.(http.Flusher).Flush()
				case <-r.Context().Done():
					return
				}
			}
		case r.URL.Path == "/api/session":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{}})
		case strings.Contains(r.URL.Path, "/permission/"):
			replies++
			if replies == 1 {
				http.Error(w, "retry", 503)
				return
			}
			w.WriteHeader(204)
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer server.Close()
	r := newEventRecorder()
	s, err := (OpenCodeAdapter{BaseURL: server.URL, Password: "secret", Client: server.Client()}).Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir(), Access: AccessAsk}, r)
	if err != nil {
		t.Fatal(err)
	}
	connected := r.wait(t, EventConnected)
	var c map[string]json.RawMessage
	_ = decodeUseNumber(connected.Cursor, &c)
	id := rawString(c["sessionId"])
	events <- `{"event":{"type":"session.text.delta","data":{"sessionID":"foreign","delta":"secret"}}}`
	events <- `{"event":{"type":"permission.asked","data":{"sessionID":"` + id + `","id":"p1","permission":"bash"}}}`
	permission := r.wait(t, EventPermission)
	if err := s.RespondPermission(context.Background(), permission.RequestID, "once"); err == nil {
		t.Fatal("first reply unexpectedly succeeded")
	}
	if err := s.RespondPermission(context.Background(), permission.RequestID, "once"); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.Text == "secret" {
			t.Fatal("foreign event leaked")
		}
	}
	_ = s.Close()
}

func TestOpenCodeNestedTypedFormRoundTripAndBound(t *testing.T) {
	var reply map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(&reply); err != nil {
			t.Errorf("decode reply: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	recorder := newEventRecorder()
	s := &openCodeSession{http: boundedHTTP{base: server.URL, client: server.Client()}, sessionCtx: context.Background(), id: "ses-form", sink: recorder, events: make(chan Event, 8), permissions: make(map[string]int), forms: make(map[string][]openCodeFormField), formSizes: make(map[string]int)}
	go s.dispatch()
	raw := json.RawMessage(`{"type":"form.created","data":{"form":{"id":"form-1","sessionID":"ses-form","title":"Configure","fields":[{"type":"string","key":"color","title":"Color","options":[{"value":"r","label":"Red","description":"Warm"}]},{"type":"multiselect","key":"tags","title":"Tags","options":[{"value":"a","label":"Alpha"}]},{"type":"boolean","key":"confirm","title":"Confirm"}]}}}`)
	var envelope map[string]json.RawMessage
	_ = decodeUseNumber(raw, &envelope)
	var data map[string]json.RawMessage
	_ = decodeUseNumber(envelope["data"], &data)
	if err := s.consumeForm(data, raw); err != nil {
		t.Fatal(err)
	}
	event := recorder.wait(t, EventUserInputRequested)
	if len(event.Questions) != 3 || event.Questions[0].Question != "Configure" || event.Questions[1].MultiSelect != true {
		t.Fatalf("questions=%#v", event.Questions)
	}
	if err := s.RespondQuestions(context.Background(), event.RequestID, []UserInputAnswer{{QuestionID: "color", Answers: []string{"Red"}}, {QuestionID: "tags", Answers: []string{"Alpha"}}, {QuestionID: "confirm", Answers: []string{"yes"}}}); err != nil {
		t.Fatal(err)
	}
	var answerEnvelope struct {
		Answer map[string]json.RawMessage `json:"answer"`
	}
	if decodeUseNumber(reply["answer"], &answerEnvelope.Answer) != nil || rawString(answerEnvelope.Answer["color"]) != "r" || string(answerEnvelope.Answer["tags"]) != `["a"]` || string(answerEnvelope.Answer["confirm"]) != `true` {
		encoded, _ := json.Marshal(reply)
		t.Fatalf("reply=%s", encoded)
	}
	s.closeEvents()
	recorder.wait(t, EventProcessExited)
}

func TestOpenCodeReconnectReconcilesWithoutFalseExit(t *testing.T) {
	var mu sync.Mutex
	connections := 0
	reconcileFailures := 0
	activeStreams := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/api/info":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": "2.0.22", "pid": 42})
		case request.URL.Path == "/api/event":
			mu.Lock()
			connections++
			connection := connections
			activeStreams++
			mu.Unlock()
			defer func() {
				mu.Lock()
				activeStreams--
				mu.Unlock()
			}()
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			if connection == 1 {
				_, _ = fmt.Fprint(w, "data: {\"type\":\"session.execution.started\",\"data\":{}}\n\ndata: {\"type\":\"session.text.delta\",\"data\":{\"assistantMessageID\":\"msg-1\",\"ordinal\":0,\"delta\":\"hel\"}}\n\n")
				return
			}
			if connection <= 3 {
				_, _ = fmt.Fprint(w, "data: {\"type\":\"session.text.delta\",\"data\":{\"assistantMessageID\":\"msg-1\",\"ordinal\":0,\"delta\":\"lo world\"}}\n\ndata: {\"type\":\"session.reasoning.delta\",\"data\":{\"assistantMessageID\":\"msg-1\",\"ordinal\":1,\"delta\":\"why\"}}\n\ndata: {\"type\":\"session.tool.success\",\"data\":{\"assistantMessageID\":\"msg-1\",\"id\":\"call-1\",\"name\":\"read\"}}\n\ndata: {\"type\":\"session.execution.succeeded\",\"data\":{}}\n\n")
				w.(http.Flusher).Flush()
			}
			<-request.Context().Done()
		case request.Method == http.MethodPost && request.URL.Path == "/api/session":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "x"}})
		case strings.Contains(request.URL.Path, "/message"):
			if request.URL.Query().Get("cursor") != "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"type": "idle", "id": "idle-1"}, map[string]any{"type": "assistant", "id": "msg-1", "content": []any{map[string]any{"type": "text", "text": "hello world"}, map[string]any{"type": "reasoning", "text": "why"}, map[string]any{"type": "tool", "id": "call-1", "name": "read", "state": map[string]any{"status": "completed", "input": map[string]any{"filePath": "a"}, "content": []any{map[string]any{"type": "text", "text": "ok"}}}}}, "tokens": map[string]any{"input": 3, "output": 4}}}, "cursor": map[string]any{"next": "done"}})
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/api/session/") && !strings.HasSuffix(request.URL.Path, "/permission") && !strings.HasSuffix(request.URL.Path, "/form"):
			mu.Lock()
			if reconcileFailures == 0 {
				reconcileFailures++
				mu.Unlock()
				http.Error(w, "transient", http.StatusServiceUnavailable)
				return
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "x", "title": "Recovered title"}})
		case strings.HasSuffix(request.URL.Path, "/permission"), strings.HasSuffix(request.URL.Path, "/form"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
		default:
			http.Error(w, "missing", http.StatusNotFound)
		}
	}))
	defer server.Close()
	recorder := newEventRecorder()
	session, err := (OpenCodeAdapter{BaseURL: server.URL, Password: "secret", Client: server.Client()}).Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir()}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	recorder.wait(t, EventConnected)
	recorder.wait(t, EventTurnStarted)
	if got := recorder.wait(t, EventTextDelta); got.Text != "hel" {
		t.Fatalf("initial=%q", got.Text)
	}
	if got := recorder.wait(t, EventReasoningDelta); got.Text != "why" {
		t.Fatalf("reasoning=%q", got.Text)
	}
	recorder.wait(t, EventActivity)
	recorder.wait(t, EventUsageUpdated)
	recorder.wait(t, EventAutoTitleUpdated)
	if !recorder.wait(t, EventTurnFinished).Success {
		t.Fatal("reconnected turn did not succeed")
	}
	recorder.mu.Lock()
	starts, exits, activities, reasoning := 0, 0, 0, 0
	var texts []string
	for _, event := range recorder.events {
		if event.Kind == EventTurnStarted {
			starts++
		}
		if event.Kind == EventProcessExited {
			exits++
		}
		if event.Kind == EventTextDelta {
			texts = append(texts, event.Text)
		}
		if event.Kind == EventActivity {
			activities++
		}
		if event.Kind == EventReasoningDelta {
			reasoning++
		}
	}
	recorder.mu.Unlock()
	if starts != 1 || exits != 0 || activities != 1 || reasoning != 1 || !reflect.DeepEqual(texts, []string{"hel", "lo world"}) {
		t.Fatalf("starts=%d exits=%d activities=%d reasoning=%d texts=%v", starts, exits, activities, reasoning, texts)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		gotConnections := connections
		mu.Unlock()
		if gotConnections >= 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	gotConnections, gotFailures := connections, reconcileFailures
	mu.Unlock()
	if gotConnections < 4 || gotFailures != 1 {
		t.Fatalf("connections=%d transient failures=%d", gotConnections, gotFailures)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
	deadline = time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		active := activeStreams
		mu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d replacement SSE handlers remained after Close", active)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOpenCodeRejectsForeignNestedFormAndUnboundedApproval(t *testing.T) {
	s := &openCodeSession{id: "ses-one", access: AccessAsk, sessionCtx: context.Background(), permissions: make(map[string]int), forms: make(map[string][]openCodeFormField), formSizes: make(map[string]int)}
	foreign := map[string]json.RawMessage{"form": json.RawMessage(`{"id":"f","sessionID":"ses-two","title":"x","fields":[{"type":"string","key":"k"}]}`)}
	if err := s.consumeForm(foreign, json.RawMessage(`{"form":"foreign"}`)); err == nil {
		t.Fatal("foreign form accepted")
	}
	large := json.RawMessage(strings.Repeat("x", maxQueuedEventBytes+1))
	if err := s.consumePermission(map[string]json.RawMessage{"id": json.RawMessage(`"p"`)}, large); err == nil {
		t.Fatal("oversize pending approval accepted")
	}
}

func TestOpenCodeRequiresCredentialAndRejectsInvalidOptionsBeforeConnect(t *testing.T) {
	a := OpenCodeAdapter{BaseURL: "http://127.0.0.1:1"}
	if _, err := a.Start(context.Background(), StartOptions{WorkingDirectory: t.TempDir()}, newEventRecorder()); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("credential err=%v", err)
	}
	a.Password = "secret"
	for _, options := range []StartOptions{{WorkingDirectory: t.TempDir(), ServiceTier: "priority"}, {WorkingDirectory: t.TempDir(), ReasoningEffort: "high"}, {WorkingDirectory: t.TempDir(), Model: "broken/"}, {WorkingDirectory: t.TempDir(), Access: AccessMode("mystery")}} {
		if _, err := a.Start(context.Background(), options, newEventRecorder()); err == nil {
			t.Fatalf("invalid options accepted: %#v", options)
		}
	}
}

func TestOpenCodeReplayPaginatesToCurrentStepAndSkipsOldResumeHistory(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("cursor") == "page-two" {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"type": "user", "id": "baseline", "content": []any{}}}})
			return
		}
		old := make([]any, 25)
		for i := range old {
			old[i] = map[string]any{"type": "assistant", "id": fmt.Sprintf("old-%d", i), "content": []any{}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": old, "cursor": map[string]any{"next": "page-two"}})
	}))
	defer server.Close()
	h := boundedHTTP{base: server.URL, client: server.Client()}
	s := &openCodeSession{id: "session", active: true, turnBaseline: "baseline"}
	replay, _, err := s.fetchReplayMessages(context.Background(), h)
	if err != nil || len(replay) != 25 || !strings.Contains(string(replay[0]), "old-24") || requests != 2 {
		t.Fatalf("replay=%q requests=%d err=%v", replay, requests, err)
	}
	requests = 0
	s.turnBaseline = "old-0"
	replay, _, err = s.fetchReplayMessages(context.Background(), h)
	if err != nil || len(replay) != 0 || requests != 1 {
		t.Fatalf("old resume replay=%s requests=%d err=%v", replay, requests, err)
	}
}

func TestOpenCodeReplayIncludesTwoOfflineStepsInOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{
			map[string]any{"type": "idle", "id": "idle"},
			map[string]any{"type": "assistant", "id": "step-two", "content": []any{map[string]any{"type": "text", "text": "two"}}},
			map[string]any{"type": "assistant", "id": "step-one", "content": []any{map[string]any{"type": "text", "text": "one"}}},
			map[string]any{"type": "user", "id": "prompt"},
			map[string]any{"type": "idle", "id": "baseline"},
		}})
	}))
	defer server.Close()
	s := &openCodeSession{id: "session", active: true, turnBaseline: "baseline"}
	replay, settled, err := s.fetchReplayMessages(context.Background(), boundedHTTP{base: server.URL, client: server.Client()})
	if err != nil || !settled || len(replay) != 2 {
		t.Fatalf("replay=%q settled=%v err=%v", replay, settled, err)
	}
	var first, second map[string]json.RawMessage
	_ = decodeUseNumber(replay[0], &first)
	_ = decodeUseNumber(replay[1], &second)
	if rawString(first["id"]) != "step-one" || rawString(second["id"]) != "step-two" {
		t.Fatalf("offline step order=%s,%s", first["id"], second["id"])
	}
}

func TestOpenCodeSnapshotAndBufferedOverlapAreDeduplicated(t *testing.T) {
	s := &openCodeSession{
		active:         true,
		parts:          make(map[string]string),
		overlapOffsets: make(map[string]int),
		completedTools: make(map[string]bool),
		toolInputs:     make(map[string]string),
		events:         make(chan Event, 16),
		cancelStream:   func() {},
	}
	data := map[string]json.RawMessage{"assistantMessageID": json.RawMessage(`"step"`), "ordinal": json.RawMessage(`0`)}
	if suffix, err := s.trackDelta(data, "hel", false); err != nil || suffix != "hel" {
		t.Fatalf("initial suffix=%q err=%v", suffix, err)
	}
	s.overlapOffsets["step:0:false"] = 3
	if err := s.replayMessage(json.RawMessage(`{"type":"assistant","id":"step","content":[{"type":"text","text":"hello"},{"type":"tool","id":"tool","name":"read","state":{"status":"completed"}}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.consume(json.RawMessage(`{"type":"session.text.delta","data":{"assistantMessageID":"step","ordinal":0,"delta":"lo"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.consume(json.RawMessage(`{"type":"session.tool.success","data":{"assistantMessageID":"step","id":"tool","name":"read"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.consume(json.RawMessage(`{"type":"session.text.delta","data":{"assistantMessageID":"step","ordinal":0,"delta":"!"}}`)); err != nil {
		t.Fatal(err)
	}
	var texts []string
	activities := 0
	for len(s.events) != 0 {
		e := <-s.events
		if e.Kind == EventTextDelta {
			texts = append(texts, e.Text)
		}
		if e.Kind == EventActivity {
			activities++
		}
	}
	if !reflect.DeepEqual(texts, []string{"lo", "!"}) || activities != 1 {
		t.Fatalf("texts=%v activities=%d", texts, activities)
	}
}

func TestOpenCodeToolInputEmptyStartFloodAndTurnReset(t *testing.T) {
	s := &openCodeSession{active: true, toolInputs: make(map[string]string), completedTools: make(map[string]bool), parts: make(map[string]string), overlapOffsets: make(map[string]int), eventsClosed: true}
	for i := 0; i < 4096; i++ {
		data := map[string]json.RawMessage{"id": json.RawMessage(fmt.Sprintf("%q", fmt.Sprintf("tool-%d", i)))}
		if err := s.consumeTool("session.tool.input.started", data, nil); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	if err := s.consumeTool("session.tool.input.started", map[string]json.RawMessage{"id": json.RawMessage(`"overflow"`)}, nil); err == nil {
		t.Fatal("empty tool-input flood exceeded count bound")
	}
	if len(s.toolInputs) != 4096 || s.toolBytes == 0 {
		t.Fatalf("inputs=%d bytes=%d", len(s.toolInputs), s.toolBytes)
	}
	s.finish(false, "missed completion", nil)
	if len(s.toolInputs) != 0 || s.toolBytes != 0 {
		t.Fatalf("turn reset retained inputs=%d bytes=%d", len(s.toolInputs), s.toolBytes)
	}
}

func TestOpenCodeApplyOptionsRejectsUnknownAccessWithoutMutation(t *testing.T) {
	s := &openCodeSession{access: AccessAsk}
	if _, err := s.ApplyOptions(context.Background(), SessionOptions{Access: AccessMode("mystery")}); err == nil {
		t.Fatal("unknown access accepted")
	}
	if s.access != AccessAsk {
		t.Fatalf("access mutated to %q", s.access)
	}
}

func TestOpenCodeReplayDedupeRetiredAcrossLongSession(t *testing.T) {
	s := &openCodeSession{active: true, parts: make(map[string]string), completedTools: make(map[string]bool), eventsClosed: true}
	for turn := 0; turn < 5000; turn++ {
		s.mu.Lock()
		s.active = true
		s.lastStepID = fmt.Sprintf("step-%d", turn)
		s.mu.Unlock()
		data := map[string]json.RawMessage{
			"assistantMessageID": json.RawMessage(fmt.Sprintf("%q", s.lastStepID)),
			"ordinal":            json.RawMessage(`0`),
		}
		if err := s.trackPart(data, "payload", false, false); err != nil {
			t.Fatal(err)
		}
		data["id"] = json.RawMessage(`"tool"`)
		if err := s.consumeTool("session.tool.success", data, nil); err != nil {
			t.Fatal(err)
		}
		s.finish(true, "", nil)
		if len(s.parts) != 0 || len(s.completedTools) != 0 || s.partBytes != 0 || s.dedupeBytes != 0 {
			t.Fatalf("turn %d retained replay state", turn)
		}
	}
}

func TestOpenCodeAutomaticApprovalUsesSessionCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &openCodeSession{http: boundedHTTP{base: server.URL, client: server.Client()}, id: "s", access: AccessFull, sessionCtx: ctx}
	err := s.consumePermission(map[string]json.RawMessage{"id": json.RawMessage(`"p"`)}, json.RawMessage(`{"id":"p"}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
