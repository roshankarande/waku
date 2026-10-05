package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type eventRecorder struct {
	mu     sync.Mutex
	events []Event
	wake   chan struct{}
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

type shortWriter struct {
	output []byte
	max    int
	zero   bool
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if w.zero {
		return 0, nil
	}
	n := min(w.max, len(p))
	w.output = append(w.output, p[:n]...)
	return n, nil
}

func newEventRecorder() *eventRecorder { return &eventRecorder{wake: make(chan struct{}, 64)} }
func (r *eventRecorder) Emit(e Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *eventRecorder) wait(t *testing.T, kind EventKind) Event {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		r.mu.Lock()
		for _, e := range r.events {
			if e.Kind == kind {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		select {
		case <-r.wake:
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

func fakeCodexProcess(t *testing.T, inspect func(map[string]json.RawMessage)) *StdioProcess {
	t.Helper()
	providerIn, clientIn := io.Pipe()
	clientOut, providerOut := io.Pipe()
	wait := make(chan error, 1)
	p := &StdioProcess{Lines: NewJSONLines(clientOut, clientIn, clientIn), Stderr: strings.NewReader("diagnostic stderr\n"), wait: wait}
	go func() {
		defer close(wait)
		defer providerOut.Close()
		lines := NewJSONLines(providerIn, providerOut, nil)
		for {
			var msg map[string]json.RawMessage
			if err := lines.Decode(&msg); err != nil {
				wait <- nil
				return
			}
			if inspect != nil {
				inspect(msg)
			}
			method := rawString(msg["method"])
			switch method {
			case "thread/start", "thread/resume":
				_ = lines.Write(map[string]any{"id": 1, "result": map[string]any{"thread": map[string]any{"id": "thread-fake", "turns": []any{}}}})
			case "turn/start":
				_ = lines.Write(map[string]any{"method": "turn/started", "params": map[string]any{"turn": map[string]string{"id": "turn-fake"}}})
			case "turn/steer":
				var id json.RawMessage = msg["id"]
				_ = lines.Write(map[string]any{"id": id, "result": map[string]string{"turnId": "turn-fake"}})
				_ = lines.Write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]string{"delta": "after steer"}})
			case "thread/goal/get":
				_ = lines.Write(map[string]any{"id": msg["id"], "result": map[string]any{"goal": map[string]any{"objective": "ship", "status": "active", "tokensUsed": 0, "timeUsedSeconds": 0}}})
			case "thread/goal/clear":
				_ = lines.Write(map[string]any{"id": msg["id"], "result": map[string]any{}})
			case "thread/goal/set":
				_ = lines.Write(map[string]any{"id": msg["id"], "result": map[string]any{"goal": map[string]any{"objective": "new", "status": "active", "tokensUsed": 0, "timeUsedSeconds": 0}}})
			}
		}
	}()
	return p
}

func TestCodexFakeProcessHandshakeGoalsSteerAndClose(t *testing.T) {
	var methodsMu sync.Mutex
	var methods []string
	var startParams map[string]json.RawMessage
	var stderrReads atomic.Int32
	a := CodexAdapter{startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		process := fakeCodexProcess(t, func(msg map[string]json.RawMessage) {
			methodsMu.Lock()
			method := rawString(msg["method"])
			methods = append(methods, method)
			if method == "thread/start" {
				_ = decodeUseNumber(msg["params"], &startParams)
			}
			methodsMu.Unlock()
		})
		source := strings.NewReader("diagnostic stderr\n")
		process.Stderr = readerFunc(func(buffer []byte) (int, error) { stderrReads.Add(1); return source.Read(buffer) })
		return process, nil
	}}
	r := newEventRecorder()
	session, err := a.Start(context.Background(), StartOptions{Access: AccessAuto}, r)
	if err != nil {
		t.Fatal(err)
	}
	r.wait(t, EventConnected)
	if err := session.Prompt(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	r.wait(t, EventTurnStarted)
	if err := session.Steer(context.Background(), "focus"); err != nil {
		t.Fatal(err)
	}
	r.wait(t, EventTextDelta)
	if err := session.Goal(context.Background(), GoalOperation{Kind: "refresh"}); err != nil {
		t.Fatal(err)
	}
	goal := r.wait(t, EventGoalUpdated)
	if !goal.GoalPresent {
		t.Fatal("goal response was not emitted")
	}
	objective := "new"
	if err := session.Goal(context.Background(), GoalOperation{Kind: "set", Objective: &objective, Replace: true}); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	r.wait(t, EventProcessExited)
	if stderrReads.Load() == 0 {
		t.Fatal("provider stderr was not drained")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var steer, text int = -1, -1
	for i, e := range r.events {
		if e.Kind == EventSteerAccepted {
			steer = i
		}
		if e.Kind == EventTextDelta && e.Text == "after steer" {
			text = i
		}
	}
	if steer < 0 || text < 0 || steer > text {
		t.Fatalf("steer response order drifted: %d %d", steer, text)
	}
	methodsMu.Lock()
	defer methodsMu.Unlock()
	if len(methods) < 3 || methods[0] != "initialize" || methods[1] != "initialized" || methods[2] != "thread/start" {
		t.Fatalf("handshake order: %v", methods)
	}
	if rawString(startParams["approvalPolicy"]) != "on-request" || rawString(startParams["sandbox"]) != "workspace-write" || rawString(startParams["approvalsReviewer"]) != "auto_review" {
		t.Fatalf("auto access handshake: %#v", startParams)
	}
	clearIndex, setIndex := -1, -1
	for i, method := range methods {
		if method == "thread/goal/clear" {
			clearIndex = i
		}
		if method == "thread/goal/set" {
			setIndex = i
		}
	}
	if clearIndex < 0 || setIndex < 0 || clearIndex > setIndex {
		t.Fatalf("goal replacement order: %v", methods)
	}
}

func TestCodexRejectsInvalidOptionsBeforeSpawn(t *testing.T) {
	var starts atomic.Int32
	a := CodexAdapter{startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) { starts.Add(1); return nil, nil }}
	sink := SinkFunc(func(Event) {})
	for _, options := range []StartOptions{{Access: "invalid"}, {ComputerUse: true}, {Model: "unprobed"}, {ResumeCursor: json.RawMessage(`{"provider":"claude","threadId":"x"}`)}} {
		if _, err := a.Start(context.Background(), options, sink); err == nil {
			t.Fatalf("accepted invalid options: %#v", options)
		}
	}
	if starts.Load() != 0 {
		t.Fatalf("spawned %d invalid sessions", starts.Load())
	}
}

func TestCodexValidCursorUsesResumeHandshake(t *testing.T) {
	var resumed atomic.Bool
	a := CodexAdapter{startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeCodexProcess(t, func(msg map[string]json.RawMessage) {
			if rawString(msg["method"]) == "thread/resume" {
				resumed.Store(true)
			}
		}), nil
	}}
	r := newEventRecorder()
	session, err := a.Start(context.Background(), StartOptions{ResumeCursor: json.RawMessage(`{"provider":"codex","threadId":"existing"}`)}, r)
	if err != nil {
		t.Fatal(err)
	}
	r.wait(t, EventConnected)
	if !resumed.Load() {
		t.Fatal("valid cursor did not use thread/resume")
	}
	_ = session.Close()
	r.wait(t, EventProcessExited)
}

func TestJSONLinesRejectsOversizedFrame(t *testing.T) {
	exact := `"` + strings.Repeat("x", MaxFrameBytes-2) + `"`
	lines := NewJSONLines(strings.NewReader(exact+"\n"), io.Discard, nil)
	var value any
	if err := lines.Decode(&value); err != nil {
		t.Fatalf("exact inbound boundary rejected: %v", err)
	}
	frame := `"` + strings.Repeat("x", MaxFrameBytes-1) + `"` + "\n"
	lines = NewJSONLines(strings.NewReader(frame), io.Discard, nil)
	if err := lines.Decode(&value); err == nil {
		t.Fatal("oversized provider frame accepted")
	}
	if err := NewJSONLines(strings.NewReader(""), io.Discard, nil).Write(json.RawMessage(exact)); err != nil {
		t.Fatalf("exact outbound boundary rejected: %v", err)
	}
	if err := NewJSONLines(strings.NewReader(""), io.Discard, nil).Write(json.RawMessage(strings.TrimSpace(frame))); err == nil {
		t.Fatal("oversized outbound frame accepted")
	}
}

func TestJSONLinesRejectsTrailingValuesAndCompletesShortWrites(t *testing.T) {
	var value any
	if err := NewJSONLines(strings.NewReader(`{} {}`+"\n"), io.Discard, nil).Decode(&value); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	writer := &shortWriter{max: 2}
	if err := NewJSONLines(strings.NewReader(""), writer, nil).Write(map[string]int{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if string(writer.output) != "{\"x\":1}\n" {
		t.Fatalf("short writes produced %q", writer.output)
	}
	zero := &shortWriter{zero: true}
	if err := NewJSONLines(strings.NewReader(""), zero, nil).Write(map[string]int{"x": 1}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write = %v", err)
	}
	if err := decodeUseNumber([]byte(`{} {}`), &value); err == nil {
		t.Fatal("helper accepted trailing JSON")
	}
}

func TestJSONRPCIDsRoundTripWithoutChangingType(t *testing.T) {
	var output bytes.Buffer
	s := &codexSession{process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), &output, nil)}}
	if err := s.RespondPermission(context.Background(), json.RawMessage(`"9007199254740993"`), "accept"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"id":"9007199254740993"`) {
		t.Fatalf("string id changed type: %s", output.String())
	}
	output.Reset()
	if err := s.RespondPermission(context.Background(), json.RawMessage(`9007199254740993`), "accept"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"id":9007199254740993`) {
		t.Fatalf("numeric id changed: %s", output.String())
	}
}

func TestPendingRPCLimitsAreBounded(t *testing.T) {
	s := &codexSession{pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := uint64(0); id < maxPendingRPCs; id++ {
		if err := s.reservePendingLocked(id, 1, "x", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.reservePendingLocked(999, 1, "x", nil); err == nil {
		t.Fatal("pending count limit not enforced")
	}
}

func TestSlowSinkQueueIsBoundedAndClosesInput(t *testing.T) {
	var closes atomic.Int32
	s := &codexSession{eventQueue: make(chan Event, 1), process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, closerFunc(func() error { closes.Add(1); return nil }))}}
	if !s.emit(Event{Kind: EventTextDelta, Text: "one"}) {
		t.Fatal("first event rejected")
	}
	if s.emit(Event{Kind: EventTextDelta, Text: "two"}) {
		t.Fatal("overflow event accepted")
	}
	if !s.slowSink.Load() || closes.Load() != 1 {
		t.Fatalf("overflow did not terminate transport: slow=%v closes=%d", s.slowSink.Load(), closes.Load())
	}
}

func TestTerminalFailureCompletesPendingExactlyOnce(t *testing.T) {
	s := &codexSession{eventQueue: make(chan Event, 4), process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, nil)}, pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
	call := make(chan RPCMessage, 1)
	s.mu.Lock()
	if err := s.reservePendingLocked(1, 1, "", call); err != nil {
		t.Fatal(err)
	}
	if err := s.reservePendingLocked(2, 1, "focus", nil); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	s.failPending("gone")
	s.failPending("gone again")
	response, ok := <-call
	if !ok || !strings.Contains(string(response.Error), "gone") {
		t.Fatalf("pending response: %#v", response)
	}
	if _, ok := <-call; ok {
		t.Fatal("pending call completed twice")
	}
	select {
	case e := <-s.eventQueue:
		if e.Kind != EventSteerRejected || e.Text != "focus" {
			t.Fatalf("steer terminal event: %#v", e)
		}
	default:
		t.Fatal("steer was not rejected")
	}
	select {
	case e := <-s.eventQueue:
		t.Fatalf("duplicate terminal event: %#v", e)
	default:
	}
}

func TestTerminalFailureRejectsSteersInSubmissionOrder(t *testing.T) {
	s := &codexSession{eventQueue: make(chan Event, 4), process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, nil)}, pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
	s.mu.Lock()
	for id, text := range []string{"first", "second", "third"} {
		if err := s.reservePendingLocked(uint64(id+1), 1, text, nil); err != nil {
			t.Fatal(err)
		}
	}
	// A response that arrived before exit must not leave a stale ordering entry.
	s.releasePendingLocked(2)
	s.mu.Unlock()
	s.failPending("gone")
	for _, want := range []string{"first", "third"} {
		select {
		case event := <-s.eventQueue:
			if event.Kind != EventSteerRejected || event.Text != want {
				t.Fatalf("terminal steer order: got %#v, want %q", event, want)
			}
		default:
			t.Fatalf("missing terminal rejection for %q", want)
		}
	}
}

func TestCorrelatedResponsesValidateBeforePublishingOutcome(t *testing.T) {
	invalid := []string{
		`{"id":7}`,
		`{"id":7,"method":null,"result":{}}`,
		`{"id":7,"result":{},"error":{"code":-1,"message":"provider failed"}}`,
		`{"id":7,"error":"provider failed"}`,
		`{"id":7,"error":{"message":"provider failed"}}`,
	}
	for _, raw := range invalid {
		s := &codexSession{eventQueue: make(chan Event, 2), process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, nil)}, pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
		s.mu.Lock()
		if err := s.reservePendingLocked(7, 5, "focus", nil); err != nil {
			t.Fatal(err)
		}
		s.mu.Unlock()
		if _, err := decodeRPCMessage([]byte(raw)); err == nil {
			t.Fatalf("accepted malformed correlated response %s", raw)
		} else if id, ok := malformedResponseID([]byte(raw)); !ok || !s.rejectCorrelated(id, err) {
			t.Fatalf("did not correlate malformed response %s", raw)
		}
		event := <-s.eventQueue
		if event.Kind != EventSteerRejected || event.Text != "focus" || event.Summary == "" {
			t.Fatalf("malformed response published %#v", event)
		}
		select {
		case extra := <-s.eventQueue:
			t.Fatalf("malformed response also published %#v", extra)
		default:
		}
	}

	s := &codexSession{eventQueue: make(chan Event, 1), pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
	s.mu.Lock()
	if err := s.reservePendingLocked(7, 5, "focus", nil); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	rpc, err := decodeRPCMessage([]byte(`{"id":7,"error":{"code":-32000,"message":"provider failed"}}`))
	if err != nil || !s.handleCorrelatedResponse(rpc) {
		t.Fatalf("structured error was not correlated: %#v, %v", rpc, err)
	}
	if event := <-s.eventQueue; event.Kind != EventSteerRejected || event.Summary != "provider failed" {
		t.Fatalf("structured error outcome %#v", event)
	}
}

func TestCorrelatedCallCancellationReleasesBudget(t *testing.T) {
	s := &codexSession{process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, nil)}, pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
	s.next.Store(10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.call(ctx, "thread/rollback", map[string]any{"numTurns": 1}); err != context.Canceled {
		t.Fatalf("call error = %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) != 0 || s.pendingBytes != 0 {
		t.Fatalf("canceled call leaked budget: pending=%d bytes=%d", len(s.pending), s.pendingBytes)
	}
}

func TestRewindAndForkRejectOverflowBeforeRPC(t *testing.T) {
	s := &codexSession{normalizer: &CodexNormalizer{threadID: "thread", turnIDs: []string{"turn"}}, pending: map[uint64]chan RPCMessage{}, pendingSizes: map[uint64]int{}, pendingSteers: map[uint64]string{}, pendingGoals: map[uint64]string{}}
	if _, err := s.Rollback(context.Background(), ^uint64(0)); err == nil {
		t.Fatal("rollback overflow accepted")
	}
	if _, err := s.Fork(context.Background(), ^uint64(0)); err == nil {
		t.Fatal("fork overflow accepted")
	}
}

func TestGoalHonorsAlreadyCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &codexSession{}
	if err := s.Goal(ctx, GoalOperation{Kind: "refresh"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("goal error = %v", err)
	}
}

func TestEventByteLimitBoundary(t *testing.T) {
	var closes atomic.Int32
	s := &codexSession{eventQueue: make(chan Event, 2), process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, closerFunc(func() error { closes.Add(1); return nil }))}}
	exact := Event{Kind: EventTextDelta, Text: strings.Repeat("x", maxQueuedEventBytes-64)}
	if eventSize(exact) != maxQueuedEventBytes || !s.emit(exact) {
		t.Fatal("exact event-byte boundary rejected")
	}
	s.eventMu.Lock()
	s.eventBytes = 0
	s.eventQueue = make(chan Event, 2)
	s.eventMu.Unlock()
	over := exact
	over.Text += "x"
	if s.emit(over) {
		t.Fatal("oversized event accepted")
	}
	if closes.Load() != 1 {
		t.Fatalf("overflow close count = %d", closes.Load())
	}
}
