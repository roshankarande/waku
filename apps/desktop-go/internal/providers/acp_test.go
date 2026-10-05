package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeACPConfig struct {
	capabilities map[string]any
	handle       func(map[string]json.RawMessage, *JSONLines) bool
	exitOnPrompt bool
}

type failFirstWriter struct {
	mu     sync.Mutex
	failed bool
	max    int
	output strings.Builder
}

func (w *failFirstWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.failed {
		w.failed = true
		return 0, errors.New("transient provider write failure")
	}
	if w.max > 0 && len(p) > w.max {
		p = p[:w.max]
	}
	return w.output.Write(p)
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	output  strings.Builder
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.output.Write(p)
}

func fakeACPProcess(t *testing.T, config fakeACPConfig) *StdioProcess {
	t.Helper()
	providerIn, clientIn := io.Pipe()
	clientOut, providerOut := io.Pipe()
	wait := make(chan error, 1)
	process := &StdioProcess{Lines: NewJSONLines(clientOut, clientIn, clientIn), Stderr: strings.NewReader("sanitized ACP diagnostic\n"), wait: wait}
	go func() {
		defer close(wait)
		defer providerOut.Close()
		lines := NewJSONLines(providerIn, providerOut, nil)
		for {
			var message map[string]json.RawMessage
			if err := lines.Decode(&message); err != nil {
				wait <- nil
				return
			}
			if config.handle != nil && config.handle(message, lines) {
				if config.exitOnPrompt && rawString(message["method"]) == "session/prompt" {
					wait <- nil
					return
				}
				continue
			}
			method := rawString(message["method"])
			switch method {
			case "initialize":
				caps := config.capabilities
				if caps == nil {
					caps = map[string]any{"loadSession": true, "sessionCapabilities": map[string]any{"resume": map[string]any{}}}
				}
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]any{"protocolVersion": 1, "agentCapabilities": caps}})
			case "session/new":
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": fakeACPSessionResult()})
			case "session/resume", "session/load":
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]any{"modes": fakeACPSessionResult()["modes"]}})
			case "session/set_mode":
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]any{}})
			}
		}
	}()
	return process
}

func fakeACPSessionResult() map[string]any {
	return map[string]any{
		"sessionId": "acp-fixture",
		"modes":     map[string]any{"currentModeId": "code", "availableModes": []any{map[string]string{"id": "ask"}, map[string]string{"id": "code"}}},
	}
}

func startFakeACP(t *testing.T, provider Provider, config fakeACPConfig, options StartOptions) (Session, *eventRecorder) {
	t.Helper()
	adapter := AcpAdapter{Provider: provider, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeACPProcess(t, config), nil
	}}
	recorder := newEventRecorder()
	session, err := adapter.Start(context.Background(), options, recorder)
	if err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventConnected)
	return session, recorder
}

func TestACPFixturePreservesOrderedStreamAndOpaqueNumbers(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "providers", "acp-session.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) != "session/prompt" {
			return false
		}
		for _, frame := range strings.Split(strings.TrimSpace(string(fixture)), "\n") {
			_ = lines.Write(json.RawMessage(frame))
		}
		return true
	}}
	session, recorder := startFakeACP(t, ProviderCursor, config, StartOptions{Access: AccessAsk})
	if err := session.Prompt(context.Background(), "inspect fixture"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if !finished.Success {
		t.Fatalf("turn failed: %#v", finished)
	}
	recorder.mu.Lock()
	events := append([]Event(nil), recorder.events...)
	recorder.mu.Unlock()
	var visible []EventKind
	var activities []*Activity
	var usage Event
	for _, event := range events {
		if event.Kind != EventConnected {
			visible = append(visible, event.Kind)
		}
		if event.Activity != nil {
			activities = append(activities, event.Activity)
		}
		if event.Kind == EventUsageUpdated {
			usage = event
		}
		if event.Kind == EventAvailableCommands && string(event.Raw) != `[{"name":"review","description":"Review changes"}]` {
			t.Fatalf("available commands payload drifted: %s", event.Raw)
		}
		if strings.Contains(string(event.Raw), "must-not-surface") {
			t.Fatal("private ACP notification surfaced")
		}
	}
	want := []EventKind{EventTurnStarted, EventReasoningDelta, EventActivity, EventActivity, EventTextDelta, EventUsageUpdated, EventAutoTitleUpdated, EventAvailableCommands, EventTurnFinished}
	if !reflect.DeepEqual(visible, want) {
		t.Fatalf("event order = %v, want %v", visible, want)
	}
	if len(activities) != 2 || activities[1].ID != "tool-1" || activities[1].Kind != "fileRead" || activities[1].Title != "Read fixture" || !activities[1].Complete {
		t.Fatalf("tool correlation drifted: %#v", activities)
	}
	if usage.ContextTokens == nil || *usage.ContextTokens != 9007199254740993 || usage.ContextWindow == nil || *usage.ContextWindow != ^uint64(0) {
		t.Fatalf("usage lost uint64 fidelity: %#v", usage)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestGrokPromptCorrelationAndExtensionCompletion(t *testing.T) {
	var promptMeta map[string]string
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) != "session/prompt" {
			return false
		}
		var params map[string]json.RawMessage
		_ = decodeUseNumber(message["params"], &params)
		_ = decodeUseNumber(params["_meta"], &promptMeta)
		_ = lines.Write(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/session/prompt_complete", "params": map[string]any{"sessionId": "acp-fixture", "promptId": promptMeta["promptId"], "stopReason": "end_turn"}})
		// Some Grok versions also send the ordinary response later. It must be
		// consumed without settling the merged turn twice.
		_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]string{"stopReason": "end_turn"}})
		return true
	}}
	session, recorder := startFakeACP(t, ProviderGrok, config, StartOptions{})
	if err := session.Prompt(context.Background(), "grok"); err != nil {
		t.Fatal(err)
	}
	if !recorder.wait(t, EventTurnFinished).Success {
		t.Fatal("Grok extension completion failed")
	}
	if promptMeta["promptId"] == "" || promptMeta["requestId"] != promptMeta["promptId"] {
		t.Fatalf("Grok correlation metadata: %#v", promptMeta)
	}
	time.Sleep(5 * time.Millisecond)
	recorder.mu.Lock()
	count := 0
	for _, event := range recorder.events {
		if event.Kind == EventTurnFinished {
			count++
		}
	}
	recorder.mu.Unlock()
	if count != 1 {
		t.Fatalf("Grok prompt settled %d times", count)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestKimiEmptyTurnRecoversOnlyPostPromptWireFailure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("KIMI_CODE_HOME", home)
	wire := filepath.Join(home, "sessions", "workspace-hash", "acp-fixture", "agents", "main", "wire.jsonl")
	if err := os.MkdirAll(filepath.Dir(wire), 0o700); err != nil {
		t.Fatal(err)
	}
	stale := []byte("{\"type\":\"turn.ended\",\"turnId\":1,\"reason\":\"failed\",\"error\":{\"message\":\"stale quota error\"}}\n")
	if err := os.WriteFile(wire, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join("..", "..", "testdata", "providers", "kimi-wire-current.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) != "session/prompt" {
			return false
		}
		file, _ := os.OpenFile(wire, os.O_APPEND|os.O_WRONLY, 0o600)
		_, _ = file.Write(current)
		_ = file.Close()
		_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]string{"stopReason": "end_turn"}})
		return true
	}}
	session, recorder := startFakeACP(t, ProviderKimi, config, StartOptions{})
	if err := session.Prompt(context.Background(), "quota"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if finished.Success || finished.Summary != "402 membership inactive" || strings.Contains(finished.Summary, "stale") {
		t.Fatalf("Kimi recovered failure = %#v", finished)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestKimiSteerRecoveryUsesPerPromptProducedState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("KIMI_CODE_HOME", home)
	wire := filepath.Join(home, "sessions", "workspace-hash", "acp-fixture", "agents", "main", "wire.jsonl")
	if err := os.MkdirAll(filepath.Dir(wire), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wire, []byte("{\"type\":\"turn.ended\",\"reason\":\"completed\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	failure, err := os.ReadFile(filepath.Join("..", "..", "testdata", "providers", "kimi-wire-current.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var firstID json.RawMessage
	prompts := 0
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) != "session/prompt" {
			return false
		}
		prompts++
		if prompts == 1 {
			firstID = cloneRaw(message["id"])
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "acp-fixture", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": "before steer"}}}})
			return true
		}
		file, _ := os.OpenFile(wire, os.O_APPEND|os.O_WRONLY, 0o600)
		_, _ = file.Write(failure)
		_ = file.Close()
		_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": firstID, "result": map[string]string{"stopReason": "cancelled"}})
		_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]string{"stopReason": "end_turn"}})
		return true
	}}
	session, recorder := startFakeACP(t, ProviderKimi, config, StartOptions{})
	if err := session.Prompt(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventTextDelta)
	if err := session.Steer(context.Background(), "steered request"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if finished.Success || finished.Summary != "402 membership inactive" {
		t.Fatalf("steered Kimi failure was suppressed by earlier content: %#v", finished)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestACPHandshakeCapabilitiesResumeFallbackAndFxMode(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	var initialize map[string]json.RawMessage
	config := fakeACPConfig{
		capabilities: map[string]any{"loadSession": true, "sessionCapabilities": map[string]any{"resume": map[string]any{}}},
		handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
			method := rawString(message["method"])
			mu.Lock()
			methods = append(methods, method)
			if method == "initialize" {
				_ = decodeUseNumber(message["params"], &initialize)
			}
			mu.Unlock()
			if method == "session/resume" {
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "error": map[string]any{"code": -32001, "message": "session expired"}})
				return true
			}
			if method == "session/load" {
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "old", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": "imported"}}}})
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]any{"modes": fakeACPSessionResult()["modes"]}})
				return true
			}
			return false
		},
	}
	cursor := json.RawMessage(`{"provider":"fx","sessionId":"old"}`)
	session, recorder := startFakeACP(t, ProviderFx, config, StartOptions{Access: AccessAsk, ResumeCursor: cursor})
	mu.Lock()
	gotMethods := append([]string(nil), methods...)
	mu.Unlock()
	if !reflect.DeepEqual(gotMethods[:4], []string{"initialize", "session/resume", "session/load", "session/set_mode"}) {
		t.Fatalf("handshake order: %v", gotMethods)
	}
	var caps map[string]json.RawMessage
	if err := decodeUseNumber(initialize["clientCapabilities"], &caps); err != nil || rawString(caps["terminal"]) != "" && string(caps["terminal"]) != "false" {
		t.Fatalf("client capabilities: %#v, %v", caps, err)
	}
	if string(caps["terminal"]) != "false" || len(caps["fs"]) == 0 {
		t.Fatalf("unsupported client services advertised: %#v", caps)
	}
	recorder.mu.Lock()
	for _, event := range recorder.events {
		if event.Kind == EventTextDelta && event.Text == "imported" {
			t.Fatal("imported history replay was duplicated into live transcript")
		}
	}
	recorder.mu.Unlock()
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestACPSupervisedPermissionAndAutoApproval(t *testing.T) {
	permissionResponse := make(chan map[string]json.RawMessage, 1)
	requestID := json.RawMessage(`"permission-9007199254740993"`)
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) == "session/prompt" {
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": requestID, "method": "session/request_permission", "params": map[string]any{
				"sessionId": "acp-fixture", "toolCall": map[string]any{"title": "Run command", "content": []any{map[string]any{"type": "content", "content": map[string]string{"type": "text", "text": "Not in allowlist: cat"}}}},
				"options": []any{map[string]string{"optionId": "once", "name": "Allow once", "kind": "allow_once"}, map[string]string{"optionId": "always", "name": "Always", "kind": "allow_always"}, map[string]string{"optionId": "deny", "name": "Deny", "kind": "reject_once"}},
			}})
			return true
		}
		if rawString(message["method"]) == "" && string(message["id"]) == string(requestID) {
			permissionResponse <- message
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": 11, "result": map[string]string{"stopReason": "end_turn"}})
			return true
		}
		return false
	}}
	session, recorder := startFakeACP(t, ProviderCursor, config, StartOptions{Access: AccessAsk})
	if err := session.Prompt(context.Background(), "permission"); err != nil {
		t.Fatal(err)
	}
	event := recorder.wait(t, EventPermission)
	if event.Permission.Detail != "Not in allowlist: cat" || len(event.Permission.Options) != 3 || !event.Permission.Options[1].Allow {
		t.Fatalf("permission mapping: %#v", event.Permission)
	}
	if err := session.RespondPermission(context.Background(), event.RequestID, "once"); err != nil {
		t.Fatal(err)
	}
	response := <-permissionResponse
	if !strings.Contains(string(response["result"]), `"optionId":"once"`) || string(response["id"]) != string(requestID) {
		t.Fatalf("permission response lost id/outcome: %#v", response)
	}
	recorder.wait(t, EventTurnFinished)
	_ = session.Close()
	recorder.wait(t, EventProcessExited)

	autoResponse := make(chan map[string]json.RawMessage, 1)
	config.handle = func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) == "session/prompt" {
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": 99, "method": "session/request_permission", "params": map[string]any{"sessionId": "acp-fixture", "toolCall": map[string]string{"title": "Tool"}, "options": []any{map[string]string{"optionId": "once", "name": "Once", "kind": "allow_once"}, map[string]string{"optionId": "always", "name": "Always", "kind": "allow_always"}}}})
			return true
		}
		if string(message["id"]) == "99" && rawString(message["method"]) == "" {
			autoResponse <- message
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "acp-fixture", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": "approved"}}}})
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": 11, "result": map[string]string{"stopReason": "end_turn"}})
			return true
		}
		return false
	}
	auto, autoRecorder := startFakeACP(t, ProviderKimi, config, StartOptions{Access: AccessAuto})
	if err := auto.Prompt(context.Background(), "auto"); err != nil {
		t.Fatal(err)
	}
	if response := <-autoResponse; !strings.Contains(string(response["result"]), `"optionId":"always"`) {
		t.Fatalf("auto approval did not prefer durable allow: %#v", response)
	}
	autoRecorder.wait(t, EventTurnFinished)
	_ = auto.Close()
	autoRecorder.wait(t, EventProcessExited)
}

func TestACPCursorQuestionsAndCapabilityRestrictions(t *testing.T) {
	response := make(chan map[string]json.RawMessage, 1)
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) == "session/prompt" {
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": "question-id", "method": "cursor/ask_question", "params": map[string]any{"questions": []any{map[string]any{"id": "q1", "prompt": "Choose", "allowMultiple": true, "options": []any{map[string]string{"label": "A"}, map[string]string{"label": "B"}}}}}})
			return true
		}
		if string(message["id"]) == `"question-id"` && rawString(message["method"]) == "" {
			response <- message
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": 11, "result": map[string]string{"stopReason": "end_turn"}})
			return true
		}
		return false
	}}
	session, recorder := startFakeACP(t, ProviderCursor, config, StartOptions{})
	if err := session.Prompt(context.Background(), "question"); err != nil {
		t.Fatal(err)
	}
	event := recorder.wait(t, EventUserInputRequested)
	if len(event.Questions) != 1 || !event.Questions[0].MultiSelect {
		t.Fatalf("questions: %#v", event.Questions)
	}
	if err := session.RespondQuestions(context.Background(), event.RequestID, []UserInputAnswer{{QuestionID: "q1", Answers: []string{"A", "B"}}}); err != nil {
		t.Fatal(err)
	}
	if got := <-response; !strings.Contains(string(got["result"]), `"q1":["A","B"]`) {
		t.Fatalf("question response: %#v", got)
	}
	recorder.wait(t, EventTurnFinished)
	_ = session.Close()
	recorder.wait(t, EventProcessExited)

	for _, provider := range []Provider{ProviderCursor, ProviderFx, ProviderGrok, ProviderKimi} {
		d := Descriptors()[provider]
		if !d.Implemented || !d.Capabilities.Approvals || !d.Capabilities.Resume || d.Capabilities.ImportedHistory || d.Capabilities.Models || d.Capabilities.Fork || d.Capabilities.Rewind || d.Capabilities.ComputerUse {
			t.Fatalf("dishonest %s capabilities: %#v", provider, d.Capabilities)
		}
	}
	if Descriptors()[ProviderFx].Capabilities.Steering || Descriptors()[ProviderKimi].Capabilities.Questions {
		t.Fatal("provider-specific ACP restriction missing")
	}
}

func TestACPXAIQuestionPreservesNotesAndPreview(t *testing.T) {
	params := json.RawMessage(`{"questions":[{"id":"q","question":"Pick","options":[{"label":"A","description":"first","preview":"preview A"}],"multiSelect":false}]}`)
	questions, err := parseACPQuestions(params, "xai")
	if err != nil || len(questions) != 1 || questions[0].Options[0].Description != "first" {
		t.Fatalf("xAI questions = %#v, %v", questions, err)
	}
	response, err := acpQuestionResponse("xai", params, []UserInputAnswer{{QuestionID: "q", Answers: []string{"A", "extra context"}}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(response)
	if !strings.Contains(string(encoded), `"preview":"preview A"`) || !strings.Contains(string(encoded), `"notes":"extra context"`) {
		t.Fatalf("xAI response lost annotations: %s", encoded)
	}
}

func TestACPProviderSpecificOperationalGates(t *testing.T) {
	fx := &acpSession{provider: ProviderFx, pendingPrompts: map[uint64]acpPrompt{1: {size: 1}}}
	if err := fx.Steer(context.Background(), "not supported"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Fx steer = %v", err)
	}
	kimi := &acpSession{provider: ProviderKimi}
	if _, err := kimi.Fork(context.Background(), 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Kimi fork = %v", err)
	}
	if _, err := kimi.Rollback(context.Background(), 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Kimi rewind = %v", err)
	}
}

func TestACPCancelEOFMalformedAndLimits(t *testing.T) {
	cancelled := make(chan struct{}, 1)
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		switch rawString(message["method"]) {
		case "session/prompt":
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "wrong", "update": map[string]string{"sessionUpdate": "agent_message_chunk"}}})
			return true
		case "session/cancel":
			cancelled <- struct{}{}
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": 11, "result": map[string]string{"stopReason": "cancelled"}})
			return true
		}
		return false
	}}
	session, recorder := startFakeACP(t, ProviderGrok, config, StartOptions{})
	if err := session.Prompt(context.Background(), "cancel"); err != nil {
		t.Fatal(err)
	}
	malformed := recorder.wait(t, EventError)
	if !strings.Contains(malformed.Text, "unexpected sessionId") {
		t.Fatalf("malformed update was not rejected: %#v", malformed)
	}
	if err := session.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-cancelled
	if finished := recorder.wait(t, EventTurnFinished); !finished.Success {
		t.Fatalf("ACP cancelled stop reason = %#v", finished)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)

	bounded := &acpSession{provider: ProviderCursor, session: "s", process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, nil)}, eventQueue: make(chan Event, 256), pendingPrompts: make(map[uint64]acpPrompt), permissions: make(map[string]acpPermissionRequest), questions: make(map[string]acpQuestionRequest), normal: newACPNormalizer(ProviderCursor, "s")}
	for i := 0; i < maxPendingRPCs; i++ {
		if err := bounded.startPrompt("x", i != 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := bounded.startPrompt("overflow", true); err == nil {
		t.Fatal("ACP pending request limit was not enforced")
	}
}

func TestACPEOFFailsActivePromptExactlyOnce(t *testing.T) {
	config := fakeACPConfig{exitOnPrompt: true, handle: func(message map[string]json.RawMessage, _ *JSONLines) bool {
		return rawString(message["method"]) == "session/prompt"
	}}
	session, recorder := startFakeACP(t, ProviderCursor, config, StartOptions{})
	if err := session.Prompt(context.Background(), "will exit"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if finished.Success || !strings.Contains(finished.Summary, "exited") {
		t.Fatalf("EOF turn outcome: %#v", finished)
	}
	recorder.wait(t, EventProcessExited)
	time.Sleep(5 * time.Millisecond)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	count := 0
	for _, event := range recorder.events {
		if event.Kind == EventTurnFinished {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("EOF settled turn %d times", count)
	}
	_ = session.Close()
}

func TestACPMalformedCorrelatedResponseFailsPrompt(t *testing.T) {
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) != "session/prompt" {
			return false
		}
		_ = lines.Write(json.RawMessage(`{"jsonrpc":"2.0","id":11}`))
		return true
	}}
	session, recorder := startFakeACP(t, ProviderCursor, config, StartOptions{})
	if err := session.Prompt(context.Background(), "malformed"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if finished.Success || !strings.Contains(finished.Summary, "exactly one") {
		t.Fatalf("malformed response outcome: %#v", finished)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestACPUnfinishedToolStateOverflowFailsSession(t *testing.T) {
	config := fakeACPConfig{handle: func(message map[string]json.RawMessage, lines *JSONLines) bool {
		if rawString(message["method"]) != "session/prompt" {
			return false
		}
		for index := 0; index <= maxACPTools; index++ {
			_ = lines.Write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "acp-fixture", "update": map[string]any{"sessionUpdate": "tool_call", "toolCallId": fmt.Sprintf("tool-%d", index), "title": "pending", "status": "pending"}}})
		}
		return true
	}}
	session, recorder := startFakeACP(t, ProviderCursor, config, StartOptions{})
	if err := session.Prompt(context.Background(), "many tools"); err != nil {
		t.Fatal(err)
	}
	errorEvent := recorder.wait(t, EventError)
	if !strings.Contains(errorEvent.Text, "tool state limit") {
		t.Fatalf("tool overflow = %#v", errorEvent)
	}
	if recorder.wait(t, EventTurnFinished).Success {
		t.Fatal("tool overflow reported success")
	}
	recorder.wait(t, EventProcessExited)
	_ = session.Close()
}

func TestACPStrictHandshakeShapes(t *testing.T) {
	for _, raw := range []string{
		`null`,
		`{"protocolVersion":1,"agentCapabilities":null}`,
		`{"protocolVersion":1,"agentCapabilities":{"loadSession":null}}`,
		`{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":true}}}`,
	} {
		if _, err := parseACPInitialize(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted initialize %s", raw)
		}
	}
	for _, raw := range []string{
		`null`,
		`{"sessionId":"s","modes":null}`,
		`{"sessionId":"s","modes":{"currentModeId":"ask"}}`,
		`{"sessionId":"s","modes":{"currentModeId":"ask","availableModes":null}}`,
		`{"sessionId":"s","modes":{"currentModeId":1,"availableModes":[]}}`,
		`{"sessionId":"s","configOptions":null}`,
		`{"sessionId":"s","configOptions":[null]}`,
	} {
		if _, err := parseACPSessionResult(json.RawMessage(raw), true); err == nil {
			t.Fatalf("accepted session result %s", raw)
		}
	}
	if _, err := parseACPInitialize(json.RawMessage(`{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"resume":{}}}}`)); err != nil {
		t.Fatal(err)
	}
}

func TestACPConfigOptionsMatchTypedSDKShape(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "providers", "acp-config-options.json"))
	if err != nil {
		t.Fatal(err)
	}
	result := append([]byte(`{"sessionId":"s","configOptions":`), fixture...)
	result = append(result, '}')
	if _, err := parseACPSessionResult(result, true); err != nil {
		t.Fatalf("rejected ACP-shaped config options: %v", err)
	}
	malformed := []string{
		`{}`,
		`{"id":"mode","name":"Mode","type":"select","options":[]}`,
		`{"id":"mode","name":"Mode","type":"select","currentValue":"ask"}`,
		`{"id":"fast","name":"Fast","type":"boolean","currentValue":"false"}`,
		`{"id":"fast","name":"Fast","description":7,"type":"boolean","currentValue":false}`,
		`{"id":"fast","name":"Fast","category":{},"type":"boolean","currentValue":false}`,
		`{"id":"mode","name":"Mode","type":"unknown","currentValue":"ask"}`,
		`{"id":"mode","name":"Mode","type":"select","currentValue":"ask","options":[{"value":"ask","name":"Ask"},{"group":"g","name":"Group","options":[]}]}`,
		`{"id":"mode","name":"Mode","type":"select","currentValue":"ask","options":[{"group":"g","name":"Group","options":[{}]}]}`,
		`{"id":"mode","name":"Mode","type":"select","currentValue":"ask","options":[{"value":"ask","name":"Ask","_meta":false}]}`,
	}
	for _, option := range malformed {
		raw := json.RawMessage(`{"sessionId":"s","configOptions":[` + option + `]}`)
		if _, err := parseACPSessionResult(raw, true); err == nil {
			t.Fatalf("accepted malformed config option %s", option)
		}
	}
}

func TestACPDuplicateInboundRequestIDDoesNotLeakAccounting(t *testing.T) {
	var output strings.Builder
	s := &acpSession{
		provider: ProviderCursor, session: "s", access: AccessAsk,
		process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), &output, nil)}, eventQueue: make(chan Event, 2),
		permissions: make(map[string]acpPermissionRequest), questions: make(map[string]acpQuestionRequest), inboundSizes: make(map[string]int),
	}
	params := json.RawMessage(`{"sessionId":"s","toolCall":{"title":"Tool"},"options":[{"optionId":"once","name":"Once","kind":"allow_once"}]}`)
	message := RPCMessage{JSONRPC: "2.0", ID: json.RawMessage(`"duplicate"`), Method: "session/request_permission", Params: params}
	if err := s.handlePermission(message, nil); err != nil {
		t.Fatal(err)
	}
	bytesBefore := s.inboundBytes
	if err := s.handlePermission(message, nil); err == nil {
		t.Fatal("duplicate inbound id accepted")
	}
	if len(s.permissions) != 1 || s.inboundBytes != bytesBefore || len(s.inboundSizes) != 1 {
		t.Fatalf("duplicate changed accounting: pending=%d bytes=%d sizes=%d", len(s.permissions), s.inboundBytes, len(s.inboundSizes))
	}
}

func TestACPPermissionFailedWriteRetainsExactRequestForRetry(t *testing.T) {
	writer := &failFirstWriter{max: 2}
	id := json.RawMessage(`"permission-retry"`)
	key := string(id)
	s := &acpSession{
		process:     &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), writer, nil)},
		permissions: map[string]acpPermissionRequest{key: {id: cloneRaw(id), options: map[string]struct{}{"once": {}}}},
		questions:   make(map[string]acpQuestionRequest), inboundSizes: map[string]int{key: 37}, inboundBytes: 37,
	}
	if err := s.RespondPermission(context.Background(), id, "once"); err == nil {
		t.Fatal("failed provider write reported success")
	}
	s.mu.Lock()
	pending := s.permissions[key]
	if len(s.permissions) != 1 || pending.resolving || s.inboundBytes != 37 || s.inboundSizes[key] != 37 || string(pending.id) != string(id) {
		t.Fatalf("failed write changed pending request: %#v bytes=%d sizes=%v", pending, s.inboundBytes, s.inboundSizes)
	}
	s.mu.Unlock()
	if err := s.RespondPermission(context.Background(), id, "once"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if len(s.permissions) != 0 || s.inboundBytes != 0 || len(s.inboundSizes) != 0 {
		t.Fatalf("successful retry retained accounting: pending=%d bytes=%d sizes=%d", len(s.permissions), s.inboundBytes, len(s.inboundSizes))
	}
	s.mu.Unlock()
	if got := writer.output.String(); !strings.Contains(got, `"id":"permission-retry"`) || !strings.HasSuffix(got, "\n") {
		t.Fatalf("permission retry frame = %q", got)
	}
}

func TestACPQuestionFailedWriteRetainsExactRequestForRetry(t *testing.T) {
	writer := &failFirstWriter{max: 3}
	id := json.RawMessage(`9007199254740993`)
	key := string(id)
	params := json.RawMessage(`{"questions":[{"id":"q","prompt":"Choose","options":[{"label":"A"}]}]}`)
	s := &acpSession{
		process: &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), writer, nil)}, permissions: make(map[string]acpPermissionRequest),
		questions:    map[string]acpQuestionRequest{key: {id: cloneRaw(id), method: "cursor", params: cloneRaw(params)}},
		inboundSizes: map[string]int{key: 73}, inboundBytes: 73,
	}
	answers := []UserInputAnswer{{QuestionID: "q", Answers: []string{"A"}}}
	if err := s.RespondQuestions(context.Background(), id, answers); err == nil {
		t.Fatal("failed question write reported success")
	}
	s.mu.Lock()
	pending := s.questions[key]
	if len(s.questions) != 1 || pending.resolving || s.inboundBytes != 73 || string(pending.params) != string(params) || string(pending.id) != string(id) {
		t.Fatalf("failed question write changed pending request: %#v bytes=%d", pending, s.inboundBytes)
	}
	s.mu.Unlock()
	if err := s.RespondQuestions(context.Background(), id, answers); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if len(s.questions) != 0 || s.inboundBytes != 0 || len(s.inboundSizes) != 0 {
		t.Fatalf("successful question retry retained accounting: pending=%d bytes=%d", len(s.questions), s.inboundBytes)
	}
	s.mu.Unlock()
	if got := writer.output.String(); !strings.Contains(got, `"id":9007199254740993`) || !strings.Contains(got, `"q":"A"`) {
		t.Fatalf("question retry frame = %q", got)
	}
}

func TestACPConcurrentPermissionResolutionIsSerialized(t *testing.T) {
	writer := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	id := json.RawMessage(`"permission-concurrent"`)
	key := string(id)
	s := &acpSession{
		process:     &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), writer, nil)},
		permissions: map[string]acpPermissionRequest{key: {id: cloneRaw(id), options: map[string]struct{}{"once": {}}}},
		questions:   make(map[string]acpQuestionRequest), inboundSizes: map[string]int{key: 1}, inboundBytes: 1,
	}
	first := make(chan error, 1)
	go func() { first <- s.RespondPermission(context.Background(), id, "once") }()
	<-writer.entered
	if err := s.RespondPermission(context.Background(), id, "once"); err == nil || !strings.Contains(err.Error(), "already resolving") {
		t.Fatalf("concurrent resolution = %v", err)
	}
	close(writer.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestACPCloseClearsRetryableInboundRequests(t *testing.T) {
	wait := make(chan error, 1)
	wait <- nil
	close(wait)
	id := json.RawMessage(`"close-pending"`)
	key := string(id)
	s := &acpSession{
		process:      &StdioProcess{Lines: NewJSONLines(strings.NewReader(""), io.Discard, closerFunc(func() error { return nil })), wait: wait},
		permissions:  map[string]acpPermissionRequest{key: {id: cloneRaw(id), options: map[string]struct{}{"once": {}}}},
		questions:    map[string]acpQuestionRequest{`"question"`: {id: json.RawMessage(`"question"`), method: "cursor", params: json.RawMessage(`{"questions":[]}`)}},
		inboundSizes: map[string]int{key: 4, `"question"`: 5}, inboundBytes: 9,
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(s.permissions) != 0 || len(s.questions) != 0 || len(s.inboundSizes) != 0 || s.inboundBytes != 0 {
		t.Fatalf("Close retained inbound requests: permissions=%d questions=%d sizes=%d bytes=%d", len(s.permissions), len(s.questions), len(s.inboundSizes), s.inboundBytes)
	}
}

func TestACPDiscoveryLaunchAndOptionsValidatedBeforeSpawn(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "cursor-agent")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	discovery, err := (AcpAdapter{Provider: ProviderCursor}).Discover(context.Background(), StartOptions{Binary: executable})
	if err != nil || !discovery.Available || discovery.Executable != executable {
		t.Fatalf("discovery = %#v, %v", discovery, err)
	}
	missing, err := (AcpAdapter{Provider: ProviderCursor}).Discover(context.Background(), StartOptions{Binary: filepath.Join(t.TempDir(), "missing")})
	if err != nil || missing.Available || missing.Reason == "" {
		t.Fatalf("missing discovery = %#v, %v", missing, err)
	}
	var starts atomic.Int32
	adapter := AcpAdapter{Provider: ProviderKimi, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		starts.Add(1)
		return nil, errors.New("must not spawn")
	}}
	for _, options := range []StartOptions{
		{Access: "invalid"}, {Model: "unimplemented"}, {ReasoningEffort: "high"}, {ComputerUse: true},
		{ResumeCursor: json.RawMessage(`{"provider":"grok","sessionId":"wrong"}`)},
	} {
		if _, err := adapter.Start(context.Background(), options, SinkFunc(func(Event) {})); err == nil {
			t.Fatalf("invalid options accepted: %#v", options)
		}
	}
	if starts.Load() != 0 {
		t.Fatalf("invalid options spawned %d processes", starts.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Start(ctx, StartOptions{}, SinkFunc(func(Event) {})); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled start = %v", err)
	}
}

func TestACPProviderLaunchArguments(t *testing.T) {
	want := map[Provider][]string{ProviderCursor: {"acp"}, ProviderFx: {"acp"}, ProviderGrok: {"agent", "stdio"}, ProviderKimi: {"acp"}}
	for provider, args := range want {
		if got := acpArgs(provider); !reflect.DeepEqual(got, args) {
			t.Fatalf("%s args = %v, want %v", provider, got, args)
		}
	}
}

func TestACPGoalAndHistoryControlsRemainExplicitlyUnsupported(t *testing.T) {
	s := &acpSession{}
	if err := s.Goal(context.Background(), GoalOperation{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := s.Rollback(context.Background(), 1); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := s.Fork(context.Background(), 1); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Goal(ctx, GoalOperation{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestACPStartContextCanCancelBlockedHandshake(t *testing.T) {
	providerIn, clientIn := io.Pipe()
	clientOut, providerOut := io.Pipe()
	wait := make(chan error, 1)
	process := &StdioProcess{Lines: NewJSONLines(clientOut, clientIn, clientIn), Stderr: strings.NewReader(""), wait: wait}
	go func() {
		_, _ = io.Copy(io.Discard, providerIn)
		_ = providerOut.Close()
		wait <- nil
		close(wait)
	}()
	adapter := AcpAdapter{Provider: ProviderCursor, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) { return process, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := adapter.Start(ctx, StartOptions{}, SinkFunc(func(Event) {})); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked handshake cancellation = %v", err)
	}
}

func TestACPStartupCancellationKillsAndReapsUncooperativeProcess(t *testing.T) {
	stdout, providerOut := io.Pipe()
	wait := make(chan error, 1)
	var killed atomic.Int32
	var killOnce sync.Once
	process := &StdioProcess{
		Lines: NewJSONLines(stdout, io.Discard, closerFunc(func() error { return nil })), Stderr: strings.NewReader(""), wait: wait, stdout: stdout,
	}
	process.kill = func() error {
		killOnce.Do(func() {
			killed.Add(1)
			_ = providerOut.Close()
			wait <- nil
			close(wait)
		})
		return nil
	}
	adapter := AcpAdapter{Provider: ProviderCursor, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) { return process, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := adapter.Start(ctx, StartOptions{}, SinkFunc(func(Event) {})); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("start cancellation = %v", err)
	}
	if killed.Load() != 1 || time.Since(started) > time.Second {
		t.Fatalf("uncooperative startup killed=%d elapsed=%s", killed.Load(), time.Since(started))
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("uncooperative startup was not reaped: %v", err)
	}
}

func TestACPCloseKillsAndReapsUncooperativeSession(t *testing.T) {
	providerIn, clientIn := io.Pipe()
	clientOut, providerOut := io.Pipe()
	wait := make(chan error, 1)
	release := make(chan struct{})
	var killed atomic.Int32
	var killOnce sync.Once
	process := &StdioProcess{Lines: NewJSONLines(clientOut, clientIn, clientIn), Stderr: strings.NewReader(""), wait: wait, stdout: clientOut}
	process.kill = func() error {
		killOnce.Do(func() {
			killed.Add(1)
			close(release)
		})
		return nil
	}
	go func() {
		lines := NewJSONLines(providerIn, providerOut, nil)
		for {
			var message map[string]json.RawMessage
			if err := lines.Decode(&message); err != nil {
				<-release
				_ = providerOut.Close()
				wait <- nil
				close(wait)
				return
			}
			switch rawString(message["method"]) {
			case "initialize":
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]any{}}})
			case "session/new":
				_ = lines.Write(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]string{"sessionId": "uncooperative"}})
			}
		}
	}()
	adapter := AcpAdapter{Provider: ProviderCursor, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) { return process, nil }}
	recorder := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventConnected)
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if killed.Load() != 1 {
		t.Fatalf("uncooperative close killed=%d", killed.Load())
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("uncooperative close was not reaped: %v", err)
	}
	recorder.wait(t, EventProcessExited)
}
