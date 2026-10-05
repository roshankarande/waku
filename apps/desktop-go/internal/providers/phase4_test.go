package providers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func fakeNDJSONProcess(t *testing.T, serve func(*JSONLines)) *StdioProcess {
	t.Helper()
	providerIn, clientIn := io.Pipe()
	clientOut, providerOut := io.Pipe()
	stderrOut, stderrIn := io.Pipe()
	wait := make(chan error, 1)
	process := &StdioProcess{Lines: NewJSONLines(clientOut, clientIn, clientIn), Stderr: stderrOut, wait: wait, kill: func() error {
		_ = providerIn.Close()
		_ = providerOut.Close()
		_ = stderrIn.Close()
		return nil
	}, stdout: clientOut, errout: stderrOut}
	go func() {
		serve(NewJSONLines(providerIn, providerOut, providerOut))
		_ = providerOut.Close()
		_ = stderrIn.Close()
		wait <- nil
		close(wait)
	}()
	return process
}

func fixtureFrames(t *testing.T, name string) []json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "providers", name))
	if err != nil {
		t.Fatal(err)
	}
	var frames []json.RawMessage
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		frames = append(frames, json.RawMessage(line))
	}
	return frames
}

func TestPiFixtureHandshakeAndOrderedTurn(t *testing.T) {
	frames := fixtureFrames(t, "pi-session.ndjson")
	var requests []string
	adapter := PiAdapter{Provider: ProviderPi, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeNDJSONProcess(t, func(lines *JSONLines) {
			for {
				var request map[string]json.RawMessage
				if lines.Decode(&request) != nil {
					return
				}
				typ := rawString(request["type"])
				requests = append(requests, typ)
				id := rawString(request["id"])
				switch typ {
				case "get_state":
					_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{"sessionId": "pi-session", "sessionFile": "/safe/pi.jsonl", "sessionName": "Fixture"}})
				case "get_commands":
					_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{"commands": []any{map[string]string{"name": "review", "description": "Review"}}}})
				case "prompt":
					_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{"agentInvoked": true}})
					for _, frame := range frames {
						_ = lines.Write(frame)
					}
				default:
					_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{}})
				}
			}
		}), nil
	}}
	recorder := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{Access: AccessFull}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Prompt(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if !finished.Success {
		t.Fatalf("turn failed: %#v", finished)
	}
	recorder.mu.Lock()
	events := append([]Event(nil), recorder.events...)
	recorder.mu.Unlock()
	var kinds []EventKind
	for _, event := range events {
		if event.Kind != EventConnected && event.Kind != EventAutoTitleUpdated && event.Kind != EventAvailableCommands {
			kinds = append(kinds, event.Kind)
		}
	}
	want := []EventKind{EventTurnStarted, EventReasoningDelta, EventActivity, EventActivity, EventTextDelta, EventTurnFinished}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("order=%v want=%v", kinds, want)
	}
	if len(requests) < 3 || !reflect.DeepEqual(requests[:3], []string{"get_state", "get_state", "get_commands"}) {
		t.Fatalf("handshake=%v", requests)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestPiDiscoveryHonorsBoundedModelAndVersionHints(t *testing.T) {
	versionBinary := filepath.Join(t.TempDir(), "pi-version")
	if err := os.WriteFile(versionBinary, []byte("#!/bin/sh\nprintf 'pi 0.42.0\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	version, err := (PiAdapter{Provider: ProviderPi}).Discover(context.Background(), StartOptions{Binary: versionBinary, ProbeVersion: true})
	if err != nil || !version.Available || version.Version != "pi 0.42.0" {
		t.Fatalf("version discovery = %#v, %v", version, err)
	}

	var gotSpec ProcessSpec
	adapter := PiAdapter{Provider: ProviderOhMyPi, startProcess: func(_ context.Context, spec ProcessSpec) (*StdioProcess, error) {
		gotSpec = spec
		return fakeNDJSONProcess(t, func(lines *JSONLines) {
			for {
				var request map[string]json.RawMessage
				if lines.Decode(&request) != nil {
					return
				}
				id, kind := rawString(request["id"]), rawString(request["type"])
				switch kind {
				case "get_available_models":
					_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{"models": []any{
						map[string]any{"provider": "anthropic", "id": "sonnet", "name": "Sonnet", "reasoning": true, "thinking": map[string]any{"efforts": []string{"low", "high", "max"}}, "contextWindow": json.Number("9007199254740993")},
					}}})
				case "get_state":
					_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{"model": map[string]string{"provider": "anthropic", "id": "sonnet"}, "thinkingLevel": "high"}})
				}
			}
		}), nil
	}}
	discovery, err := adapter.Discover(context.Background(), StartOptions{Binary: "omp-fixture", DiscoverModels: true})
	if err != nil || len(discovery.Models) != 1 || discovery.Models[0].ID != "anthropic/sonnet" || discovery.Models[0].Label != "Sonnet" {
		t.Fatalf("model discovery = %#v, %v", discovery, err)
	}
	if !reflect.DeepEqual(gotSpec.Args, []string{"--mode", "rpc", "--no-session", "--no-skills", "--no-rules", "--no-extensions"}) {
		t.Fatalf("OMP probe args = %v", gotSpec.Args)
	}
	var metadata struct {
		ID                     string              `json:"id"`
		Name                   string              `json:"name"`
		SubProvider            string              `json:"sub_provider"`
		IsDefault              bool                `json:"is_default"`
		ReasoningEfforts       []map[string]string `json:"reasoning_efforts"`
		DefaultReasoningEffort string              `json:"default_reasoning_effort"`
		ContextWindows         []map[string]string `json:"context_windows"`
		DefaultContextWindow   string              `json:"default_context_window"`
	}
	if err := decodeUseNumber(discovery.Models[0].Raw, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.ID != "anthropic/sonnet" || metadata.Name != "Sonnet" || metadata.SubProvider != "Anthropic" || !metadata.IsDefault || metadata.DefaultReasoningEffort != "high" {
		t.Fatalf("protocol metadata drifted: %#v", metadata)
	}
	wantEfforts := []map[string]string{{"id": "off", "label": "Off"}, {"id": "low", "label": "Low"}, {"id": "high", "label": "High"}, {"id": "max", "label": "Max"}}
	if !reflect.DeepEqual(metadata.ReasoningEfforts, wantEfforts) {
		t.Fatalf("OMP reasoning efforts = %#v", metadata.ReasoningEfforts)
	}
	if metadata.DefaultContextWindow != "9007199254740993" || len(metadata.ContextWindows) != 1 || metadata.ContextWindows[0]["id"] != "9007199254740993" {
		t.Fatalf("large context window lost precision: %#v", metadata)
	}
}

func TestPiModelMetadataUsesLevelMapAndStateFallback(t *testing.T) {
	models := map[string]json.RawMessage{"models": json.RawMessage(`[{"provider":"openai","id":"reasoner","reasoning":true,"thinkingLevelMap":{"off":null,"xhigh":"xhigh","max":null}}]`)}
	state := map[string]json.RawMessage{
		"model":         json.RawMessage(`{"provider":"openai","id":"reasoner","contextWindow":18446744073709551615}`),
		"thinkingLevel": json.RawMessage(`"xhigh"`),
	}
	parsed, err := parsePiModels(ProviderPi, models, state)
	if err != nil || len(parsed) != 1 {
		t.Fatalf("Pi metadata parse = %#v, %v", parsed, err)
	}
	var metadata map[string]json.RawMessage
	if err := decodeUseNumber(parsed[0].Raw, &metadata); err != nil {
		t.Fatal(err)
	}
	if rawString(metadata["default_reasoning_effort"]) != "xhigh" || rawString(metadata["default_context_window"]) != "18446744073709551615" {
		t.Fatalf("state defaults lost: %s", parsed[0].Raw)
	}
	var efforts []map[string]string
	if err := decodeUseNumber(metadata["reasoning_efforts"], &efforts); err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{"minimal", "low", "medium", "high", "xhigh"}
	var gotIDs []string
	for _, effort := range efforts {
		gotIDs = append(gotIDs, effort["id"])
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("Pi level map ids = %v, want %v", gotIDs, wantIDs)
	}
}

func TestPiModelDiscoveryDeadlineTerminatesUncooperativeProbe(t *testing.T) {
	killed := make(chan struct{})
	adapter := PiAdapter{Provider: ProviderPi, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		providerIn, clientIn := io.Pipe()
		clientOut, providerOut := io.Pipe()
		stderrOut, stderrIn := io.Pipe()
		wait := make(chan error, 1)
		var once sync.Once
		kill := func() error {
			once.Do(func() {
				_ = providerIn.Close()
				_ = providerOut.Close()
				_ = stderrIn.Close()
				wait <- nil
				close(wait)
				close(killed)
			})
			return nil
		}
		return &StdioProcess{Lines: NewJSONLines(clientOut, clientIn, clientIn), Stderr: stderrOut, wait: wait, kill: kill, stdout: clientOut, errout: stderrOut}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	discovery, err := adapter.Discover(ctx, StartOptions{DiscoverModels: true})
	if err != nil || !discovery.Available || !strings.Contains(discovery.Reason, context.DeadlineExceeded.Error()) {
		t.Fatalf("deadline discovery = %#v, %v", discovery, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("model discovery ignored its deadline")
	}
	select {
	case <-killed:
	case <-time.After(time.Second):
		t.Fatal("uncooperative model probe was not terminated")
	}
}

func TestOhMyPiNegotiatesAndOnlyTerminalEndSettles(t *testing.T) {
	var first string
	adapter := PiAdapter{Provider: ProviderOhMyPi, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeNDJSONProcess(t, func(lines *JSONLines) {
			for {
				var request map[string]json.RawMessage
				if lines.Decode(&request) != nil {
					return
				}
				typ, id := rawString(request["type"]), rawString(request["id"])
				if first == "" {
					first = typ
				}
				data := map[string]any{}
				if typ == "get_state" {
					data = map[string]any{"sessionId": "omp-session", "sessionFile": "/safe/omp.jsonl"}
				}
				_ = lines.Write(map[string]any{"type": "response", "id": id, "success": true, "data": data})
				if typ == "prompt" {
					_ = lines.Write(map[string]any{"type": "agent_start"})
					_ = lines.Write(map[string]any{"type": "agent_end", "isTerminal": false})
					_ = lines.Write(map[string]any{"type": "message_update", "assistantMessageEvent": map[string]string{"type": "text_delta", "delta": "after"}})
					_ = lines.Write(map[string]any{"type": "agent_end", "isTerminal": true})
				}
			}
		}), nil
	}}
	recorder := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{Access: AccessFull}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	if first != "negotiate_protocol" {
		t.Fatalf("first request=%q", first)
	}
	if err := session.Prompt(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventTurnFinished)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	finished := 0
	for _, e := range recorder.events {
		if e.Kind == EventTurnFinished {
			finished++
		}
	}
	if finished != 1 {
		t.Fatalf("finished=%d", finished)
	}
	_ = session.Close()
}

func TestPiChunkReassemblyRejectsBrokenSequences(t *testing.T) {
	payload := []byte(`{"type":"response","id":"waku-1","success":true,"data":{}}`)
	cut := len(payload) / 2
	frames := []map[string]any{{"type": "rpc_chunk", "chunkId": "c", "index": 0, "count": 2, "byteLength": len(payload), "data": base64.StdEncoding.EncodeToString(payload[:cut])}, {"type": "rpc_chunk", "chunkId": "c", "index": 1, "count": 2, "byteLength": len(payload), "data": base64.StdEncoding.EncodeToString(payload[cut:])}}
	var active *piChunk
	raw1, _ := json.Marshal(frames[0])
	raw2, _ := json.Marshal(frames[1])
	if got, err := acceptPiChunk(&active, raw1); err != nil || got != nil {
		t.Fatalf("first=%s %v", got, err)
	}
	got, err := acceptPiChunk(&active, raw2)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("reassembled=%s %v", got, err)
	}
	active = nil
	broken := frames[1]
	bad, _ := json.Marshal(broken)
	if _, err := acceptPiChunk(&active, bad); err == nil {
		t.Fatal("accepted chunk sequence starting at one")
	}
}

func TestClaudeFixturePermissionsQuestionsAndSteer(t *testing.T) {
	frames := fixtureFrames(t, "claude-session.ndjson")
	written := make(chan map[string]json.RawMessage, 16)
	adapter := StreamingAdapter{Provider: ProviderClaude, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeNDJSONProcess(t, func(lines *JSONLines) {
			for {
				var request map[string]json.RawMessage
				if lines.Decode(&request) != nil {
					return
				}
				written <- request
				if rawString(request["type"]) == "user" {
					for _, frame := range frames {
						_ = lines.Write(frame)
					}
				}
			}
		}), nil
	}}
	recorder := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{Access: AccessAsk}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	recorder.wait(t, EventConnected)
	if err := session.Prompt(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	finished := recorder.wait(t, EventTurnFinished)
	if !finished.Success {
		t.Fatal("Claude turn failed")
	}
	recorder.mu.Lock()
	var kinds []EventKind
	for _, e := range recorder.events {
		if e.Kind != EventConnected {
			kinds = append(kinds, e.Kind)
		}
	}
	recorder.mu.Unlock()
	want := []EventKind{EventTurnStarted, EventReasoningDelta, EventTextDelta, EventActivity, EventUsageUpdated, EventActivity, EventTurnFinished}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("order=%v", kinds)
	}
	_ = session.Close()
	recorder.wait(t, EventProcessExited)
}

func TestClaudeControlRequestsAndRetryableResolution(t *testing.T) {
	responses := make(chan map[string]json.RawMessage, 4)
	adapter := StreamingAdapter{Provider: ProviderClaude, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeNDJSONProcess(t, func(lines *JSONLines) {
			_ = lines.Write(map[string]any{"type": "control_request", "request_id": "permission-1", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "Bash", "input": map[string]string{"command": "pwd"}}})
			_ = lines.Write(map[string]any{"type": "control_request", "request_id": "question-1", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "AskUserQuestion", "input": map[string]any{"questions": []any{map[string]any{"question": "Pick", "header": "Choice", "options": []any{map[string]string{"label": "A", "description": "first"}}, "multiSelect": false}}}}})
			for {
				var frame map[string]json.RawMessage
				if lines.Decode(&frame) != nil {
					return
				}
				responses <- frame
			}
		}), nil
	}}
	r := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{Access: AccessAsk}, r)
	if err != nil {
		t.Fatal(err)
	}
	permission := r.wait(t, EventPermission)
	question := r.wait(t, EventUserInputRequested)
	if err := session.RespondPermission(context.Background(), permission.RequestID, "allow"); err != nil {
		t.Fatal(err)
	}
	if err := session.RespondQuestions(context.Background(), question.RequestID, []UserInputAnswer{{QuestionID: "Pick", Answers: []string{"A"}}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case frame := <-responses:
			if rawString(frame["type"]) != "control_response" {
				t.Fatalf("response=%s", frame["type"])
			}
		case <-time.After(time.Second):
			t.Fatal("missing control response")
		}
	}
	_ = session.Close()
}

func TestAmpFixtureUsesMarkedSteerAndEndTurn(t *testing.T) {
	frames := fixtureFrames(t, "amp-session.ndjson")
	requests := make(chan map[string]json.RawMessage, 4)
	adapter := StreamingAdapter{Provider: ProviderAmp, startProcess: func(context.Context, ProcessSpec) (*StdioProcess, error) {
		return fakeNDJSONProcess(t, func(lines *JSONLines) {
			_ = lines.Write(frames[0])
			var first map[string]json.RawMessage
			if lines.Decode(&first) != nil {
				return
			}
			requests <- first
			var steer map[string]json.RawMessage
			if lines.Decode(&steer) == nil {
				requests <- steer
			}
			for _, frame := range frames[1:] {
				_ = lines.Write(frame)
			}
		}), nil
	}}
	r := newEventRecorder()
	session, err := adapter.Start(context.Background(), StartOptions{Access: AccessFull}, r)
	if err != nil {
		t.Fatal(err)
	}
	r.wait(t, EventConnected)
	if err := session.Prompt(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if err := session.Steer(context.Background(), "also test"); err != nil {
		t.Fatal(err)
	}
	finished := r.wait(t, EventTurnFinished)
	if !finished.Success {
		t.Fatal("Amp turn failed")
	}
	first := <-requests
	if _, ok := first["steer"]; ok {
		t.Fatal("prompt was marked as steer")
	}
	second := <-requests
	var marked bool
	_ = decodeUseNumber(second["steer"], &marked)
	if !marked {
		t.Fatalf("steer frame=%v", second)
	}
	_ = session.Close()
}

func TestPhase4AdaptersRejectWrongCursorsAndUnsupportedModes(t *testing.T) {
	for _, tc := range []struct {
		adapter Adapter
		options StartOptions
	}{{PiAdapter{Provider: ProviderPi}, StartOptions{Access: AccessAsk}}, {StreamingAdapter{Provider: ProviderAmp}, StartOptions{Access: AccessAsk}}, {StreamingAdapter{Provider: ProviderClaude}, StartOptions{ResumeCursor: json.RawMessage(`{"provider":"amp","threadId":"x"}`)}}} {
		if _, err := tc.adapter.Start(context.Background(), tc.options, SinkFunc(func(Event) {})); err == nil {
			t.Fatalf("%T accepted invalid options", tc.adapter)
		}
	}
}
