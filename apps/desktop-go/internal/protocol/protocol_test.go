package protocol

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "protocol", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHelloGolden(t *testing.T) {
	var want any
	if err := Decode(fixture(t, "hello-v7.json"), &want); err != nil {
		t.Fatal(err)
	}
	haveBytes, err := Marshal(NewHello("secret", "00000000-0000-0000-0000-000000000002", []ReplayCursor{{
		SessionID: NilUUID, RuntimeID: "00000000-0000-0000-0000-000000000001",
		Epoch: "00000000-0000-0000-0000-000000000003", Sequence: 9007199254740993,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	var have any
	if err := Decode(haveBytes, &have); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(have, want) {
		t.Fatalf("golden mismatch\nhave %s\nwant %s", haveBytes, fixture(t, "hello-v7.json"))
	}
}

func TestIntegerBoundariesAndOpaqueNumbers(t *testing.T) {
	var event ServerMessage
	if err := Decode(fixture(t, "event-integer-boundaries.json"), &event); err != nil {
		t.Fatal(err)
	}
	if event.Event.Sequence != math.MaxUint64 {
		t.Fatalf("sequence=%d", event.Event.Sequence)
	}
	payload, err := RawObject(event.Event.Event.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"aboveJsSafe": "9007199254740993", "windowId": "18446744073709551615", "signedMin": "-9223372036854775808", "signedMax": "9223372036854775807"} {
		got, ok := payload[key].(json.Number)
		if !ok || got.String() != want {
			t.Fatalf("%s=%T(%v), want json.Number(%s)", key, payload[key], payload[key], want)
		}
	}
}

func TestAbsentNullAndValueAreDistinct(t *testing.T) {
	type sample struct {
		Model Optional[string] `json:"model"`
	}
	var absent, null, value sample
	if err := Decode([]byte(`{}`), &absent); err != nil {
		t.Fatal(err)
	}
	if err := Decode([]byte(`{"model":null}`), &null); err != nil {
		t.Fatal(err)
	}
	if err := Decode([]byte(`{"model":"gpt"}`), &value); err != nil {
		t.Fatal(err)
	}
	if absent.Model.Present || !null.Model.Present || !null.Model.Null || value.Model.Value != "gpt" {
		t.Fatalf("states lost: %#v %#v %#v", absent, null, value)
	}
	encoded := map[string]any{"type": "options"}
	absent.Model.Put(encoded, "absent")
	null.Model.Put(encoded, "null")
	value.Model.Put(encoded, "model")
	b, err := json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"model":"gpt","null":null,"type":"options"}` {
		t.Fatalf("states encoded incorrectly: %s", b)
	}
}

func TestBase64AndUUIDValidation(t *testing.T) {
	b, err := json.Marshal(WriteTerminal([]byte{0, 1, 2, 255}))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"writeTerminal","data":"AAEC/w=="}` {
		t.Fatalf("%s", b)
	}
	var u UUID
	if json.Unmarshal([]byte(`"not-a-uuid"`), &u) == nil {
		t.Fatal("invalid UUID accepted")
	}
}

func TestSizeLimit(t *testing.T) {
	err := DecodeReader(strings.NewReader(strings.Repeat("x", MaxWireMessageBytes+1)), new(any))
	if err == nil {
		t.Fatal("oversized message accepted")
	}
}

func TestRustCompatiblePromptAndBlobFixtures(t *testing.T) {
	turn := UUID("00000000-0000-4000-8000-000000000104")
	command := struct {
		Type      CommandType `json:"type"`
		Prompt    string      `json:"prompt"`
		TurnID    *UUID       `json:"turnId,omitempty"`
		MessageID *UUID       `json:"messageId,omitempty"`
	}{CommandPrompt, "hello", &turn, nil}
	request, err := NewRequest("00000000-0000-4000-8000-000000000101", "00000000-0000-4000-8000-000000000102", "00000000-0000-4000-8000-000000000103", command)
	if err != nil {
		t.Fatal(err)
	}
	have, err := Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var a, b any
	if err := Decode(have, &a); err != nil {
		t.Fatal(err)
	}
	if err := Decode(fixture(t, "request-prompt-v7.json"), &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("request=%s", have)
	}
	var response ServerMessage
	if err := Decode(fixture(t, "response-blob-v7.json"), &response); err != nil {
		t.Fatal(err)
	}
	type blob struct {
		Type  string      `json:"type"`
		Bytes Base64Bytes `json:"bytes"`
	}
	decoded, err := DecodeResponsePayload[blob](response.Outcome.Payload, "blobData")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]byte(decoded.Bytes), []byte{0, 1, 2, 255}) {
		t.Fatalf("bytes=%v", decoded.Bytes)
	}
}

func TestRawCommandPreservesExactInteger(t *testing.T) {
	command, err := NewRawCommand(CommandRunComputerTool, map[string]any{"request": map[string]any{"arguments": map[string]any{"windowId": uint64(math.MaxUint64)}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "18446744073709551615") {
		t.Fatalf("integer lost: %s", b)
	}
}

func TestDecodeClientRequestAndEncodeServerEvent(t *testing.T) {
	var message ClientMessage
	if err := Decode(fixture(t, "request-prompt-v7.json"), &message); err != nil {
		t.Fatal(err)
	}
	if message.Type != MessageRequest || message.Request == nil {
		t.Fatalf("message=%#v", message)
	}
	kind, err := CommandTypeOf(message.Request.Command)
	if err != nil || kind != CommandPrompt {
		t.Fatalf("kind=%q err=%v", kind, err)
	}
	var event ServerMessage
	if err := Decode(fixture(t, "event-integer-boundaries.json"), &event); err != nil {
		t.Fatal(err)
	}
	encoded, err := Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var a, b any
	Decode(encoded, &a)
	Decode(fixture(t, "event-integer-boundaries.json"), &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("event round trip=%s", encoded)
	}
}

func TestAllRustShapedCommandAndResponseVariants(t *testing.T) {
	var commands []json.RawMessage
	if err := Decode(fixture(t, "commands-v7.json"), &commands); err != nil {
		t.Fatal(err)
	}
	seenCommands := make(map[CommandType]bool)
	for i, raw := range commands {
		value, err := DecodeCommand(raw)
		if err != nil {
			t.Fatalf("command[%d] %s: %v", i, raw, err)
		}
		if value == nil {
			t.Fatalf("command[%d] decoded nil", i)
		}
		kind, _ := CommandTypeOf(raw)
		seenCommands[kind] = true
	}
	if len(seenCommands) != len(knownCommandTypes) {
		t.Fatalf("covered %d/%d command variants", len(seenCommands), len(knownCommandTypes))
	}

	var responses []json.RawMessage
	if err := Decode(fixture(t, "responses-v7.json"), &responses); err != nil {
		t.Fatal(err)
	}
	seenResponses := make(map[ResponsePayloadType]bool)
	for i, raw := range responses {
		value, err := DecodeResponsePayloadAny(raw)
		if err != nil {
			t.Fatalf("response[%d] %s: %v", i, raw, err)
		}
		if value == nil {
			t.Fatalf("response[%d] decoded nil", i)
		}
		var envelope ResponsePayloadEnvelope
		if err := Decode(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		seenResponses[ResponsePayloadType(envelope.Type)] = true
	}
	if len(seenResponses) != 25 {
		t.Fatalf("covered %d/25 response variants", len(seenResponses))
	}
	var events []WireDriverEvent
	if err := Decode(fixture(t, "events-v7.json"), &events); err != nil {
		t.Fatal(err)
	}
	seenEvents := make(map[string]bool)
	for i, event := range events {
		value, err := DecodeEventPayloadAny(event)
		if err != nil {
			t.Fatalf("event[%d] %#v: %v", i, event, err)
		}
		if value == nil {
			t.Fatalf("event[%d] decoded nil", i)
		}
		seenEvents[event.Kind] = true
	}
	if len(seenEvents) != 26 {
		t.Fatalf("covered %d/26 event variants", len(seenEvents))
	}
}

func TestStrictVariantValidationAndNumericBoundaries(t *testing.T) {
	for _, raw := range []string{
		`{"type":"futureCommand"}`,
		`{"type":"prompt"}`,
		`{"type":"start","options":{"provider":"codex"}}`,
		`{"type":"runComputerTool","request":{"callId":"c","tool":"click"}}`,
		`{"type":"rollback","turns":-1}`,
		`{"type":"rollback","turns":18446744073709551616}`,
		`{"type":"writeTerminal","data":"not-base64"}`,
		`{"type":"probeProvider","provider":"future","discoverModels":false,"probeVersion":false}`,
	} {
		if _, err := DecodeCommand(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted invalid command %s", raw)
		}
	}
	for _, raw := range []string{
		`{"type":"futurePayload"}`,
		`{"type":"started"}`,
		`{"type":"blobData","bytes":"not-base64"}`,
	} {
		if _, err := DecodeResponsePayloadAny(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted invalid payload %s", raw)
		}
	}
	for _, raw := range []string{
		`{"type":"futureServerMessage"}`,
		`{"type":"response","requestId":"00000000-0000-4000-8000-000000000001"}`,
		`{"type":"response","requestId":"00000000-0000-4000-8000-000000000001","outcome":{"status":"error","error":{}}}`,
		`{"type":"taskStateChanged"}`,
		`{"type":"event","sessionId":"bad","runtimeId":"00000000-0000-4000-8000-000000000002","epoch":"00000000-0000-4000-8000-000000000003","sequence":1,"event":{"kind":"x","payload":null}}`,
		`{"type":"event","sessionId":"00000000-0000-4000-8000-000000000001","runtimeId":"00000000-0000-4000-8000-000000000002","epoch":"00000000-0000-4000-8000-000000000003","sequence":1,"event":{"kind":"futureEvent","payload":null}}`,
	} {
		var message ServerMessage
		if err := Decode([]byte(raw), &message); err == nil {
			t.Fatalf("accepted invalid server message %s", raw)
		}
	}
	for _, event := range []WireDriverEvent{{Kind: "futureEvent", Payload: json.RawMessage(`null`)}, {Kind: "turnStarted", Payload: json.RawMessage(`{}`)}, {Kind: "terminalOutput", Payload: json.RawMessage(`{"data":"bad"}`)}} {
		if _, err := DecodeEventPayloadAny(event); err == nil {
			t.Fatalf("accepted invalid event %#v", event)
		}
	}
}

func TestHelloRejectsEmptyTokenAndPreservesOptionalStates(t *testing.T) {
	if _, err := MarshalHello(NewHello("  ", "00000000-0000-4000-8000-000000000001", nil)); err == nil {
		t.Fatal("empty hello token accepted")
	}
	for _, raw := range []string{`{}`, `{"cursor":null}`, `{"cursor":{"threadId":"x"}}`} {
		type holder struct {
			Cursor Optional[json.RawMessage] `json:"cursor"`
		}
		var got holder
		if err := Decode([]byte(raw), &got); err != nil {
			t.Fatal(err)
		}
		if raw == `{}` && got.Cursor.Present {
			t.Fatal("absent optional became present")
		}
		if raw == `{"cursor":null}` && (!got.Cursor.Present || !got.Cursor.Null) {
			t.Fatal("null optional was lost")
		}
		if strings.Contains(raw, "threadId") && (!got.Cursor.Present || got.Cursor.Null) {
			t.Fatal("optional value was lost")
		}
	}
}

func nullField(t *testing.T, raw json.RawMessage, field string) json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		t.Fatal(err)
	}
	object[field] = json.RawMessage("null")
	out, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func rawArray(t *testing.T, name string) []json.RawMessage {
	t.Helper()
	var values []json.RawMessage
	if err := Decode(fixture(t, name), &values); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestFixtureRequiredFieldsRejectExplicitNull(t *testing.T) {
	hello := json.RawMessage(fixture(t, "hello-v7.json"))
	for _, field := range []string{"type", "protocolVersion", "token", "clientId", "resumeFrom"} {
		var message ClientMessage
		if err := Decode(nullField(t, hello, field), &message); err == nil {
			t.Errorf("hello accepted null %s", field)
		}
	}
	var helloObject map[string]json.RawMessage
	_ = Decode(hello, &helloObject)
	var cursors []json.RawMessage
	_ = Decode(helloObject["resumeFrom"], &cursors)
	for _, field := range []string{"sessionId", "runtimeId", "epoch", "sequence"} {
		cursors[0] = nullField(t, cursors[0], field)
		encoded, _ := json.Marshal(cursors)
		helloObject["resumeFrom"] = encoded
		mutated, _ := json.Marshal(helloObject)
		var message ClientMessage
		if err := Decode(mutated, &message); err == nil {
			t.Errorf("cursor accepted null %s", field)
		}
		_ = Decode(hello, &helloObject)
		_ = Decode(helloObject["resumeFrom"], &cursors)
	}
	commandOptional := map[CommandType]map[string]bool{
		CommandPrompt: {"turnId": true, "messageId": true}, CommandProbeProvider: {"binaryOverride": true}, CommandFetchPlanUsage: {"binaryOverride": true, "cliVersion": true},
	}
	for _, raw := range rawArray(t, "commands-v7.json") {
		kind, _ := CommandTypeOf(raw)
		var object map[string]json.RawMessage
		_ = Decode(raw, &object)
		for field := range object {
			if commandOptional[kind][field] {
				continue
			}
			mutated := nullField(t, raw, field)
			if _, err := DecodeCommand(mutated); err == nil {
				t.Errorf("%s accepted null %s: %s", kind, field, mutated)
			}
		}
	}
	responseOptional := map[ResponsePayloadType]map[string]bool{
		ResponseSessionRuntime: {"runtimeId": true}, ResponseCursor: {"cursor": true}, ResponseProviderProbe: {"version": true}, ResponsePlanUsage: {"usage": true}, ResponseTaskState: {"projectlessRoot": true}, ResponseSession: {"session": true}, ResponseSessionForked: {"checkpointWarning": true}, ResponseSessionRewound: {"cleanupWarning": true},
	}
	for _, raw := range rawArray(t, "responses-v7.json") {
		var envelope ResponsePayloadEnvelope
		_ = Decode(raw, &envelope)
		kind := ResponsePayloadType(envelope.Type)
		var object map[string]json.RawMessage
		_ = Decode(raw, &object)
		for field := range object {
			if responseOptional[kind][field] {
				continue
			}
			mutated := nullField(t, raw, field)
			if _, err := DecodeResponsePayloadAny(mutated); err == nil {
				t.Errorf("%s accepted null %s: %s", kind, field, mutated)
			}
		}
	}
	eventNullable := map[EventKind]bool{EventConnected: true, EventAgentPresetSelected: true, EventAutoTitleUpdated: true, EventTurnStarted: true, EventTurnParked: true, EventGoalUpdated: true, EventProcessExited: true, EventTerminalExited: true}
	for _, raw := range rawArray(t, "events-v7.json") {
		var event WireDriverEvent
		if err := Decode(raw, &event); err != nil {
			t.Fatal(err)
		}
		mutated := event
		mutated.Payload = json.RawMessage("null")
		if !eventNullable[EventKind(event.Kind)] {
			if _, err := DecodeEventPayloadAny(mutated); err == nil {
				t.Errorf("%s accepted null payload", event.Kind)
			}
		}
	}
}

func TestNestedRequiredFieldsRejectNull(t *testing.T) {
	for name, raw := range map[string]json.RawMessage{
		"start provider":  json.RawMessage(`{"type":"start","options":{"provider":null,"binary":"b","cwd":"/","mode":"ask","computerUseEnabled":false}}`),
		"apply mode":      json.RawMessage(`{"type":"applyOptions","options":{"mode":null}}`),
		"computer call":   json.RawMessage(`{"type":"runComputerTool","request":{"callId":null,"tool":"click","arguments":{}}}`),
		"answer id":       json.RawMessage(`{"type":"respondUserInput","requestId":"r","answers":[{"questionId":null,"answers":[]}]}`),
		"skill tuple":     json.RawMessage(`{"type":"loadSkills","projects":[null]}`),
		"goal replace":    json.RawMessage(`{"type":"goal","operation":{"kind":"set","replace":null}}`),
		"attachment data": json.RawMessage(`{"type":"importAttachment","name":"a","upload":{"kind":"file","data_base64":null}}`),
	} {
		if _, err := DecodeCommand(raw); err == nil {
			t.Errorf("%s accepted: %s", name, raw)
		}
	}
	for name, event := range map[string]WireDriverEvent{
		"permission option": {Kind: "permission", Payload: json.RawMessage(`{"requestId":"r","title":"t","detail":"d","options":[{"id":null,"label":"l","allow":true}]}`)},
		"question":          {Kind: "userInputRequested", Payload: json.RawMessage(`{"requestId":"r","questions":[{"id":null,"header":"h","question":"q"}]}`)},
		"reported command":  {Kind: "availableCommands", Payload: json.RawMessage(`[{"name":null}]`)},
		"plan window":       {Kind: "planUsageUpdated", Payload: json.RawMessage(`{"windows":[{"label":null,"percent":1}]}`)},
	} {
		if _, err := DecodeEventPayloadAny(event); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestAttachmentUploadUsesRustSerdeFieldNames(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"type":"importAttachment","name":"a","upload":{"kind":"file","data_base64":"AA=="}}`),
		json.RawMessage(`{"type":"importAttachment","name":"a","upload":{"kind":"directory","entries":[{"relativePath":"a","dataBase64":"AA=="}]}}`),
	} {
		if _, err := DecodeCommand(raw); err != nil {
			t.Errorf("Rust-shaped attachment rejected: %v", err)
		}
	}
	if _, err := DecodeCommand(json.RawMessage(`{"type":"importAttachment","name":"a","upload":{"kind":"file","dataBase64":"AA=="}}`)); err == nil {
		t.Fatal("camelCase file payload accepted despite Rust serde data_base64 field")
	}
}

func TestProviderForkRequestUsesRustSerdeFieldNames(t *testing.T) {
	valid := json.RawMessage(`{"type":"forkProviderSession","request":{"provider":"claude","session_id":"s","resume_at":null,"turn_count":1,"title":"Fork"}}`)
	if _, err := DecodeCommand(valid); err != nil {
		t.Fatalf("Rust-shaped provider fork rejected: %v", err)
	}
	camel := json.RawMessage(`{"type":"forkProviderSession","request":{"provider":"claude","sessionId":"s","resumeAt":null,"turnCount":1,"title":"Fork"}}`)
	if _, err := DecodeCommand(camel); err == nil {
		t.Fatal("camelCase provider fork fields accepted despite Rust enum field names")
	}
}

func TestNestedFixtureRequiredFieldsRejectNull(t *testing.T) {
	responses := make(map[ResponsePayloadType]json.RawMessage)
	for _, raw := range rawArray(t, "responses-v7.json") {
		var envelope ResponsePayloadEnvelope
		if err := Decode(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		responses[ResponsePayloadType(envelope.Type)] = raw
	}
	for kind, test := range map[ResponsePayloadType]struct {
		parent string
		fields []string
	}{
		ResponseProviderProbe:       {"probe", []string{"provider", "installed"}},
		ResponseComputerPermissions: {"permissions", []string{"screenRecording", "accessibility"}},
		ResponseUsageHistory:        {"history", []string{"window", "sinceDay", "untilDay", "totals", "totalTokens", "costUsd", "records", "sessions", "providers", "models", "daily", "months", "projects", "quality", "pricing", "scannedFiles", "skippedFiles", "errors", "scanDuration"}},
		ResponseSkillsCatalog:       {"catalog", []string{"skills"}},
		ResponseAttachmentStored:    {"attachment", []string{"reference", "path", "name", "isDir"}},
		ResponseSessionForked:       {"session", []string{"id", "title", "project_id", "provider", "runtime_mode", "status", "created_at", "updated_at"}},
		ResponseSessionRewound:      {"session", []string{"id", "title", "project_id", "provider", "runtime_mode", "status", "created_at", "updated_at"}},
		ResponseWorkspace:           {"result", []string{"type"}},
	} {
		for _, field := range test.fields {
			mutated := nullNestedField(t, responses[kind], test.parent, field)
			if _, err := DecodeResponsePayloadAny(mutated); err == nil {
				t.Errorf("%s.%s accepted null", kind, field)
			}
		}
	}

	for _, raw := range rawArray(t, "events-v7.json") {
		var event WireDriverEvent
		if err := Decode(raw, &event); err != nil {
			t.Fatal(err)
		}
		required := map[EventKind][]string{
			EventActivity: {"kind", "title", "complete"}, EventRichActivity: {"id", "kind", "title", "complete"},
			EventPermission: {"requestId", "title", "detail", "options"}, EventUserInputRequested: {"requestId", "questions"},
			EventComputerUseUpdated: {"phase", "visible"}, EventPromptSubmitted: {"message", "turnId", "messageId"},
			EventSteerAccepted: {"message"}, EventSteerRejected: {"message", "reason"}, EventPlanUsageUpdated: {"windows"},
			EventTurnFinished: {"success"}, EventTerminalOutput: {"data"},
		}[EventKind(event.Kind)]
		for _, field := range required {
			mutated := event
			mutated.Payload = nullField(t, event.Payload, field)
			if _, err := DecodeEventPayloadAny(mutated); err == nil {
				t.Errorf("%s.%s accepted null", event.Kind, field)
			}
		}
	}
}

func nullNestedField(t *testing.T, raw json.RawMessage, parent, field string) json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		t.Fatal(err)
	}
	object[parent] = nullField(t, object[parent], field)
	out, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAgentSessionNestedRustShapes(t *testing.T) {
	valid := `{"id":"00000000-0000-4000-8000-000000000301","title":"Full","project_id":"00000000-0000-4000-8000-000000000302","workspace":{"kind":"local"},"provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"provider_cursor":{"provider":"codex","threadId":"thread"},"available_commands":["compact",{"name":"review"}],"thread_goal":{"objective":"ship","status":"active"},"context_usage":{"tokens":9007199254740993,"window":18446744073709551615},"runtime_event_cursor":{"runtime_id":"00000000-0000-4000-8000-000000000303","epoch":"00000000-0000-4000-8000-000000000304","sequence":18446744073709551615},"messages":[{"id":"00000000-0000-4000-8000-000000000305","role":"user","content":"hi","attachments":[{"path":"/tmp/a","mention":"a","name":"a","is_dir":false,"is_image":false}],"created_at":1,"streaming":false}],"transcript_blocks":[{"after_message":1,"content":{"kind":"activities","data":[{"id":"00000000-0000-4000-8000-000000000306","kind":"command","title":"run","complete":true}]}}],"turns":[{"id":"00000000-0000-4000-8000-000000000307","turn_count":1,"status":"completed","started_at":1,"checkpoint":{"turn_count":1,"git_ref":"refs/waku/1","status":"ready","files":[{"path":"a","additions":1,"deletions":0}],"created_at":2}}],"queued_messages":[{"id":"00000000-0000-4000-8000-000000000308","content":"next","created_at":3}]}`
	if err := validateAgentSession([]byte(valid)); err != nil {
		t.Fatalf("valid session: %v", err)
	}
	invalid := []string{
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","workspace":null,"provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","workspace":{"kind":"worktree","path":"/tmp"},"provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"messages":{}}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"messages":[{}]}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"transcript_blocks":[{"after_message":0,"content":{"kind":"activities","data":[{}]}}]}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"turns":[{}]}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"queued_messages":[{}]}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"thread_goal":{"objective":"x","status":"future"}}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"context_usage":{"tokens":"1"}}`,
		`{"id":"00000000-0000-4000-8000-000000000301","title":"x","project_id":"00000000-0000-4000-8000-000000000302","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"runtime_event_cursor":{"runtime_id":"bad","epoch":"00000000-0000-4000-8000-000000000304","sequence":1}}`,
	}
	for i, raw := range invalid {
		if err := validateAgentSession([]byte(raw)); err == nil {
			t.Errorf("invalid session %d accepted", i)
		}
	}
}

func TestProviderAndSkillsNestedRustShapes(t *testing.T) {
	probe := `{"provider":"codex","installed":true,"path":"/bin/codex","models":[{"id":"m","name":"Model","reasoning_efforts":[{"id":"high","label":"High"}],"service_tiers":[],"context_windows":[]}],"agent_presets":[{"id":"a","name":"Agent"}]}`
	if err := validateProviderProbe([]byte(probe)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"provider":"codex","installed":true,"models":[{"id":"m"}]}`,
		`{"provider":"codex","installed":true,"models":[{"id":"m","name":"M","reasoning_efforts":[{}]}]}`,
		`{"provider":"codex","installed":true,"agent_presets":[{"name":"A"}]}`,
	} {
		if err := validateProviderProbe([]byte(raw)); err == nil {
			t.Errorf("invalid probe accepted: %s", raw)
		}
	}
	catalog := `{"skills":[{"name":"s","description":"d","scope":"user","project":null,"installs":[{"source":"shared","dir":"/tmp/s","skillFile":"/tmp/s/SKILL.md","enabled":true}],"enabled":true,"allowedTools":null,"body":"body","supportingFiles":0,"totalBytes":4,"modifiedAt":null,"duplicates":0,"rowKey":1}]}`
	if err := validateSkillsCatalog([]byte(catalog)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"skills":[{"name":"s"}]}`,
		`{"skills":[{"name":"s","description":"d","scope":"user","installs":[{}],"enabled":true,"body":"b","supportingFiles":0,"totalBytes":1,"duplicates":0,"rowKey":1}]}`,
		`{"skills":{}}`,
	} {
		if err := validateSkillsCatalog([]byte(raw)); err == nil {
			t.Errorf("invalid catalog accepted: %s", raw)
		}
	}
}

func TestWorkspaceResultRustShapesAndWrongTypes(t *testing.T) {
	valid := []string{
		`{"type":"ack"}`,
		`{"type":"workingTree","entries":[{"relativePath":"a","absolutePath":"/tmp/a","name":"a","isDir":false,"expanded":false,"depth":0}]}`,
		`{"type":"directory","path":"/tmp","parent":null,"home":"/home/u","filesystem_root":"/","entries":[]}`,
		`{"type":"textFile","content":"x"}`,
		`{"type":"projectFiles","entries":[{"path":"a","is_dir":false}]}`,
		`{"type":"slashCommands","commands":[{"name":"x","description":"d","scope":"Project","argument_hint":null,"template":null}]}`,
		`{"type":"projectlessWorkspace","cwd":"/tmp"}`,
		`{"type":"branches","snapshot":null}`,
		`{"type":"branchChanged","snapshot":{"repository":"/tmp","current":null,"detached_head":null,"default_branch":null,"branches":[{"name":"main","checked_out_elsewhere":false}],"additions":0,"deletions":0}}`,
		`{"type":"worktreeCreated","worktree":{"path":"/tmp/w","branch":"b"}}`,
		`{"type":"commitSnapshot","snapshot":{"branch":"main","additions":0,"deletions":0,"staged_additions":0,"staged_deletions":0,"has_staged":false,"has_unstaged":false,"can_push":true}}`,
		`{"type":"commitMessage","message":"m"}`,
		`{"type":"checkpoint","checkpoint":{"turn_count":1,"git_ref":"r","status":"ready","created_at":1}}`,
		`{"type":"bool","value":true}`,
		`{"type":"turnRefs","turn_counts":[1,9007199254740993]}`,
		`{"type":"reviewDiff","data":{"source":"uncommitted","numstat":"","patch":"","completeContext":true}}`,
	}
	for _, raw := range valid {
		if err := validateWorkspaceResult([]byte(raw)); err != nil {
			t.Errorf("valid result rejected: %v: %s", err, raw)
		}
	}
	for _, raw := range []string{
		`{"type":"workingTree","entries":[{}]}`,
		`{"type":"directory","path":1,"home":"h","filesystem_root":"/","entries":[]}`,
		`{"type":"projectFiles","entries":[{"path":"a"}]}`,
		`{"type":"slashCommands","commands":[{"name":"x","description":"d","scope":"future"}]}`,
		`{"type":"branchChanged","snapshot":{"repository":"/tmp","branches":{},"additions":0,"deletions":0}}`,
		`{"type":"worktreeCreated","worktree":{"path":"/tmp"}}`,
		`{"type":"commitSnapshot","snapshot":{"branch":"main"}}`,
		`{"type":"checkpoint","checkpoint":{"turn_count":"1","git_ref":"r","status":"ready","created_at":1}}`,
		`{"type":"bool","value":"true"}`,
		`{"type":"turnRefs","turn_counts":[-1]}`,
		`{"type":"reviewDiff","data":{"source":"future","numstat":"","patch":"","completeContext":true}}`,
	} {
		if err := validateWorkspaceResult([]byte(raw)); err == nil {
			t.Errorf("invalid result accepted: %s", raw)
		}
	}
}

func TestCrossPackagePayloadRustShapes(t *testing.T) {
	for _, raw := range []string{
		`{"type":"taskState","projects":[{"id":"00000000-0000-4000-8000-000000000401","name":"P","path":"/tmp/p","created_at":1}],"sessions":[],"defaultCwd":"/tmp","projectlessRoot":null}`,
		`{"type":"sessionMessageMatches","matches":[{"session_id":"00000000-0000-4000-8000-000000000401","source":"assistant","snippet":"hit"}]}`,
		`{"type":"providerSessions","sessions":[{"cursor":{"provider":"codex","threadId":"t"},"title":"T","cwd":"/tmp","created_at":1,"updated_at":2}]}`,
		`{"type":"settings","settings":{"computer_use_enabled":false,"computer_use_allowed_apps":[{"bundleId":"dev.waku","appName":"Waku"}],"disabled_providers":["claude"],"provider_binary_overrides":{"codex":"/bin/codex"}}}`,
		`{"type":"composerDrafts","drafts":{"sessions":{"00000000-0000-4000-8000-000000000401":{"text":"draft","attachments":[{"path":"/tmp/a","mention":"a","name":"a","is_dir":false,"is_image":false}]}}}}`,
	} {
		if _, err := DecodeResponsePayloadAny([]byte(raw)); err != nil {
			t.Errorf("valid cross-package response rejected: %v: %s", err, raw)
		}
	}
	for _, raw := range []string{
		`{"type":"taskState","projects":[{"id":"bad","name":"P","path":"/tmp"}],"sessions":[],"defaultCwd":"/tmp","projectlessRoot":null}`,
		`{"type":"sessionMessageMatches","matches":[{"session_id":"00000000-0000-4000-8000-000000000401","source":"future","snippet":"hit"}]}`,
		`{"type":"providerSessions","sessions":[{"cursor":{},"title":"T","cwd":"/tmp","created_at":1,"updated_at":2}]}`,
		`{"type":"settings","settings":{"disabled_providers":["future"]}}`,
		`{"type":"composerDrafts","drafts":{"sessions":{"bad":{}}}}`,
	} {
		if _, err := DecodeResponsePayloadAny([]byte(raw)); err == nil {
			t.Errorf("invalid cross-package response accepted: %s", raw)
		}
	}

	usage := `{"window":{"trailingDays":30},"sinceDay":"2026-01-01","untilDay":"2026-01-30","totals":{"uncachedInput":0,"cachedInput":0,"cacheCreation":0,"output":0,"reasoning":0},"totalTokens":1,"costUsd":1,"records":1,"sessions":1,"providers":[{"provider":"codex","costUsd":1,"totalTokens":1,"costShare":1,"tokenShare":1}],"models":[{"provider":"codex","model":"m","costUsd":1,"totalTokens":1,"costShare":1}],"daily":[{"day":"2026-01-01","costUsd":1,"totalTokens":1,"byProvider":[{"costUsd":0,"totalTokens":0},{"costUsd":1,"totalTokens":1}]}],"months":[{"firstDay":"2026-01-01","costUsd":1,"totalTokens":1,"byProvider":[{"costUsd":0,"totalTokens":0},{"costUsd":1,"totalTokens":1}],"sessions":1,"activeDays":1,"topModels":[["m",1]]}],"projects":[{"path":"/tmp","costUsd":1,"totalTokens":1,"byProvider":[{"costUsd":0,"totalTokens":0},{"costUsd":1,"totalTokens":1}],"sessions":1,"costShare":1,"lastDay":"2026-01-01","topModels":[["m",1]]}],"quality":{"providerReportedShare":1,"modelPricedShare":1,"unpricedShare":0,"cacheSavingsUsd":0},"pricing":"fresh","scannedFiles":1,"skippedFiles":0,"errors":[],"scanDuration":{"secs":0,"nanos":1}}`
	if err := validateUsageHistory([]byte(usage)); err != nil {
		t.Fatalf("valid usage history rejected: %v", err)
	}
	if err := validateUsageHistory([]byte(strings.Replace(usage, `"byProvider":[{"costUsd":0,"totalTokens":0},{"costUsd":1,"totalTokens":1}]`, `"byProvider":[]`, 1))); err == nil {
		t.Fatal("invalid usage provider-day tuple accepted")
	}

	for _, event := range []WireDriverEvent{
		{Kind: "richActivity", Payload: json.RawMessage(`{"id":"00000000-0000-4000-8000-000000000402","kind":"fileChange","title":"edit","complete":true,"image_urls":[],"failed":false,"file_changes":[{"path":"a","status":"modified"}],"reasoning":{"content":"why","started_at_ms":1,"finished_at_ms":2}}`)},
	} {
		if _, err := DecodeEventPayloadAny(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := DecodeEventPayloadAny(WireDriverEvent{Kind: "richActivity", Payload: json.RawMessage(`{"id":"00000000-0000-4000-8000-000000000402","kind":"fileChange","title":"edit","complete":true,"file_changes":[{}]}`)}); err == nil {
		t.Fatal("invalid rich activity file change accepted")
	}
}
