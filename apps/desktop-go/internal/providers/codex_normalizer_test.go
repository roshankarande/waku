package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCodexReplayPreservesOrderSemanticsAndIntegers(t *testing.T) {
	fixture := filepath.Join("..", "..", "testdata", "providers", "codex-session.ndjson")
	file, err := os.Open(fixture)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	normalizer := NewCodexNormalizer()
	var events []Event
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	for scanner.Scan() {
		got, err := normalizer.Consume(scanner.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, got...)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	wantKinds := []EventKind{EventConnected, EventTurnStarted, EventActivity, EventTextDelta, EventTextDelta, EventReasoningDelta, EventReasoningDelta, EventReasoningDelta, EventPermission, EventUserInputRequested, EventUsageUpdated, EventGoalUpdated, EventActivity, EventTurnFinished}
	gotKinds := make([]EventKind, len(events))
	for i := range events {
		gotKinds[i] = events[i].Kind
	}
	if !reflect.DeepEqual(gotKinds, wantKinds) {
		t.Fatalf("event order\n got: %v\nwant: %v", gotKinds, wantKinds)
	}
	if events[3].Text+events[4].Text != "Result[1](https://example.test/waku) done" {
		t.Fatalf("citation rewrite: %q + %q", events[3].Text, events[4].Text)
	}
	if events[7].Text != "Second" || events[6].Text != "\n\n" {
		t.Fatalf("reasoning boundary was not preserved: %#v %#v", events[6], events[7])
	}
	if string(events[8].RequestID) != "9007199254740993" {
		t.Fatalf("opaque numeric request id rounded: %s", events[8].RequestID)
	}
	if *events[10].ContextTokens != 9007199254740993 || *events[10].ContextWindow != ^uint64(0) {
		t.Fatalf("64-bit usage lost: %#v", events[10])
	}
	if !bytes.Contains(events[11].Goal, []byte("9223372036854775807")) || !bytes.Contains(events[12].Activity.Arguments, []byte("18446744073709551615")) {
		t.Fatal("opaque native JSON lost an integer token")
	}
}

func TestCodexDropsUnknownAndPrivateMarkers(t *testing.T) {
	n := NewCodexNormalizer()
	events, err := n.Consume([]byte(`{"method":"item/agentMessage/delta","params":{"delta":"before\ue200cite\ue202missing\ue201 after \ue123secret"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Text != "before after secret" {
		t.Fatalf("private marker leaked: %#v", events)
	}
}

func TestCodexRejectsMalformedLifecycleMessages(t *testing.T) {
	for _, raw := range []string{
		`{"method":"turn/started","params":{"turn":{}}}`,
		`{"method":"turn/started","params":[]}`,
		`{"method":"item/agentMessage/delta","params":{}}`,
		`{"method":"item/started","params":{}}`,
		`{"method":"turn/completed","params":{"turn":{}}}`,
		`{"method":"turn/completed","params":{"turn":{"status":"inProgress"}}}`,
		`{"id":1,"result":{"thread":{}}}`,
	} {
		if _, err := NewCodexNormalizer().Consume([]byte(raw)); err == nil {
			t.Fatalf("accepted malformed message %s", raw)
		}
	}
}

func TestCodexRejectsInvalidJSONRPCEnvelopes(t *testing.T) {
	for _, raw := range []string{
		`null`,
		`{}`,
		`{"jsonrpc":null,"method":"turn/started","params":{"turn":{"id":"turn"}}}`,
		`{"jsonrpc":"1.0","method":"turn/started","params":{"turn":{"id":"turn"}}}`,
		`{"method":""}`,
		`{"id":null,"result":{}}`,
		`{"id":1}`,
		`{"id":1,"result":{},"error":{"code":-1,"message":"failed"}}`,
		`{"id":1,"error":null}`,
		`{"id":1,"error":"failed"}`,
		`{"id":1,"error":{"message":"failed"}}`,
		`{"id":1,"error":{"code":-1.5,"message":"failed"}}`,
		`{"id":1,"params":{},"result":{}}`,
	} {
		if _, err := NewCodexNormalizer().Consume([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid JSON-RPC envelope %s", raw)
		}
	}
	if events, err := NewCodexNormalizer().Consume([]byte(`{"id":2,"result":null}`)); err != nil || len(events) != 0 {
		t.Fatalf("valid null result = %#v, %v", events, err)
	}
	if events, err := NewCodexNormalizer().Consume([]byte(`{"id":2,"error":{"code":-32000,"message":"failed"}}`)); err != nil || len(events) != 1 || events[0].Kind != EventError || events[0].Text != "failed" {
		t.Fatalf("valid structured error = %#v, %v", events, err)
	}
}

func TestCodexMCPStartupStatusValidation(t *testing.T) {
	n := NewCodexNormalizer()
	for _, status := range []string{"starting", "ready", "cancelled"} {
		raw := []byte(`{"method":"mcpServer/startupStatus/updated","params":{"name":"server","status":"` + status + `"}}`)
		if events, err := n.Consume(raw); err != nil || len(events) != 0 {
			t.Fatalf("status %q = %#v, %v", status, events, err)
		}
	}
	events, err := n.Consume([]byte(`{"method":"mcpServer/startupStatus/updated","params":{"name":"server","status":"failed","error":"offline"}}`))
	if err != nil || len(events) != 1 || events[0].Kind != EventError || events[0].Text != "server: offline" {
		t.Fatalf("failed status = %#v, %v", events, err)
	}
	for _, raw := range []string{
		`{"method":"mcpServer/startupStatus/updated","params":{"name":"server","status":"unknown"}}`,
		`{"method":"mcpServer/startupStatus/updated","params":{"name":"server","status":null}}`,
		`{"method":"mcpServer/startupStatus/updated","params":{"name":"server","status":"failed","error":null}}`,
	} {
		if _, err := n.Consume([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid MCP status %s", raw)
		}
	}
}

func TestCodexApprovalMethodMustMatchExactlyAndKeepsStringID(t *testing.T) {
	n := NewCodexNormalizer()
	events, err := n.Consume([]byte(`{"id":"9007199254740993","method":"item/commandExecution/requestApproval","params":{"command":"go test"}}`))
	if err != nil || len(events) != 1 || string(events[0].RequestID) != `"9007199254740993"` {
		t.Fatalf("approval = %#v, %v", events, err)
	}
	events, err = n.Consume([]byte(`{"id":2,"method":"evil/requestApproval/suffix","params":{}}`))
	if err != nil || len(events) != 0 {
		t.Fatalf("substring approval accepted: %#v, %v", events, err)
	}
}

func TestJSONLinesUsesNumbersAndWritesOneFrame(t *testing.T) {
	input := strings.NewReader(`{"id":18446744073709551615,"nested":{"value":9007199254740993}}`)
	var output bytes.Buffer
	lines := NewJSONLines(input, &output, nil)
	var value map[string]any
	if err := lines.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if value["id"].(json.Number).String() != "18446744073709551615" {
		t.Fatalf("number changed: %#v", value["id"])
	}
	if err := lines.Write(map[string]json.RawMessage{"id": json.RawMessage(`9007199254740993`)}); err != nil {
		t.Fatal(err)
	}
	if output.String() != "{\"id\":9007199254740993}\n" {
		t.Fatalf("frame = %q", output.String())
	}
}

func TestDescriptorsGateKnownProviderRestrictions(t *testing.T) {
	d := Descriptors()
	if !d[ProviderCodex].Implemented || !d[ProviderCodex].Capabilities.Goals {
		t.Fatal("implemented Codex controls missing")
	}
	if d[ProviderCodex].Capabilities.Models || d[ProviderCodex].Capabilities.Attachments || d[ProviderCodex].Capabilities.BackgroundWork || d[ProviderCodex].Capabilities.ComputerUse {
		t.Fatal("unimplemented Codex features advertised")
	}
	implemented := map[Provider]bool{
		ProviderCodex: true, ProviderCursor: true, ProviderFx: true, ProviderGrok: true, ProviderKimi: true,
		ProviderPi: true, ProviderOhMyPi: true, ProviderClaude: true, ProviderAmp: true, ProviderOpenCode: true, ProviderDeepSeek: true,
	}
	for provider, descriptor := range d {
		if !implemented[provider] && (descriptor.Implemented || !reflect.DeepEqual(descriptor.Capabilities, Capabilities{})) {
			t.Fatalf("unimplemented %s adapter advertised capabilities", provider)
		}
	}
	if d[ProviderFx].Capabilities.Steering || d[ProviderFx].Capabilities.Questions || d[ProviderFx].Capabilities.Fork || d[ProviderFx].Capabilities.Rewind {
		t.Fatal("Fx restrictions were overclaimed")
	}
	if d[ProviderKimi].Capabilities.Fork || d[ProviderKimi].Capabilities.Rewind || d[ProviderKimi].Capabilities.Questions {
		t.Fatal("Kimi restrictions were overclaimed")
	}
	if !d[ProviderPi].Capabilities.Fork || !d[ProviderPi].Capabilities.Rewind || d[ProviderPi].Capabilities.Approvals || !reflect.DeepEqual(d[ProviderPi].Capabilities.AccessModes, []AccessMode{AccessFull}) {
		t.Fatal("Pi capability gates drifted")
	}
	if d[ProviderOhMyPi].Capabilities.Fork || !d[ProviderOhMyPi].Capabilities.Rewind || d[ProviderOhMyPi].Capabilities.ImportedHistory {
		t.Fatal("Oh My Pi incomplete clone/history paths were advertised")
	}
	if d[ProviderClaude].Capabilities.Fork || d[ProviderClaude].Capabilities.Rewind || !d[ProviderClaude].Capabilities.Questions || !d[ProviderClaude].Capabilities.Approvals {
		t.Fatal("Claude capability gates drifted")
	}
	if d[ProviderClaude].Capabilities.ImportedHistory {
		t.Fatal("Claude native history import was advertised without a catalog adapter")
	}
	if d[ProviderAmp].Capabilities.Fork || d[ProviderAmp].Capabilities.Rewind || d[ProviderAmp].Capabilities.Approvals || d[ProviderAmp].Capabilities.Cancellation != CancellationDrop {
		t.Fatal("Amp capability gates drifted")
	}
	if d[ProviderAmp].Capabilities.ImportedHistory {
		t.Fatal("Amp native history import was advertised without a catalog adapter")
	}
	for _, provider := range []Provider{ProviderCodex, ProviderCursor, ProviderFx, ProviderGrok, ProviderKimi, ProviderClaude, ProviderAmp} {
		if d[provider].Capabilities.Models {
			t.Fatalf("%s advertised model discovery without a Discover implementation", provider)
		}
	}
}

func TestOptionalSessionCatalogContractKeepsWireShapesRaw(t *testing.T) {
	type catalog struct{}
	var _ SessionCatalogAdapter = (*fakeSessionCatalog)(nil)
	_ = catalog{}
}

type fakeSessionCatalog struct{}

func (*fakeSessionCatalog) ListSessions(context.Context, uint64) ([]json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (*fakeSessionCatalog) LoadSession(context.Context, json.RawMessage, string) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (*fakeSessionCatalog) ForkSession(context.Context, json.RawMessage) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
