package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type uiSurfaceFixture struct {
	Commands  []json.RawMessage `json:"commands"`
	Responses []json.RawMessage `json:"responses"`
	Events    []WireDriverEvent `json:"events"`
}

func loadUISurfaceFixture(t *testing.T) uiSurfaceFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "protocol", "ui-surface-v7.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture uiSurfaceFixture
	if err := Decode(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestUISurfaceGoldenFixtureStrictlyDecodes(t *testing.T) {
	fixture := loadUISurfaceFixture(t)
	workspaceCommands, workspaceResponses := 0, 0
	for i, raw := range fixture.Commands {
		decoded, err := DecodeCommand(raw)
		if err != nil {
			t.Fatalf("command[%d]: %v", i, err)
		}
		if command, ok := decoded.(*WorkspaceCommand); ok {
			workspaceCommands++
			if _, err := DecodeWorkspaceOperationAny(command.Operation); err != nil {
				t.Fatalf("workspace command[%d]: %v", i, err)
			}
		}
	}
	for i, raw := range fixture.Responses {
		decoded, err := DecodeResponsePayloadAny(raw)
		if err != nil {
			t.Fatalf("response[%d]: %v", i, err)
		}
		if payload, ok := decoded.(*SettingsPayload); ok {
			settings, err := DecodeDaemonSettings(payload.Settings)
			if err != nil || !settings.ComputerUseEnabled || string(settings.Extra["future_setting"]) != `{"kept":true}` {
				t.Fatalf("settings=%+v err=%v", settings, err)
			}
		}
		if payload, ok := decoded.(*UsageHistoryPayload); ok {
			history, err := DecodeUsageHistoryValue(payload.History)
			if err != nil || history.TotalTokens != 9007199254740994 || history.ScanDuration.Secs != 9007199254740993 {
				t.Fatalf("history=%+v err=%v", history, err)
			}
		}
		if payload, ok := decoded.(*SkillsCatalogPayload); ok {
			catalog, err := DecodeSkillsCatalogValue(payload.Catalog)
			if err != nil || len(catalog.Skills) != 1 || catalog.Skills[0].RowKey != 9007199254740996 {
				t.Fatalf("catalog=%+v err=%v", catalog, err)
			}
		}
		if payload, ok := decoded.(*WorkspacePayload); ok {
			workspaceResponses++
			if _, err := DecodeWorkspaceResultAny(payload.Result); err != nil {
				t.Fatalf("workspace response[%d]: %v", i, err)
			}
		}
	}
	if workspaceCommands != 26 || workspaceResponses != 16 {
		t.Fatalf("workspace fixtures commands=%d responses=%d", workspaceCommands, workspaceResponses)
	}
	for i, event := range fixture.Events {
		if _, err := DecodeEventPayloadAny(event); err != nil {
			t.Fatalf("event[%d]: %v", i, err)
		}
	}
}

func TestUISurfaceRejectsWrongTypesNullsAndUnknownVariants(t *testing.T) {
	commands := []string{
		`{"type":"workspace","operation":{"type":"listTree","root":"/remote","expanded_paths":[7]}}`,
		`{"type":"workspace","operation":{"type":"createWorktree","project_path":"/remote","project_id":null,"session_id":"22222222-2222-4222-8222-222222222222","prompt":"x","base_branch":null}}`,
		`{"type":"workspace","operation":{"type":"collectReviewDiff","cwd":"/remote","source":{"lastTurn":{"session_id":"22222222-2222-4222-8222-222222222222","turn_id":"bad","turn_count":1}}}}`,
		`{"type":"workspace","operation":{"type":"hostPathOnClient","path":"/remote"}}`,
		`{"type":"loadUsageHistory","window":{"months":-1},"projectRoots":[]}`,
	}
	for _, raw := range commands {
		if _, err := DecodeCommand(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted invalid command %s", raw)
		}
	}
	responses := []string{
		`{"type":"workspace","result":{"type":"checkpoint","checkpoint":{"turn_count":1,"git_ref":"r","status":"ready","files":[{"path":"x","additions":null,"deletions":0}],"created_at":1}}}`,
		`{"type":"workspace","result":{"type":"directory","path":"/r","parent":7,"home":"/h","filesystem_root":"/","entries":[]}}`,
		`{"type":"usageHistory","history":{"window":"thisMonth","sinceDay":"bad","untilDay":"2025-01-01","totals":{"uncachedInput":0,"cachedInput":0,"cacheCreation":0,"output":0,"reasoning":0},"totalTokens":0,"costUsd":0,"records":0,"sessions":0,"providers":[],"models":[],"daily":[],"months":[],"projects":[],"quality":{"providerReportedShare":0,"modelPricedShare":0,"unpricedShare":0,"cacheSavingsUsd":0},"pricing":"fresh","scannedFiles":0,"skippedFiles":0,"errors":[],"scanDuration":{"secs":0,"nanos":0}}}`,
	}
	for _, raw := range responses {
		if _, err := DecodeResponsePayloadAny(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted invalid response %s", raw)
		}
	}
}

func TestDaemonSettingsRoundTripPreservesUnknownFields(t *testing.T) {
	raw := json.RawMessage(`{"computer_use_enabled":false,"computer_use_allowed_apps":[],"disabled_providers":[],"provider_binary_overrides":{},"future":{"remotePath":"/daemon/path"}}`)
	settings, err := DecodeDaemonSettings(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := Decode(encoded, &object); err != nil {
		t.Fatal(err)
	}
	if string(object["future"]) != `{"remotePath":"/daemon/path"}` {
		t.Fatalf("unknown field lost: %s", encoded)
	}
}
