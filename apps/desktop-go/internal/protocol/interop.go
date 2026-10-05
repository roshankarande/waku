package protocol

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// InteropCommandResult records one cross-language compatibility check. Output
// is retained so release validation can archive exact Rust/TypeScript evidence.
type InteropCommandResult struct {
	Name   string
	Output string
}

// InteropBlockedError distinguishes a missing required cross-language runtime
// from an incompatibility in a fixture. Release acceptance must not turn a
// missing Bun or Rust toolchain into a passing Node-only check.
type InteropBlockedError struct {
	Tool   string
	Reason string
}

func (e *InteropBlockedError) Error() string {
	return fmt.Sprintf("cross-language interoperability blocked: %s: %s", e.Tool, e.Reason)
}

type interopCommand struct {
	name string
	argv []string
	env  []string
}

// CheckCrossLanguageInterop verifies Rust serde/tests, generated TypeScript
// bindings, and the shared protocol-v7 fixture corpus without modifying it.
func CheckCrossLanguageInterop(ctx context.Context, repoRoot string) ([]InteropCommandResult, error) {
	cargoBinary, err := requiredExecutable("WAKU_CARGO_BIN", "cargo")
	if err != nil {
		return nil, err
	}
	rustEnv, err := explicitRustEnvironment(cargoBinary)
	if err != nil {
		return nil, err
	}
	rustFixtureManifest, cleanup, err := rustFixtureChecker(repoRoot)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	fixtureDir := filepath.Join(repoRoot, "apps", "desktop-go", "testdata", "protocol")
	checkedTypescript, cleanupTypescript, err := typescriptFixtureChecker(repoRoot, fixtureDir)
	if err != nil {
		return nil, err
	}
	defer cleanupTypescript()
	cargo := func(args ...string) []string {
		argv := []string{cargoBinary}
		if toolchain := strings.TrimSpace(os.Getenv("WAKU_RUST_TOOLCHAIN")); toolchain != "" {
			argv = append(argv, "+"+toolchain)
		}
		return append(argv, args...)
	}
	commands := []interopCommand{
		{"Rust golden fixture decoding", cargo("run", "--quiet", "--manifest-path", rustFixtureManifest, "--", fixtureDir), rustEnv},
		{"rust protocol tests", cargo("test", "-p", "waku-protocol", "--lib"), rustEnv},
		{"generated TypeScript bindings", cargo("run", "-p", "waku-protocol", "--bin", "export_types", "--", "--check"), rustEnv},
	}
	fixture := filepath.Join("apps", "desktop-go", "testdata", "protocol", "verify-typescript.ts")
	tscArgs := []string{"--ignoreConfig", "--noEmit", "--target", "ESNext", "--module", "Preserve", "--moduleResolution", "bundler", "--allowImportingTsExtensions", "--skipLibCheck", "--types", "node", fixture, checkedTypescript}
	bunBinary, bunErr := requiredExecutable("WAKU_BUN_BIN", "bun")
	var failures []error
	if bunErr == nil {
		commands = append(commands,
			interopCommand{"TypeScript full-fixture type-check (Bun)", append([]string{bunBinary, filepath.Join(repoRoot, "node_modules", "typescript", "bin", "tsc")}, tscArgs...), nil},
			interopCommand{"TypeScript fixture verification (Bun)", []string{bunBinary, "run", fixture}, nil},
		)
	} else {
		failures = append(failures, bunErr)
		// Node is useful diagnostic evidence, but cannot satisfy the Bun gate.
		if node, nodeErr := exec.LookPath("node"); nodeErr == nil {
			commands = append(commands,
				interopCommand{"TypeScript full-fixture type-check (Node supplemental)", append([]string{node, filepath.Join(repoRoot, "node_modules", "typescript", "bin", "tsc")}, tscArgs...), nil},
				interopCommand{"TypeScript fixture runtime (Node supplemental; Bun blocked)", []string{node, "--experimental-strip-types", fixture}, nil},
			)
		}
	}
	results := make([]InteropCommandResult, 0, len(commands))
	for _, check := range commands {
		cmd := exec.CommandContext(ctx, check.argv[0], check.argv[1:]...)
		cmd.Dir = repoRoot
		if check.env != nil {
			cmd.Env = check.env
		}
		output, err := cmd.CombinedOutput()
		results = append(results, InteropCommandResult{Name: check.name, Output: string(output)})
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w\n%s", check.name, err, output))
		}
	}
	return results, errors.Join(failures...)
}

func requiredExecutable(envName, fallback string) (string, error) {
	if configured := strings.TrimSpace(os.Getenv(envName)); configured != "" {
		path, err := exec.LookPath(configured)
		if err != nil || !executableExists(path) {
			return "", &InteropBlockedError{Tool: fallback, Reason: fmt.Sprintf("%s=%q is not executable", envName, configured)}
		}
		return path, nil
	}
	path, err := exec.LookPath(fallback)
	if err != nil || !executableExists(path) {
		return "", &InteropBlockedError{Tool: fallback, Reason: fmt.Sprintf("set %s to an executable", envName)}
	}
	return path, nil
}

func explicitRustEnvironment(cargoBinary string) ([]string, error) {
	cargoHome := strings.TrimSpace(os.Getenv("CARGO_HOME"))
	if cargoHome == "" {
		candidate := filepath.Dir(filepath.Dir(cargoBinary))
		if info, statErr := os.Stat(filepath.Join(candidate, "registry")); statErr == nil && info.IsDir() {
			cargoHome = candidate
		} else {
			home, homeErr := os.UserHomeDir()
			if homeErr != nil {
				return nil, &InteropBlockedError{Tool: "cargo", Reason: "CARGO_HOME is unset and the home directory cannot be determined"}
			}
			cargoHome = filepath.Join(home, ".cargo")
		}
	}
	rustupHome := strings.TrimSpace(os.Getenv("RUSTUP_HOME"))
	if rustupHome == "" {
		candidate := filepath.Join(filepath.Dir(cargoHome), "rustup")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			rustupHome = candidate
		} else {
			home, homeErr := os.UserHomeDir()
			if homeErr != nil {
				return nil, &InteropBlockedError{Tool: "rustup", Reason: "RUSTUP_HOME is unset and the home directory cannot be determined"}
			}
			rustupHome = filepath.Join(home, ".rustup")
		}
	}
	if cargoHome == "" || rustupHome == "" {
		return nil, &InteropBlockedError{Tool: "Rust toolchain", Reason: "CARGO_HOME and RUSTUP_HOME could not be resolved"}
	}
	env := make([]string, 0, len(os.Environ())+2)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "CARGO_HOME=") && !strings.HasPrefix(value, "RUSTUP_HOME=") {
			env = append(env, value)
		}
	}
	return append(env, "CARGO_HOME="+cargoHome, "RUSTUP_HOME="+rustupHome), nil
}

func rustFixtureChecker(repoRoot string) (string, func(), error) {
	directory, err := os.MkdirTemp("", "waku-rust-fixture-check-*")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	if err := os.Mkdir(filepath.Join(directory, "src"), 0o700); err != nil {
		cleanup()
		return "", func() {}, err
	}
	manifest := fmt.Sprintf("[package]\nname = \"waku-fixture-check\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[dependencies]\nbase64 = \"0.22\"\nserde = { version = \"1\", features = [\"derive\"] }\nserde_json = \"1\"\nwaku-protocol = { path = %q }\n", filepath.Join(repoRoot, "crates", "waku-protocol"))
	source := `use std::{env, fs, path::Path};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Deserialize;
use waku_protocol::{event_from_wire, ClientMessage, Command, ResponsePayload, ServerMessage, WireDriverEvent};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalOutput { data: String }

#[derive(Deserialize)]
struct UiSurfaceFixtures {
    commands: Vec<Command>,
    responses: Vec<ResponsePayload>,
    events: Vec<WireDriverEvent>,
}

fn decode_array<T: serde::de::DeserializeOwned>(root: &Path, name: &str) {
    let bytes = fs::read(root.join(name)).unwrap();
    serde_json::from_slice::<Vec<T>>(&bytes).unwrap();
}
fn decode<T: serde::de::DeserializeOwned>(root: &Path, name: &str) {
    let bytes = fs::read(root.join(name)).unwrap();
    serde_json::from_slice::<T>(&bytes).unwrap();
}
fn main() {
    let root = env::args().nth(1).unwrap();
    let root = Path::new(&root);
    decode_array::<Command>(root, "commands-v7.json");
    decode_array::<ResponsePayload>(root, "responses-v7.json");
    let ui_bytes = fs::read(root.join("ui-surface-v7.json")).unwrap();
    let ui = serde_json::from_slice::<UiSurfaceFixtures>(&ui_bytes).unwrap();
    assert!(!ui.commands.is_empty() && !ui.responses.is_empty());
    for event in ui.events {
        match event.kind.as_str() {
            "terminalOutput" => { let output: TerminalOutput = serde_json::from_value(event.payload).unwrap(); STANDARD.decode(output.data).unwrap(); }
            "terminalExited" => assert!(event.payload.is_null()),
            "terminalError" => { serde_json::from_value::<String>(event.payload).unwrap(); }
            _ => { event_from_wire(event).unwrap(); }
        }
    }
    let event_bytes = fs::read(root.join("events-v7.json")).unwrap();
    let events = serde_json::from_slice::<Vec<WireDriverEvent>>(&event_bytes).unwrap();
    let mut terminal_contracts = Vec::new();
    for event in events {
        match event.kind.as_str() {
            "terminalOutput" => {
                assert!(event_from_wire(event.clone()).is_err(), "terminalOutput unexpectedly became a DriverEvent");
                let output: TerminalOutput = serde_json::from_value(event.payload).unwrap();
                STANDARD.decode(output.data).unwrap();
                terminal_contracts.push("terminalOutput");
            }
            "terminalExited" => {
                assert!(event_from_wire(event.clone()).is_err(), "terminalExited unexpectedly became a DriverEvent");
                assert!(event.payload.is_null(), "terminalExited payload must be null");
                terminal_contracts.push("terminalExited");
            }
            "terminalError" => {
                assert!(event_from_wire(event.clone()).is_err(), "terminalError unexpectedly became a DriverEvent");
                serde_json::from_value::<String>(event.payload).unwrap();
                terminal_contracts.push("terminalError");
            }
            _ => { event_from_wire(event).unwrap(); }
        }
    }
    assert_eq!(terminal_contracts, ["terminalOutput", "terminalExited", "terminalError"]);
    decode::<ClientMessage>(root, "hello-v7.json");
    decode::<ClientMessage>(root, "request-prompt-v7.json");
    decode::<ServerMessage>(root, "event-integer-boundaries.json");
    decode::<ServerMessage>(root, "response-blob-v7.json");
    println!("Rust decoded protocol-v7 golden fixtures including typed UI settings/usage/skills/workspace/terminal surfaces; DriverEvent payloads used event_from_wire; terminalOutput/terminalExited/terminalError used explicit terminal contracts");
}`
	manifestPath := filepath.Join(directory, "Cargo.toml")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		cleanup()
		return "", func() {}, err
	}
	if err := os.WriteFile(filepath.Join(directory, "src", "main.rs"), []byte(source), 0o600); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return manifestPath, cleanup, nil
}

func typescriptFixtureChecker(repoRoot, fixtureDir string) (string, func(), error) {
	directory, err := os.MkdirTemp("", "waku-typescript-fixture-check-*")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	read := func(name string) (string, error) {
		value, readErr := os.ReadFile(filepath.Join(fixtureDir, name))
		if readErr != nil {
			return "", readErr
		}
		return strings.TrimSpace(string(value)), nil
	}
	fixtures := make(map[string]string)
	for _, name := range []string{"commands-v7.json", "responses-v7.json", "events-v7.json", "hello-v7.json", "request-prompt-v7.json", "event-integer-boundaries.json", "response-blob-v7.json", "ui-surface-v7.json"} {
		fixtures[name], err = read(name)
		if err != nil {
			cleanup()
			return "", func() {}, err
		}
	}
	generated := filepath.Join(repoRoot, "packages", "waku-client", "src", "generated")
	importType := func(name string) string {
		path, relErr := filepath.Rel(directory, filepath.Join(generated, name+".ts"))
		if relErr != nil {
			path = filepath.Join(generated, name+".ts")
		}
		path = filepath.ToSlash(path)
		if !strings.HasPrefix(path, ".") && !filepath.IsAbs(path) {
			path = "./" + path
		}
		return strconv.Quote(path)
	}
	source := fmt.Sprintf(`import type { ClientMessage } from %s;
import type { Command } from %s;
import type { ResponsePayload } from %s;
import type { ServerMessage } from %s;
import type { ActivityItem } from %s;
import type { ActivityKind } from %s;
import type { PlanUsage } from %s;
import type { ProviderResumeCursor } from %s;
import type { ReportedCommand } from %s;
import type { ThreadGoal } from %s;

type Activity = { id: string | null; kind: ActivityKind; title: string; detail: string | null; complete: boolean };
type BackgroundWork = { type: "outputDelta"; key: { kind: "process" | "monitor" | "subagent"; providerId: string }; delta: string };
type Permission = { requestId: string; title: string; detail: string; options: Array<{ id: string; label: string; allow: boolean }> };
type UserInput = { requestId: string; questions: Array<{ id: string; header: string; question: string; options: Array<{ label: string; description?: string | null }>; multiSelect: boolean }> };
type ComputerUse = { target: null | { kind: string; id: string }; phase: "idle" | "awaitingApproval" | "running" | "completed" | "failed"; visible: boolean; imageUrl: string | null };
// DaemonSettings is intentionally snake_case on the Rust serde wire. The
// generated ComputerAppGrant TS type currently applies ts-rs camelCase, so
// fixture checking spells the actual wire contract explicitly here.
type WireDaemonSettings = {
  computer_use_enabled?: boolean;
  computer_use_allowed_apps?: Array<{ bundleId: string; appName: string }>;
  disabled_providers?: string[];
  provider_binary_overrides?: Record<string, string>;
  [key: string]: unknown;
};
type UISurfaceCommand = Exclude<Command, { type: "updateSettings" }> | { type: "updateSettings"; settings: WireDaemonSettings };
type UISurfaceResponse = Exclude<ResponsePayload, { type: "settings" }> | { type: "settings"; settings: WireDaemonSettings };
type EventFixture =
  | { kind: "connected"; payload: ProviderResumeCursor | null }
  | { kind: "agentPresetSelected"; payload: string | null }
  | { kind: "autoTitleUpdated"; payload: string | null }
  | { kind: "availableCommands"; payload: Array<ReportedCommand> }
  | { kind: "turnStarted" | "turnParked" | "processExited" | "terminalExited"; payload: null }
  | { kind: "textDelta" | "reasoningDelta" | "error" | "terminalError"; payload: string }
  | { kind: "activity"; payload: Activity }
  | { kind: "richActivity"; payload: ActivityItem }
  | { kind: "backgroundWork"; payload: BackgroundWork }
  | { kind: "permission"; payload: Permission }
  | { kind: "userInputRequested"; payload: UserInput }
  | { kind: "computerUseUpdated"; payload: ComputerUse }
  | { kind: "promptSubmitted"; payload: { message: string; turnId: string; messageId: string } }
  | { kind: "steerAccepted"; payload: { message: string } }
  | { kind: "steerRejected"; payload: { message: string; reason: string } }
  | { kind: "usageUpdated"; payload: { contextTokens: number | null; contextWindow: number | null } }
  | { kind: "planUsageUpdated"; payload: PlanUsage }
  | { kind: "goalUpdated"; payload: ThreadGoal | null }
  | { kind: "turnFinished"; payload: { success: boolean; summary: string | null } }
  | { kind: "terminalOutput"; payload: { data: string } };

export const commands = %s as const satisfies readonly Command[];
export const responses = %s as const satisfies readonly ResponsePayload[];
export const events = %s as const satisfies readonly EventFixture[];
export const hello = %s as const satisfies ClientMessage;
export const request = %s as const satisfies ClientMessage;
export const boundary = %s as const satisfies ServerMessage;
export const blob = %s as const satisfies ServerMessage;
export const uiSurface = %s as const satisfies { readonly commands: readonly UISurfaceCommand[]; readonly responses: readonly UISurfaceResponse[]; readonly events: readonly EventFixture[] };
`, importType("ClientMessage"), importType("Command"), importType("ResponsePayload"), importType("ServerMessage"), importType("ActivityItem"), importType("ActivityKind"), importType("PlanUsage"), importType("ProviderResumeCursor"), importType("ReportedCommand"), importType("ThreadGoal"), fixtures["commands-v7.json"], fixtures["responses-v7.json"], fixtures["events-v7.json"], fixtures["hello-v7.json"], fixtures["request-prompt-v7.json"], fixtures["event-integer-boundaries.json"], fixtures["response-blob-v7.json"], fixtures["ui-surface-v7.json"])
	path := filepath.Join(directory, "checked-fixtures.ts")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return path, cleanup, nil
}

func executableExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

// GenerateRustTypeScriptBindings regenerates the checked-in TypeScript types
// from the Rust protocol crate. Callers should follow it with
// CheckCrossLanguageInterop and review the generated diff.
func GenerateRustTypeScriptBindings(ctx context.Context, repoRoot string) (InteropCommandResult, error) {
	cargoBinary, err := requiredExecutable("WAKU_CARGO_BIN", "cargo")
	if err != nil {
		return InteropCommandResult{Name: "generate TypeScript bindings"}, err
	}
	env, err := explicitRustEnvironment(cargoBinary)
	if err != nil {
		return InteropCommandResult{Name: "generate TypeScript bindings"}, err
	}
	argv := []string{"run", "-p", "waku-protocol", "--bin", "export_types"}
	if toolchain := strings.TrimSpace(os.Getenv("WAKU_RUST_TOOLCHAIN")); toolchain != "" {
		argv = append([]string{"+" + toolchain}, argv...)
	}
	cmd := exec.CommandContext(ctx, cargoBinary, argv...)
	cmd.Dir = repoRoot
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	result := InteropCommandResult{Name: "generate TypeScript bindings", Output: string(output)}
	if err != nil {
		return result, fmt.Errorf("generate TypeScript bindings: %w\n%s", err, output)
	}
	return result, nil
}
