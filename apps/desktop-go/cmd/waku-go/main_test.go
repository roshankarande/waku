package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareOwnedUsesPackagedSiblingAndEphemeralSecret(t *testing.T) {
	token, executable, args, err := prepareOwned("", "", "http://desktop", []string{"--log", "warn"}, func(name string) (string, error) {
		if name != "waku-go-daemon" {
			t.Fatalf("name=%q", name)
		}
		return "/bundle/waku-go-daemon", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 64 || executable != "/bundle/waku-go-daemon" {
		t.Fatalf("token/executable=%q %q", token, executable)
	}
	joined := strings.Join(args, " ")
	for _, required := range []string{"--listen 127.0.0.1:0", "--allowed-origin http://desktop", "--allow-client-shutdown", "--log warn"} {
		if !strings.Contains(joined, required) {
			t.Errorf("args %q lack %q", joined, required)
		}
	}
}

func TestReleaseCommandProbesAreExactAndSideEffectFree(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct{ arg, want string }{
		{"--waku-release-role-probe", "{\"schemaVersion\":1,\"protocol\":\"waku-release-role-v1\",\"role\":\"desktop\"}\n"},
		{"--waku-benchmark-identity", "{\"schemaVersion\":1,\"protocol\":\"waku-benchmark-driver-v1\",\"role\":\"desktop\",\"implementation\":\"go\",\"instrumentation\":\"waku-native-v1\"}\n"},
	} {
		t.Run(tc.arg, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			handled, code := commandContract([]string{tc.arg}, &stdout, &stderr)
			if !handled || code != 0 || stdout.String() != tc.want || stderr.Len() != 0 {
				t.Fatalf("handled=%v code=%d stdout=%q stderr=%q", handled, code, stdout.String(), stderr.String())
			}
		})
	}
	entries, err := filepath.Glob(filepath.Join(root, "*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("probe side effects=%v err=%v", entries, err)
	}
}

func TestBenchmarkDriverBlocksHeadlessWithoutArtifacts(t *testing.T) {
	t.Setenv("DISPLAY", "")
	t.Setenv("WAYLAND_DISPLAY", "")
	root := t.TempDir()
	events, samples, environment := filepath.Join(root, "events.ndjson"), filepath.Join(root, "samples.ndjson"), filepath.Join(root, "environment.json")
	args := []string{"--waku-benchmark-driver", "--run-nonce", "nonce-123", "--workload", "long-transcript", "--fixture", filepath.Join(root, "missing.fixture"), "--warmups", "1", "--iterations", "5", "--events", events, "--samples", samples, "--environment", environment}
	var stdout, stderr bytes.Buffer
	handled, code := commandContract(args, &stdout, &stderr)
	if !handled || code != 3 || stdout.Len() != 0 || !strings.Contains(stderr.String(), `"status":"blocked"`) || !strings.Contains(stderr.String(), "no live native display") {
		t.Fatalf("handled=%v code=%d stdout=%q stderr=%q", handled, code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), `"runNonce":"nonce-123"`) {
		t.Fatalf("blocked result is not nonce-bound: %q", stderr.String())
	}
	for _, path := range []string{events, samples, environment} {
		if matches, _ := filepath.Glob(path); len(matches) != 0 {
			t.Fatalf("blocked driver created %s", path)
		}
	}
}
