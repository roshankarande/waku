package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

func TestCutoverRequiresExternalSupervisorProof(t *testing.T) {
	if err := validateCutover(false); err != nil {
		t.Fatal(err)
	}
	err := validateCutover(true)
	if err == nil || !strings.Contains(err.Error(), "external supervisor proof") {
		t.Fatalf("cutover error=%v", err)
	}
}

func TestReleaseRoleProbeIsExact(t *testing.T) {
	var output bytes.Buffer
	if err := writeReleaseRoleProbe(&output); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "{\"schemaVersion\":1,\"protocol\":\"waku-release-role-v1\",\"role\":\"daemon\"}\n"; got != want {
		t.Fatalf("role probe=%q want=%q", got, want)
	}
}

func TestCutoverCLIRefusesBeforeTouchingData(t *testing.T) {
	t.Setenv(protocol.DaemonTokenEnv, "")
	target := filepath.Join(t.TempDir(), "must-not-exist")
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	flag.CommandLine = flag.NewFlagSet("waku-go-daemon-test", flag.ContinueOnError)
	os.Args = []string{"waku-go-daemon", "--cutover", "--data-dir", target}
	err := run()
	if err == nil || !strings.Contains(err.Error(), "external supervisor proof") {
		t.Fatalf("cutover error=%v", err)
	}
	if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
		t.Fatalf("refused cutover touched data directory: %v", statErr)
	}
}
