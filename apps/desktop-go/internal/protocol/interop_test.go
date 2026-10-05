package protocol

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRequiredInteropExecutableReportsTypedBlocker(t *testing.T) {
	t.Setenv("WAKU_BUN_BIN", filepath.Join(t.TempDir(), "missing-bun"))
	_, err := requiredExecutable("WAKU_BUN_BIN", "bun")
	var blocked *InteropBlockedError
	if !errors.As(err, &blocked) || blocked.Tool != "bun" {
		t.Fatalf("missing Bun error = %#v, want typed blocker", err)
	}
}

func TestCrossLanguageInteropHarness(t *testing.T) {
	if os.Getenv("WAKU_RUN_CROSS_LANGUAGE") != "1" {
		t.Skip("set WAKU_RUN_CROSS_LANGUAGE=1 to run Rust and TypeScript interoperability checks")
	}
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	results, err := CheckCrossLanguageInterop(ctx, repoRoot)
	for _, result := range results {
		t.Logf("%s:\n%s", result.Name, result.Output)
	}
	if err != nil {
		t.Fatal(err)
	}
}
