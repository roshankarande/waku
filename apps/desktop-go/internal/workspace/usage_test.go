package workspace

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

func TestUsageHistoryReadsRustCompatibleClaudeAndCodexTranscripts(t *testing.T) {
	root := t.TempDir()
	claudeRoot, codexRoot := filepath.Join(root, "claude"), filepath.Join(root, "codex")
	if err := os.MkdirAll(claudeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(codexRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	claude := `{"type":"assistant","timestamp":"2026-10-04T12:00:00Z","sessionId":"claude-session","cwd":"/repo/sub","requestId":"request","costUSD":0.5,"message":{"id":"message","model":"claude-sonnet-4","usage":{"input_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":4,"output_tokens":5}}}` + "\n"
	// Claude emits one identical usage object per content block; it counts once.
	if err := os.WriteFile(filepath.Join(claudeRoot, "session.jsonl"), []byte(claude+claude), 0o600); err != nil {
		t.Fatal(err)
	}
	tokenLine := `{"timestamp":"2026-10-04T12:00:02Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":10,"cached_input_tokens":4,"cache_write_input_tokens":1,"output_tokens":3,"reasoning_output_tokens":2}}}}`
	codex := `{"timestamp":"2026-10-04T12:00:00Z","type":"session_meta","payload":{"id":"codex-session","cwd":"/repo"}}` + "\n" +
		`{"timestamp":"2026-10-04T12:00:01Z","type":"turn_context","payload":{"model":"openai/gpt-5","cwd":"/repo/worktree"}}` + "\n" +
		tokenLine + "\n" + tokenLine + "\n"
	if err := os.WriteFile(filepath.Join(codexRoot, "rollout.jsonl"), []byte(codex), 0o600); err != nil {
		t.Fatal(err)
	}
	rates := `{"fetched_at_ms":1791158400000,"rates":{"gpt-5":[0.001,0.002,0.0001,0.003]}}`
	ratePath := filepath.Join(root, "usage-model-rates.json")
	if err := os.WriteFile(ratePath, []byte(rates), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := NewUsageHistoryService(UsageHistoryConfig{ClaudeRoot: claudeRoot, CodexRoot: codexRoot, RatesCachePath: ratePath, Now: func() time.Time { return time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := service.Load(context.Background(), UsageWindow{Kind: "trailingDays", Count: 7}, []string{"/repo"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.DecodeUsageHistoryValue(raw); err != nil {
		t.Fatalf("protocol history: %v\n%s", err, raw)
	}
	var value struct {
		Totals                                       usageTotals `json:"totals"`
		TotalTokens, Records, Sessions, ScannedFiles uint64
		Pricing                                      string
		Quality                                      struct {
			CacheSavingsUSD float64 `json:"cacheSavingsUsd"`
		} `json:"quality"`
		Projects []struct {
			Path     string `json:"path"`
			Sessions uint64 `json:"sessions"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if value.Records != 2 || value.Sessions != 2 || value.ScannedFiles != 2 || value.Pricing != "cached" {
		t.Fatalf("summary=%+v raw=%s", value, raw)
	}
	want := usageTotals{UncachedInput: 7, CachedInput: 7, CacheCreation: 5, Output: 8, Reasoning: 2}
	if value.Totals != want || value.TotalTokens != 27 {
		t.Fatalf("totals=%+v total=%d", value.Totals, value.TotalTokens)
	}
	if value.Quality.CacheSavingsUSD != 0.0036 {
		t.Fatalf("cache savings=%v", value.Quality.CacheSavingsUSD)
	}
	if len(value.Projects) != 1 || value.Projects[0].Path != "/repo" || value.Projects[0].Sessions != 2 {
		t.Fatalf("projects=%+v", value.Projects)
	}
}

func TestUsageNestedTokenAggregatesSaturate(t *testing.T) {
	day := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	a := aggregate{buckets: map[string]*usageBucket{}, sessions: map[string]bool{}, monthSessions: map[string]bool{}, projects: map[string]*projectAggregate{}, seen: map[string]bool{}}
	for _, model := range []string{"one", "two"} {
		cost := 1.0
		a.add(usageRecord{provider: "claude", model: model, session: model, project: "/repo", timestamp: day, totals: usageTotals{UncachedInput: math.MaxUint64}, reportedCost: &cost}, nil, day, day)
	}
	raw, err := json.Marshal(deriveUsageHistory(UsageWindow{Kind: "trailingDays", Count: 1}, day, day, a, "unavailable", 2, 0, []string{}, time.Nanosecond, DefaultUsageHistoryConfig()))
	if err != nil {
		t.Fatal(err)
	}
	value, err := protocol.DecodeUsageHistoryValue(raw)
	if err != nil {
		t.Fatal(err)
	}
	if value.TotalTokens != math.MaxUint64 || value.Providers[0].TotalTokens != math.MaxUint64 || value.Daily[0].TotalTokens != math.MaxUint64 || value.Months[0].TotalTokens != math.MaxUint64 || value.Projects[0].TotalTokens != math.MaxUint64 || value.Daily[0].ByProvider[0].TotalTokens != math.MaxUint64 {
		t.Fatalf("nested aggregates did not saturate: %s", raw)
	}
}

func TestUsageRatesCacheBoundsAndStalenessAreHonest(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "rates.json")
	if err := os.WriteFile(path, []byte(`{"fetched_at_ms":1,"rates":{"gpt-5":[1,1,1,1]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config := DefaultUsageHistoryConfig()
	config.MaxRatesBytes, config.MaxRates, config.RatesTTL = 1024, 10, 24*time.Hour
	rates, status, warning := readUsageRates(path, now, config)
	if len(rates) != 0 || status != "unavailable" || warning == "" {
		t.Fatalf("stale rates=%v status=%q warning=%q", rates, status, warning)
	}
	if err := os.WriteFile(path, []byte(`{"fetched_at_ms":1791158400000,"rates":{"a":[1,1,1,1],"b":[1,1,1,1]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config.MaxRates = 1
	rates, status, warning = readUsageRates(path, now, config)
	if len(rates) != 0 || status != "unavailable" || warning == "" {
		t.Fatalf("entry-bound rates=%v status=%q warning=%q", rates, status, warning)
	}
	config.MaxRates, config.MaxRatesBytes = 10, 8
	rates, status, warning = readUsageRates(path, now, config)
	if len(rates) != 0 || status != "unavailable" || warning == "" {
		t.Fatalf("byte-bound rates=%v status=%q warning=%q", rates, status, warning)
	}
}

func TestUsageHistoryBoundsOversizedFilesAndHonorsCancellation(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "large.jsonl"), make([]byte, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := NewUsageHistoryService(UsageHistoryConfig{ClaudeRoot: root, MaxFileBytes: 8, MaxTotalBytes: 16, Now: func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := service.Load(context.Background(), UsageWindow{Kind: "thisMonth"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var value struct{ ScannedFiles, SkippedFiles uint64 }
	if json.Unmarshal(raw, &value) != nil || value.ScannedFiles != 0 || value.SkippedFiles != 1 {
		t.Fatalf("bounds=%s", raw)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Load(ctx, UsageWindow{Kind: "thisMonth"}, nil); err == nil {
		t.Fatal("canceled scan succeeded")
	}
}
