package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	bench "github.com/roshankarande/waku/apps/desktop-go/benchmarks"
)

type description struct {
	ID               string `json:"id"`
	FixtureSHA256    string `json:"fixtureSha256"`
	Bytes            int    `json:"bytes"`
	TranscriptRows   int    `json:"transcriptRows,omitempty"`
	ReasoningBytes   int    `json:"reasoningBytes,omitempty"`
	DiffLines        int    `json:"diffLines,omitempty"`
	TerminalBytes    int    `json:"terminalBytes,omitempty"`
	StreamBytes      int    `json:"streamBytes,omitempty"`
	StreamIntervalMS int    `json:"streamIntervalMs,omitempty"`
}

var releaseRole = "unconfigured-fixture"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--waku-release-role-probe" {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"schemaVersion": 1, "protocol": "waku-release-role-v1", "role": releaseRole})
		return
	}
	outputDir := flag.String("write", "", "optional directory for generated fixture payloads")
	flag.Parse()
	var descriptions []description
	for _, scenario := range bench.Scenarios {
		fixture := bench.BuildFixture(scenario)
		descriptions = append(descriptions, description{ID: scenario.ID, FixtureSHA256: fixture.SHA256, Bytes: len(fixture.Data), TranscriptRows: scenario.TranscriptRows, ReasoningBytes: scenario.ReasoningBytes, DiffLines: scenario.DiffLines, TerminalBytes: scenario.TerminalBytes, StreamBytes: scenario.StreamBytes, StreamIntervalMS: scenario.StreamIntervalMS})
		if *outputDir != "" {
			if err := os.MkdirAll(*outputDir, 0755); err != nil {
				fatal(err)
			}
			if err := os.WriteFile(filepath.Join(*outputDir, scenario.ID+".fixture"), fixture.Data, 0644); err != nil {
				fatal(err)
			}
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(descriptions); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
