// Package benchmarks contains deterministic phase-0 workload construction.
// The synthetic Benchmarks measure harness/fixture overhead only; product
// performance evidence comes from running the same fixtures through Rust and
// Go release builds and ingesting native instrumentation with validation.
package benchmarks

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Scenario struct {
	ID               string
	TranscriptRows   int
	ReasoningBytes   int
	DiffLines        int
	TerminalBytes    int
	StreamBytes      int
	StreamIntervalMS int
}

var Scenarios = []Scenario{
	{ID: "long-transcript", TranscriptRows: 10_000},
	{ID: "fast-reasoning", ReasoningBytes: 2 << 20, StreamBytes: 2 << 20, StreamIntervalMS: 5},
	{ID: "large-diff", DiffLines: 100_000},
	{ID: "terminal-plus-stream", TerminalBytes: 8 << 20, StreamBytes: 2 << 20, StreamIntervalMS: 10},
}

type Fixture struct {
	Scenario Scenario
	Data     []byte
	SHA256   string
}

func BuildFixture(s Scenario) Fixture {
	var data bytes.Buffer
	fmt.Fprintf(&data, "waku-benchmark-v1\nscenario=%s\n", s.ID)
	for i := 0; i < s.TranscriptRows; i++ {
		fmt.Fprintf(&data, "message:%08d:user/assistant/tool ordered content %08x\n", i, i*2654435761)
	}
	writePattern(&data, "reasoning", s.ReasoningBytes)
	for i := 0; i < s.DiffLines; i++ {
		fmt.Fprintf(&data, "%c%08d stable diff fixture token-%08x\n", "+- "[i%3], i, i*2246822519)
	}
	writePattern(&data, "terminal", s.TerminalBytes)
	writePattern(&data, "stream", s.StreamBytes)
	sum := sha256.Sum256(data.Bytes())
	return Fixture{Scenario: s, Data: data.Bytes(), SHA256: hex.EncodeToString(sum[:])}
}

func writePattern(data *bytes.Buffer, label string, size int) {
	if size <= 0 {
		return
	}
	line := []byte(label + ":0123456789abcdef ANSI=\x1b[32mgreen\x1b[0m markdown=`code` $x^2$\n")
	for remaining := size; remaining > 0; {
		part := line
		if len(part) > remaining {
			part = part[:remaining]
		}
		data.Write(part)
		remaining -= len(part)
	}
}
