package validation

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// Sample contains cumulative counters over an explicit window. Aggregation
// uses deltas between consecutive windows and rejects attribution drift.
type Sample struct {
	Version         int    `json:"version"`
	RunID           string `json:"runId"`
	Build           string `json:"build"`
	WorkloadID      string `json:"workloadId"`
	FixtureSHA256   string `json:"fixtureSha256"`
	Iteration       int    `json:"iteration"`
	Warmup          bool   `json:"warmup"`
	WindowStartNS   int64  `json:"windowStartNs"`
	WindowEndNS     int64  `json:"windowEndNs"`
	CPUTimeNS       uint64 `json:"cpuTimeNs"`
	IdleWakeups     uint64 `json:"idleWakeups"`
	RSSBytes        uint64 `json:"rssBytes"`
	HeapBytes       uint64 `json:"heapBytes"`
	AllocationBytes uint64 `json:"allocationBytes"`
	Allocations     uint64 `json:"allocations"`
}

func DecodeSamples(r io.Reader) ([]Sample, error) {
	s := bufio.NewScanner(r)
	var out []Sample
	for line := 1; s.Scan(); line++ {
		var v Sample
		if err := json.Unmarshal(s.Bytes(), &v); err != nil {
			return nil, fmt.Errorf("sample line %d: %w", line, err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(s.Bytes(), &fields); err != nil {
			return nil, fmt.Errorf("sample line %d: %w", line, err)
		}
		for _, name := range []string{"cpuTimeNs", "idleWakeups", "rssBytes", "heapBytes", "allocationBytes", "allocations"} {
			if _, ok := fields[name]; !ok {
				return nil, fmt.Errorf("sample line %d: missing metric field %s", line, name)
			}
		}
		if v.Version != SchemaVersion || v.RunID == "" || v.Build == "" || v.WorkloadID == "" || len(v.FixtureSHA256) != 64 || v.Iteration < 0 || v.WindowStartNS < 0 || v.WindowEndNS <= v.WindowStartNS {
			return nil, fmt.Errorf("sample line %d: invalid attribution or window", line)
		}
		out = append(out, v)
	}
	return out, s.Err()
}

func AddSamples(metrics *Metrics, samples []Sample, build, workload, digest string) (map[int]bool, error) {
	groups := map[string][]Sample{}
	iterations := map[int]bool{}
	for _, s := range samples {
		if s.Build != build || s.WorkloadID != workload || s.FixtureSHA256 != digest {
			return nil, fmt.Errorf("sample attribution %s/%s does not match %s/%s", s.Build, s.WorkloadID, build, workload)
		}
		key := fmt.Sprintf("%s/%d/%t", s.RunID, s.Iteration, s.Warmup)
		groups[key] = append(groups[key], s)
		if !s.Warmup {
			iterations[s.Iteration] = true
		}
	}
	var cpu, wake, rss, heap []float64
	var allocBytes, allocs uint64
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool { return group[i].WindowEndNS < group[j].WindowEndNS })
		for i := 1; i < len(group); i++ {
			if group[i].WindowStartNS == group[i-1].WindowStartNS && group[i].WindowEndNS == group[i-1].WindowEndNS {
				return nil, fmt.Errorf("duplicate sample window in run %s", group[i].RunID)
			}
		}
		for i := 1; i < len(group); i++ {
			prev, cur := group[i-1], group[i]
			if cur.WindowStartNS != prev.WindowEndNS {
				return nil, fmt.Errorf("sample windows are not contiguous in run %s", cur.RunID)
			}
			if cur.CPUTimeNS < prev.CPUTimeNS || cur.IdleWakeups < prev.IdleWakeups || cur.AllocationBytes < prev.AllocationBytes || cur.Allocations < prev.Allocations {
				return nil, fmt.Errorf("cumulative sample counter regressed in run %s", cur.RunID)
			}
			if cur.Warmup {
				continue
			}
			duration := cur.WindowEndNS - prev.WindowEndNS
			seconds := float64(duration) / 1e9
			cpu = append(cpu, float64(cur.CPUTimeNS-prev.CPUTimeNS)/float64(duration)*100)
			wake = append(wake, float64(cur.IdleWakeups-prev.IdleWakeups)/seconds)
			rss = append(rss, float64(cur.RSSBytes))
			heap = append(heap, float64(cur.HeapBytes))
			allocBytes += cur.AllocationBytes - prev.AllocationBytes
			allocs += cur.Allocations - prev.Allocations
		}
	}
	metrics.CPUPercent = Summarize(cpu)
	metrics.IdleWakeupsPerSec = Summarize(wake)
	metrics.RSSBytes = Summarize(rss)
	metrics.HeapBytes = Summarize(heap)
	metrics.AllocationBytes = allocBytes
	metrics.Allocations = allocs
	return iterations, nil
}
