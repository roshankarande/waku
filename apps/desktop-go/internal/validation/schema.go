// Package validation defines portable evidence formats for the Go migration.
// It deliberately does not import the desktop, daemon, or MyGo packages so the
// same result files can be produced by the current Rust build and the Go build.
package validation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"time"
)

const SchemaVersion = 1

type Status string

const (
	StatusPass    Status = "pass"
	StatusFail    Status = "fail"
	StatusBlocked Status = "blocked"
	StatusSkipped Status = "skipped"
	StatusUnrun   Status = "unrun"
)

type Environment struct {
	CapturedAt      time.Time `json:"capturedAt"`
	Commit          string    `json:"commit"`
	Build           string    `json:"build"`
	GOOS            string    `json:"goos"`
	GOARCH          string    `json:"goarch"`
	Machine         string    `json:"machine"`
	CPU             string    `json:"cpu"`
	LogicalCPUs     int       `json:"logicalCpus"`
	MemoryBytes     uint64    `json:"memoryBytes"`
	Display         string    `json:"display"`
	RefreshHz       float64   `json:"refreshHz"`
	Scale           float64   `json:"scale"`
	Renderer        string    `json:"renderer"`
	GPUFallback     bool      `json:"gpuFallback"`
	PowerProfile    string    `json:"powerProfile,omitempty"`
	Instrumentation string    `json:"instrumentation"`
}

type Distribution struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// Metrics uses milliseconds, bytes, core-percent (100 == one fully occupied
// logical CPU), and counts. Allocations are totals for the measured window.
type Metrics struct {
	StartupMS         Distribution      `json:"startupMs"`
	HydrationMS       Distribution      `json:"hydrationMs"`
	InputToPaintMS    Distribution      `json:"inputToPaintMs"`
	InputToPresentMS  Distribution      `json:"inputToPresentMs"`
	FrameWorkMS       Distribution      `json:"frameWorkMs"`
	FramePresentedMS  Distribution      `json:"framePresentedMs"`
	CPUPercent        Distribution      `json:"cpuPercent"`
	IdleWakeupsPerSec Distribution      `json:"idleWakeupsPerSec"`
	RSSBytes          Distribution      `json:"rssBytes"`
	HeapBytes         Distribution      `json:"heapBytes"`
	AllocationBytes   uint64            `json:"allocationBytes"`
	Allocations       uint64            `json:"allocations"`
	ReconnectMS       Distribution      `json:"reconnectMs"`
	ShutdownMS        Distribution      `json:"shutdownMs"`
	Frames            uint64            `json:"frames"`
	DroppedFrames     uint64            `json:"droppedFrames"`
	StreamCommits     uint64            `json:"streamCommits"`
	PaneRenders       map[string]uint64 `json:"paneRenders,omitempty"`
}

type Workload struct {
	ID                 string `json:"id"`
	FixtureSHA256      string `json:"fixtureSha256"`
	Warmups            int    `json:"warmups"`
	Iterations         int    `json:"iterations"`
	TranscriptMessages int    `json:"transcriptMessages,omitempty"`
	ReasoningBytes     int64  `json:"reasoningBytes,omitempty"`
	DiffLines          int    `json:"diffLines,omitempty"`
	TerminalBytes      int64  `json:"terminalBytes,omitempty"`
	StreamBytes        int64  `json:"streamBytes,omitempty"`
	StreamIntervalMS   int    `json:"streamIntervalMs,omitempty"`
}

type Check struct {
	Name     string `json:"name"`
	Status   Status `json:"status"`
	Evidence string `json:"evidence,omitempty"`
	Blocker  string `json:"blocker,omitempty"`
}

type Result struct {
	SchemaVersion int         `json:"schemaVersion"`
	Environment   Environment `json:"environment"`
	Workload      Workload    `json:"workload"`
	Metrics       Metrics     `json:"metrics"`
	Checks        []Check     `json:"checks"`
	Evidence      RawEvidence `json:"evidence"`
	Notes         []string    `json:"notes,omitempty"`
}

type RawEvidence struct {
	EventsSHA256  string `json:"eventsSha256"`
	SamplesSHA256 string `json:"samplesSha256"`
}

var KnownWorkloadDigests = map[string]string{
	"long-transcript":      "4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180",
	"fast-reasoning":       "146d22951537176d3c7df7a1431a7904137a324c62c8b1a71c0800c401a64f78",
	"large-diff":           "392bda7fe4f761d2f0848924eb15dcd0f589fd4862a6dc5b7d6ddc71b439bc85",
	"terminal-plus-stream": "1978b7783595578b0faeba25b45b9508c0dc4e4fc337dcec5d48713255165619",
}

func NewResult(env Environment, workload Workload) Result {
	return Result{SchemaVersion: SchemaVersion, Environment: env, Workload: workload}
}

func (r Result) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schemaVersion: got %d, want %d", r.SchemaVersion, SchemaVersion)
	}
	digest, known := KnownWorkloadDigests[r.Workload.ID]
	if !known || r.Workload.FixtureSHA256 != digest || r.Workload.Warmups < 1 || r.Workload.Iterations < 5 {
		return errors.New("workload requires a known scenario/digest, at least one warmup, and at least five recorded iterations")
	}
	if r.Environment.CapturedAt.IsZero() || r.Environment.Commit == "" || r.Environment.Build == "" || r.Environment.GOOS == "" || r.Environment.GOARCH == "" || r.Environment.Machine == "" || r.Environment.CPU == "" || r.Environment.Display == "" || r.Environment.Renderer == "" || r.Environment.Instrumentation == "" {
		return errors.New("environment requires capture time, commit, build, OS/architecture, machine/CPU, display, renderer, and instrumentation")
	}
	if r.Environment.LogicalCPUs <= 0 || r.Environment.MemoryBytes == 0 || r.Environment.RefreshHz <= 0 || r.Environment.Scale <= 0 {
		return errors.New("logicalCpus, memoryBytes, refreshHz and scale must be positive")
	}
	for name, distribution := range map[string]Distribution{
		"startup": r.Metrics.StartupMS, "hydration": r.Metrics.HydrationMS,
		"inputToPaint": r.Metrics.InputToPaintMS, "inputToPresent": r.Metrics.InputToPresentMS,
		"frameWork": r.Metrics.FrameWorkMS, "framePresented": r.Metrics.FramePresentedMS,
		"cpu": r.Metrics.CPUPercent, "idleWakeups": r.Metrics.IdleWakeupsPerSec,
		"rss": r.Metrics.RSSBytes, "heap": r.Metrics.HeapBytes,
		"reconnect": r.Metrics.ReconnectMS, "shutdown": r.Metrics.ShutdownMS,
	} {
		if err := validateDistribution(distribution); err != nil {
			return fmt.Errorf("metrics.%s: %w", name, err)
		}
	}
	for i, check := range r.Checks {
		switch check.Status {
		case StatusPass, StatusFail, StatusBlocked, StatusSkipped, StatusUnrun:
		default:
			return fmt.Errorf("checks[%d]: invalid status %q", i, check.Status)
		}
		if check.Status == StatusBlocked && check.Blocker == "" {
			return fmt.Errorf("checks[%d]: blocked check requires blocker", i)
		}
		if check.Status == StatusPass && check.Evidence == "" {
			return fmt.Errorf("checks[%d]: pass requires evidence", i)
		}
	}
	return nil
}

func validateDistribution(d Distribution) error {
	values := []float64{d.P50, d.P95, d.P99, d.Max}
	if d.Count < 0 {
		return errors.New("count must be non-negative")
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return errors.New("percentiles must be finite and non-negative")
		}
	}
	if d.Count == 0 {
		if d.P50 != 0 || d.P95 != 0 || d.P99 != 0 || d.Max != 0 {
			return errors.New("zero count requires zero percentiles")
		}
		return nil
	}
	if d.P50 > d.P95 || d.P95 > d.P99 || d.P99 > d.Max {
		return errors.New("percentiles must satisfy p50 <= p95 <= p99 <= max")
	}
	return nil
}

type Budgets struct {
	ExpectedBaselineBuild         string  `json:"expectedBaselineBuild"`
	ExpectedCandidateBuild        string  `json:"expectedCandidateBuild"`
	MaxStartupRegressionPercent   float64 `json:"maxStartupRegressionPercent"`
	MaxHydrationRegressionPercent float64 `json:"maxHydrationRegressionPercent"`
	MaxInputRegressionPercent     float64 `json:"maxInputRegressionPercent"`
	MaxFrameP95RegressionPercent  float64 `json:"maxFrameP95RegressionPercent"`
	MaxCPURegressionPercent       float64 `json:"maxCpuRegressionPercent"`
	MaxRSSRegressionPercent       float64 `json:"maxRssRegressionPercent"`
	MaxFrameP95MS                 float64 `json:"maxFrameP95Ms"`
}

type Comparison struct {
	Status Status  `json:"status"`
	Checks []Check `json:"checks"`
}

type BaselineArtifacts struct {
	EventsPath  string
	SamplesPath string
}

func CompareResults(baseline, candidate Result, budgets Budgets) (Comparison, error) {
	return Comparison{}, errors.New("baseline raw artifacts are required; use CompareResultsWithArtifacts")
}

func CompareResultsWithArtifacts(baseline, candidate Result, budgets Budgets, artifacts BaselineArtifacts) (Comparison, error) {
	if err := baseline.Validate(); err != nil {
		return Comparison{}, fmt.Errorf("baseline: %w", err)
	}
	if err := candidate.Validate(); err != nil {
		return Comparison{}, fmt.Errorf("candidate: %w", err)
	}
	gate := baselineEvidenceGate(baseline.Checks, baseline.Evidence)
	if gate.Status != StatusPass {
		return Comparison{Status: gate.Status, Checks: []Check{gate}}, nil
	}
	if err := verifyBaselineArtifacts(baseline, artifacts); err != nil {
		return Comparison{}, fmt.Errorf("baseline artifacts: %w", err)
	}
	if baseline.Workload != candidate.Workload {
		return Comparison{}, errors.New("baseline and candidate workload definitions differ")
	}
	if budgets.MaxFrameP95MS <= 0 || budgets.MaxStartupRegressionPercent < 0 || budgets.MaxHydrationRegressionPercent < 0 || budgets.MaxInputRegressionPercent < 0 || budgets.MaxFrameP95RegressionPercent < 0 || budgets.MaxCPURegressionPercent < 0 || budgets.MaxRSSRegressionPercent < 0 {
		return Comparison{}, errors.New("comparison budgets must be explicit and non-negative; maxFrameP95Ms must be positive")
	}
	for _, item := range []struct {
		name            string
		base, candidate Distribution
	}{{"startup", baseline.Metrics.StartupMS, candidate.Metrics.StartupMS}, {"hydration", baseline.Metrics.HydrationMS, candidate.Metrics.HydrationMS}, {"input-to-present", baseline.Metrics.InputToPresentMS, candidate.Metrics.InputToPresentMS}, {"frame work", baseline.Metrics.FrameWorkMS, candidate.Metrics.FrameWorkMS}, {"CPU", baseline.Metrics.CPUPercent, candidate.Metrics.CPUPercent}, {"RSS", baseline.Metrics.RSSBytes, candidate.Metrics.RSSBytes}} {
		if item.base.Count < baseline.Workload.Iterations || item.candidate.Count < candidate.Workload.Iterations {
			return Comparison{}, fmt.Errorf("%s has fewer samples than recorded iterations", item.name)
		}
	}
	a, b := baseline.Environment, candidate.Environment
	if budgets.ExpectedBaselineBuild == "" || budgets.ExpectedCandidateBuild == "" || budgets.ExpectedBaselineBuild == budgets.ExpectedCandidateBuild {
		return Comparison{}, errors.New("comparison requires distinct expected baseline and candidate build identities")
	}
	if a.Build != budgets.ExpectedBaselineBuild || b.Build != budgets.ExpectedCandidateBuild {
		return Comparison{}, fmt.Errorf("build identities do not match expectations: got %q/%q, want %q/%q", a.Build, b.Build, budgets.ExpectedBaselineBuild, budgets.ExpectedCandidateBuild)
	}
	if a.Instrumentation != b.Instrumentation {
		return Comparison{}, errors.New("baseline and candidate instrumentation revisions differ")
	}
	if a.GOOS != b.GOOS || a.GOARCH != b.GOARCH || a.Machine != b.Machine || a.CPU != b.CPU || a.LogicalCPUs != b.LogicalCPUs || a.MemoryBytes != b.MemoryBytes || a.Display != b.Display || a.RefreshHz != b.RefreshHz || a.Scale != b.Scale || a.Renderer != b.Renderer || a.GPUFallback != b.GPUFallback || a.PowerProfile != b.PowerProfile {
		return Comparison{}, errors.New("baseline and candidate were not captured on the same machine/display/renderer configuration")
	}
	checks := []Check{gate}
	add := func(name string, base, cand, limit float64) {
		c := Check{Name: name}
		if base <= 0 || cand <= 0 {
			c.Status = StatusBlocked
			c.Blocker = "both baseline and candidate require non-zero measured evidence"
		} else {
			regression := (cand/base - 1) * 100
			if regression <= limit {
				c.Status = StatusPass
				c.Evidence = fmt.Sprintf("baseline %.3f, candidate %.3f, regression %.2f%% <= %.2f%%", base, cand, regression, limit)
			} else {
				c.Status = StatusFail
				c.Evidence = fmt.Sprintf("baseline %.3f, candidate %.3f, regression %.2f%% > %.2f%%", base, cand, regression, limit)
			}
		}
		checks = append(checks, c)
	}
	add("startup p95 budget", baseline.Metrics.StartupMS.P95, candidate.Metrics.StartupMS.P95, budgets.MaxStartupRegressionPercent)
	add("hydration p95 budget", baseline.Metrics.HydrationMS.P95, candidate.Metrics.HydrationMS.P95, budgets.MaxHydrationRegressionPercent)
	add("input-to-present p95 budget", baseline.Metrics.InputToPresentMS.P95, candidate.Metrics.InputToPresentMS.P95, budgets.MaxInputRegressionPercent)
	add("frame-work p95 budget", baseline.Metrics.FrameWorkMS.P95, candidate.Metrics.FrameWorkMS.P95, budgets.MaxFrameP95RegressionPercent)
	add("CPU p95 budget", baseline.Metrics.CPUPercent.P95, candidate.Metrics.CPUPercent.P95, budgets.MaxCPURegressionPercent)
	add("RSS p95 budget", baseline.Metrics.RSSBytes.P95, candidate.Metrics.RSSBytes.P95, budgets.MaxRSSRegressionPercent)
	frame := Check{Name: "absolute frame p95 budget"}
	if candidate.Metrics.FrameWorkMS.Count == 0 {
		frame.Status = StatusBlocked
		frame.Blocker = "candidate frame work is missing"
	} else if budgets.MaxFrameP95MS <= 0 {
		frame.Status = StatusBlocked
		frame.Blocker = "numeric frame budget is unset"
	} else if candidate.Metrics.FrameWorkMS.P95 <= budgets.MaxFrameP95MS {
		frame.Status = StatusPass
		frame.Evidence = fmt.Sprintf("%.3f ms <= %.3f ms", candidate.Metrics.FrameWorkMS.P95, budgets.MaxFrameP95MS)
	} else {
		frame.Status = StatusFail
		frame.Evidence = fmt.Sprintf("%.3f ms > %.3f ms", candidate.Metrics.FrameWorkMS.P95, budgets.MaxFrameP95MS)
	}
	checks = append(checks, frame)
	status := StatusPass
	for _, c := range checks {
		if c.Status == StatusFail {
			status = StatusFail
			break
		}
		if c.Status == StatusBlocked {
			status = StatusBlocked
		}
	}
	return Comparison{Status: status, Checks: checks}, nil
}

func verifyBaselineArtifacts(result Result, artifacts BaselineArtifacts) error {
	eventData, err := os.ReadFile(artifacts.EventsPath)
	if err != nil {
		return fmt.Errorf("events: %w", err)
	}
	sampleData, err := os.ReadFile(artifacts.SamplesPath)
	if err != nil {
		return fmt.Errorf("samples: %w", err)
	}
	if digestDataBytes(eventData) != result.Evidence.EventsSHA256 || digestDataBytes(sampleData) != result.Evidence.SamplesSHA256 {
		return errors.New("raw artifact digest does not match baseline result")
	}
	events, err := DecodeEvents(bytes.NewReader(eventData))
	if err != nil {
		return err
	}
	metrics, eventIterations, err := MetricsFromEvents(events, result.Environment.Build, result.Workload.ID, result.Workload.FixtureSHA256)
	if err != nil {
		return err
	}
	samples, err := DecodeSamples(bytes.NewReader(sampleData))
	if err != nil {
		return err
	}
	sampleIterations, err := AddSamples(&metrics, samples, result.Environment.Build, result.Workload.ID, result.Workload.FixtureSHA256)
	if err != nil {
		return err
	}
	if len(eventIterations) != result.Workload.Iterations || len(sampleIterations) != result.Workload.Iterations {
		return errors.New("raw artifacts do not cover the declared recorded iterations")
	}
	if err := ValidateEvidenceCoverage(events, samples, result.Workload); err != nil {
		return err
	}
	if !reflect.DeepEqual(metrics, result.Metrics) {
		return errors.New("recomputed metrics and allocation totals differ from baseline result")
	}
	if !reflect.DeepEqual(InstrumentationChecks(metrics, result.Workload.Iterations), result.Checks) {
		return errors.New("recomputed validation checks differ from baseline result")
	}
	type phaseKey struct {
		warmup    bool
		iteration int
	}
	eventRuns, sampleRuns := map[phaseKey]string{}, map[phaseKey]string{}
	for _, event := range events {
		key := phaseKey{event.Warmup, event.Iteration}
		if prior := eventRuns[key]; prior != "" && prior != event.RunID {
			return errors.New("raw events splice run IDs within an iteration")
		}
		eventRuns[key] = event.RunID
	}
	for _, sample := range samples {
		key := phaseKey{sample.Warmup, sample.Iteration}
		if prior := sampleRuns[key]; prior != "" && prior != sample.RunID {
			return errors.New("raw samples splice run IDs within an iteration")
		}
		sampleRuns[key] = sample.RunID
	}
	if len(eventRuns) != result.Workload.Warmups+result.Workload.Iterations || len(sampleRuns) != len(eventRuns) {
		return errors.New("raw artifacts do not cover declared warmups and iterations")
	}
	for key, run := range eventRuns {
		if sampleRuns[key] != run {
			return errors.New("raw event/sample run provenance differs")
		}
	}
	return nil
}

func digestDataBytes(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

var requiredBaselineChecks = []string{"startup", "hydration", "input to paint", "input to present", "frame work", "frame presented", "CPU", "idle wakeups", "RSS", "heap", "reconnect", "shutdown", "allocation counters"}

func baselineEvidenceGate(checks []Check, evidence RawEvidence) Check {
	gate := Check{Name: "baseline evidence gate"}
	if !validSHA256(evidence.EventsSHA256) || !validSHA256(evidence.SamplesSHA256) {
		gate.Status = StatusBlocked
		gate.Blocker = "baseline requires SHA-256 digests of raw event and sample streams"
		return gate
	}
	if len(checks) == 0 {
		gate.Status = StatusBlocked
		gate.Blocker = "baseline contains no validation checks"
		return gate
	}
	byName := make(map[string]Check, len(checks))
	for _, check := range checks {
		if _, duplicate := byName[check.Name]; duplicate {
			gate.Status = StatusFail
			gate.Evidence = fmt.Sprintf("baseline check %q is duplicated", check.Name)
			return gate
		}
		byName[check.Name] = check
		if check.Status == StatusFail {
			gate.Status = StatusFail
			gate.Evidence = fmt.Sprintf("baseline check %q failed", check.Name)
			return gate
		}
		if check.Status != StatusPass {
			gate.Status = StatusBlocked
			gate.Blocker = fmt.Sprintf("baseline check %q is %s", check.Name, check.Status)
			return gate
		}
	}
	for _, name := range requiredBaselineChecks {
		_, exists := byName[name]
		if !exists {
			gate.Status = StatusBlocked
			gate.Blocker = fmt.Sprintf("baseline check %q is missing", name)
			return gate
		}
	}
	gate.Status = StatusPass
	gate.Evidence = fmt.Sprintf("all %d required baseline checks passed; eventsSha256=%s samplesSha256=%s", len(requiredBaselineChecks), evidence.EventsSHA256, evidence.SamplesSHA256)
	return gate
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func Summarize(samples []float64) Distribution {
	if len(samples) == 0 {
		return Distribution{}
	}
	values := append([]float64(nil), samples...)
	sort.Float64s(values)
	return Distribution{
		Count: len(values), P50: percentile(values, .50), P95: percentile(values, .95),
		P99: percentile(values, .99), Max: values[len(values)-1],
	}
}

// percentile uses nearest-rank. This makes the recorded value an observed
// sample, remains stable for small fixture runs, and is easy to reproduce.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted))*p+.999999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
