package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
)

func main() {
	eventsPath := flag.String("events", "", "versioned event NDJSON")
	samplesPath := flag.String("samples", "", "resource sample NDJSON")
	environmentPath := flag.String("environment", "", "environment JSON")
	workloadPath := flag.String("workload", "", "workload JSON")
	outputPath := flag.String("out", "", "result JSON (stdout when empty)")
	baselineMode := flag.Bool("baseline-mode", false, "capture baseline evidence without making comparison claims")
	baselinePath := flag.String("baseline", "", "baseline result JSON (required unless -baseline-mode)")
	baselineEventsPath := flag.String("baseline-events", "", "raw baseline event NDJSON (required unless -baseline-mode)")
	baselineSamplesPath := flag.String("baseline-samples", "", "raw baseline sample NDJSON (required unless -baseline-mode)")
	budgetsPath := flag.String("budgets", "", "numeric comparison budgets JSON (required unless -baseline-mode)")
	flag.Parse()
	if *eventsPath == "" || *samplesPath == "" || *environmentPath == "" || *workloadPath == "" {
		fatalf("-events, -samples, -environment and -workload are required")
	}
	var environment validation.Environment
	readJSON(*environmentPath, &environment)
	var workload validation.Workload
	readJSON(*workloadPath, &workload)
	eventData, err := os.ReadFile(*eventsPath)
	if err != nil {
		fatalf("events: %v", err)
	}
	events, err := validation.DecodeEvents(bytes.NewReader(eventData))
	if err != nil {
		fatalf("events: %v", err)
	}
	metrics, eventIterations, err := validation.MetricsFromEvents(events, environment.Build, workload.ID, workload.FixtureSHA256)
	if err != nil {
		fatalf("metrics: %v", err)
	}
	sampleData, err := os.ReadFile(*samplesPath)
	if err != nil {
		fatalf("samples: %v", err)
	}
	samples, err := validation.DecodeSamples(bytes.NewReader(sampleData))
	if err != nil {
		fatalf("samples: %v", err)
	}
	if err := validation.ValidateEvidenceCoverage(events, samples, workload); err != nil {
		fatalf("coverage: %v", err)
	}
	sampleIterations, err := validation.AddSamples(&metrics, samples, environment.Build, workload.ID, workload.FixtureSHA256)
	if err != nil {
		fatalf("samples: %v", err)
	}
	if len(sampleIterations) != workload.Iterations {
		fatalf("samples cover %d recorded iterations, want %d", len(sampleIterations), workload.Iterations)
	}
	if len(eventIterations) != workload.Iterations {
		fatalf("events cover %d recorded iterations, want %d", len(eventIterations), workload.Iterations)
	}
	result := validation.NewResult(environment, workload)
	result.Metrics = metrics
	result.Checks = validation.InstrumentationChecks(metrics, workload.Iterations)
	result.Evidence = validation.RawEvidence{EventsSHA256: digestData(eventData), SamplesSHA256: digestData(sampleData)}
	if err := result.Validate(); err != nil {
		fatalf("result: %v", err)
	}
	if !*baselineMode {
		if *baselinePath == "" || *baselineEventsPath == "" || *baselineSamplesPath == "" || *budgetsPath == "" {
			fatalf("-baseline, -baseline-events, -baseline-samples and -budgets are required unless -baseline-mode is used")
		}
		var baseline validation.Result
		readJSON(*baselinePath, &baseline)
		var budgets validation.Budgets
		readJSON(*budgetsPath, &budgets)
		comparison, err := validation.CompareResultsWithArtifacts(baseline, result, budgets, validation.BaselineArtifacts{EventsPath: *baselineEventsPath, SamplesPath: *baselineSamplesPath})
		if err != nil {
			fatalf("comparison: %v", err)
		}
		result.Checks = append(result.Checks, comparison.Checks...)
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatalf("encode: %v", err)
	}
	data = append(data, '\n')
	if *outputPath == "" {
		_, err = os.Stdout.Write(data)
	} else {
		err = os.WriteFile(*outputPath, data, 0644)
	}
	if err != nil {
		fatalf("write: %v", err)
	}
	if code := resultExitCode(result.Checks); code != 0 {
		os.Exit(code)
	}
}

func digestData(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func instrumentationChecks(m validation.Metrics, iterations int) []validation.Check {
	checks := []validation.Check{}
	add := func(name string, count int, blocker string) {
		status, evidence := validation.StatusPass, fmt.Sprintf("%d recorded samples", count)
		if count < iterations {
			status, evidence = validation.StatusBlocked, ""
			blocker = fmt.Sprintf("%s; got %d samples for %d iterations", blocker, count, iterations)
		}
		checks = append(checks, validation.Check{Name: name, Status: status, Evidence: evidence, Blocker: func() string {
			if status == validation.StatusBlocked {
				return blocker
			}
			return ""
		}()})
	}
	add("startup", m.StartupMS.Count, "application did not emit startup/interactive markers")
	add("hydration", m.HydrationMS.Count, "application did not emit hydration markers")
	add("input to paint", m.InputToPaintMS.Count, "application did not emit input/paint markers")
	add("input to present", m.InputToPresentMS.Count, "platform did not expose compositor-present markers")
	add("frame work", m.FrameWorkMS.Count, "application did not emit frame CPU work")
	add("frame presented", m.FramePresentedMS.Count, "platform did not emit frame presentation intervals")
	add("CPU", m.CPUPercent.Count, "external process sampler was not supplied")
	add("idle wakeups", m.IdleWakeupsPerSec.Count, "platform wakeup sampler was not supplied")
	add("RSS", m.RSSBytes.Count, "external process sampler was not supplied")
	add("heap", m.HeapBytes.Count, "runtime heap sampler was not supplied")
	add("reconnect", m.ReconnectMS.Count, "driver did not exercise reconnect")
	add("shutdown", m.ShutdownMS.Count, "application did not emit shutdown/exit markers")
	checks = append(checks, validation.Check{Name: "allocation counters", Status: validation.StatusPass, Evidence: fmt.Sprintf("recorded in each of %d iterations", iterations)})
	return checks
}

func resultExitCode(checks []validation.Check) int {
	blocked := false
	for _, check := range checks {
		if check.Status == validation.StatusFail {
			return 1
		}
		if check.Status == validation.StatusBlocked || check.Status == validation.StatusUnrun {
			blocked = true
		}
	}
	if blocked {
		return 3
	}
	return 0
}

type phaseIteration struct {
	warmup    bool
	iteration int
}

func validateEvidenceCoverage(events []validation.Event, samples []validation.Sample, w validation.Workload) error {
	eventRuns := map[phaseIteration]string{}
	sampleRuns := map[phaseIteration]string{}
	usedEventRuns := map[string]phaseIteration{}
	usedSampleRuns := map[string]phaseIteration{}
	seen := map[phaseIteration]map[string]bool{}
	windows := map[phaseIteration]int{}
	add := func(dst map[phaseIteration]string, used map[string]phaseIteration, key phaseIteration, run, source string) error {
		if prior, ok := dst[key]; ok && prior != run {
			return fmt.Errorf("%s phase/iteration %+v splices run IDs %q and %q", source, key, prior, run)
		}
		if prior, ok := used[run]; ok && prior != key {
			return fmt.Errorf("%s run ID %q is reused by %+v and %+v", source, run, prior, key)
		}
		dst[key] = run
		used[run] = key
		return nil
	}
	for _, e := range events {
		key := phaseIteration{e.Warmup, e.Iteration}
		if err := add(eventRuns, usedEventRuns, key, e.RunID, "event"); err != nil {
			return err
		}
		if seen[key] == nil {
			seen[key] = map[string]bool{}
		}
		seen[key][e.Name] = true
	}
	for _, s := range samples {
		key := phaseIteration{s.Warmup, s.Iteration}
		if err := add(sampleRuns, usedSampleRuns, key, s.RunID, "sample"); err != nil {
			return err
		}
		windows[key]++
	}
	if len(eventRuns) != w.Warmups+w.Iterations || len(sampleRuns) != len(eventRuns) {
		return fmt.Errorf("event/sample phase coverage mismatch: events=%d samples=%d want=%d", len(eventRuns), len(sampleRuns), w.Warmups+w.Iterations)
	}
	for key, run := range eventRuns {
		if sampleRuns[key] != run {
			return fmt.Errorf("sample run %q does not match event run %q for %+v", sampleRuns[key], run, key)
		}
		if windows[key] < 2 {
			return fmt.Errorf("run %q has %d resource samples; at least two are required", run, windows[key])
		}
	}
	required := []string{validation.EventProcessStart, validation.EventInteractive, validation.EventHydrateStart, validation.EventHydrateReady, validation.EventInput, validation.EventPaint, validation.EventPresent, validation.EventFrameWork, validation.EventFramePresented, validation.EventReconnectStart, validation.EventReconnectReady, validation.EventShutdownStart, validation.EventProcessExit}
	for i := 0; i < w.Iterations; i++ {
		key := phaseIteration{false, i}
		if _, ok := eventRuns[key]; !ok {
			return fmt.Errorf("recorded iteration %d is missing", i)
		}
		for _, name := range required {
			if !seen[key][name] {
				return fmt.Errorf("recorded iteration %d lacks metric event %s", i, name)
			}
		}
	}
	for i := 0; i < w.Warmups; i++ {
		key := phaseIteration{true, i}
		if _, ok := eventRuns[key]; !ok {
			return fmt.Errorf("warmup iteration %d is missing", i)
		}
		for _, name := range required {
			if !seen[key][name] {
				return fmt.Errorf("warmup iteration %d lacks metric event %s", i, name)
			}
		}
	}
	return nil
}

func validateCoverage(events []validation.Event, w validation.Workload) error {
	warm, recorded := map[int]string{}, map[int]string{}
	usedRuns := map[string]string{}
	for _, e := range events {
		phase := "recorded"
		if e.Warmup {
			phase = "warmup"
			if run, ok := warm[e.Iteration]; ok && run != e.RunID {
				return fmt.Errorf("warmup iteration %d has multiple run IDs", e.Iteration)
			}
			warm[e.Iteration] = e.RunID
		} else {
			if run, ok := recorded[e.Iteration]; ok && run != e.RunID {
				return fmt.Errorf("recorded iteration %d has multiple run IDs", e.Iteration)
			}
			recorded[e.Iteration] = e.RunID
		}
		key := fmt.Sprintf("%s/%d", phase, e.Iteration)
		if previous, ok := usedRuns[e.RunID]; ok && previous != key {
			return fmt.Errorf("run ID %q is reused by %s and %s", e.RunID, previous, key)
		}
		usedRuns[e.RunID] = key
	}
	if len(warm) != w.Warmups || len(recorded) != w.Iterations {
		return fmt.Errorf("got %d warmups/%d recorded iterations, want %d/%d", len(warm), len(recorded), w.Warmups, w.Iterations)
	}
	return nil
}

func readJSON(path string, target any) {
	data, err := os.ReadFile(path)
	if err != nil {
		fatalf("%s: %v", path, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		fatalf("%s: %v", path, err)
	}
}
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "validation aggregate: "+format+"\n", args...)
	os.Exit(2)
}
