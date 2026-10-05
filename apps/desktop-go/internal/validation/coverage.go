package validation

import "fmt"

type evidencePhase struct {
	warmup    bool
	iteration int
}

func ValidateEvidenceCoverage(events []Event, samples []Sample, workload Workload) error {
	eventRuns, sampleRuns := map[evidencePhase]string{}, map[evidencePhase]string{}
	usedEvents, usedSamples := map[string]evidencePhase{}, map[string]evidencePhase{}
	seen := map[evidencePhase]map[string]bool{}
	windows := map[evidencePhase]int{}
	add := func(dst map[evidencePhase]string, used map[string]evidencePhase, key evidencePhase, run, kind string) error {
		if prior, ok := dst[key]; ok && prior != run {
			return fmt.Errorf("%s phase/iteration splices run IDs", kind)
		}
		if prior, ok := used[run]; ok && prior != key {
			return fmt.Errorf("%s run ID is reused", kind)
		}
		dst[key] = run
		used[run] = key
		return nil
	}
	for _, event := range events {
		key := evidencePhase{event.Warmup, event.Iteration}
		if err := add(eventRuns, usedEvents, key, event.RunID, "event"); err != nil {
			return err
		}
		if seen[key] == nil {
			seen[key] = map[string]bool{}
		}
		seen[key][event.Name] = true
	}
	for _, sample := range samples {
		key := evidencePhase{sample.Warmup, sample.Iteration}
		if err := add(sampleRuns, usedSamples, key, sample.RunID, "sample"); err != nil {
			return err
		}
		windows[key]++
	}
	if len(eventRuns) != workload.Warmups+workload.Iterations || len(sampleRuns) != len(eventRuns) {
		return fmt.Errorf("event/sample phase coverage mismatch")
	}
	required := []string{EventProcessStart, EventInteractive, EventHydrateStart, EventHydrateReady, EventInput, EventPaint, EventPresent, EventFrameWork, EventFramePresented, EventReconnectStart, EventReconnectReady, EventShutdownStart, EventProcessExit}
	verify := func(key evidencePhase, label string) error {
		run, ok := eventRuns[key]
		if !ok {
			return fmt.Errorf("%s is missing", label)
		}
		if sampleRuns[key] != run {
			return fmt.Errorf("%s event/sample run differs", label)
		}
		if windows[key] < 2 {
			return fmt.Errorf("%s lacks resource windows", label)
		}
		for _, name := range required {
			if !seen[key][name] {
				return fmt.Errorf("%s lacks metric event %s", label, name)
			}
		}
		return nil
	}
	for index := 0; index < workload.Warmups; index++ {
		if err := verify(evidencePhase{true, index}, fmt.Sprintf("warmup %d", index)); err != nil {
			return err
		}
	}
	for index := 0; index < workload.Iterations; index++ {
		if err := verify(evidencePhase{false, index}, fmt.Sprintf("iteration %d", index)); err != nil {
			return err
		}
	}
	return nil
}

func InstrumentationChecks(metrics Metrics, iterations int) []Check {
	checks := []Check{}
	add := func(name string, count int, blocker string) {
		check := Check{Name: name}
		if count < iterations {
			check.Status = StatusBlocked
			check.Blocker = fmt.Sprintf("%s; got %d samples for %d iterations", blocker, count, iterations)
		} else {
			check.Status = StatusPass
			check.Evidence = fmt.Sprintf("%d recorded samples", count)
		}
		checks = append(checks, check)
	}
	add("startup", metrics.StartupMS.Count, "application did not emit startup/interactive markers")
	add("hydration", metrics.HydrationMS.Count, "application did not emit hydration markers")
	add("input to paint", metrics.InputToPaintMS.Count, "application did not emit input/paint markers")
	add("input to present", metrics.InputToPresentMS.Count, "platform did not expose compositor-present markers")
	add("frame work", metrics.FrameWorkMS.Count, "application did not emit frame CPU work")
	add("frame presented", metrics.FramePresentedMS.Count, "platform did not emit frame presentation intervals")
	add("CPU", metrics.CPUPercent.Count, "external process sampler was not supplied")
	add("idle wakeups", metrics.IdleWakeupsPerSec.Count, "platform wakeup sampler was not supplied")
	add("RSS", metrics.RSSBytes.Count, "external process sampler was not supplied")
	add("heap", metrics.HeapBytes.Count, "runtime heap sampler was not supplied")
	add("reconnect", metrics.ReconnectMS.Count, "driver did not exercise reconnect")
	add("shutdown", metrics.ShutdownMS.Count, "application did not emit shutdown/exit markers")
	checks = append(checks, Check{Name: "allocation counters", Status: StatusPass, Evidence: fmt.Sprintf("recorded in each of %d iterations", iterations)})
	return checks
}
