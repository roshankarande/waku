package main

import (
	"fmt"
	"testing"

	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
)

func TestValidateCoverageRequiresExactIndependentRuns(t *testing.T) {
	w := validation.Workload{Warmups: 1, Iterations: 5}
	var events []validation.Event
	for i := 0; i < 1; i++ {
		events = append(events, validation.Event{RunID: fmt.Sprintf("warm-%d", i), Iteration: i, Warmup: true})
	}
	for i := 0; i < 5; i++ {
		events = append(events, validation.Event{RunID: fmt.Sprintf("run-%d", i), Iteration: i})
	}
	if err := validateCoverage(events, w); err != nil {
		t.Fatal(err)
	}
	events[len(events)-1].RunID = "run-0"
	if err := validateCoverage(events, w); err == nil {
		t.Fatal("expected reused run rejection")
	}
}

func TestInstrumentationChecksDoNotPassSingleSample(t *testing.T) {
	one := validation.Distribution{Count: 1, P50: 1, P95: 1, P99: 1, Max: 1}
	checks := instrumentationChecks(validation.Metrics{StartupMS: one, HydrationMS: one, InputToPaintMS: one, InputToPresentMS: one, FrameWorkMS: one, CPUPercent: one, IdleWakeupsPerSec: one, RSSBytes: one, ReconnectMS: one, ShutdownMS: one}, 5)
	for _, check := range checks {
		if check.Name == "allocation counters" {
			continue // Presence is enforced while decoding every raw sample.
		}
		if check.Status == validation.StatusPass {
			t.Fatalf("single sample passed %q", check.Name)
		}
	}
}

func TestResultExitCodeReflectsFailedAndBlockedChecks(t *testing.T) {
	if got := resultExitCode([]validation.Check{{Status: validation.StatusPass}}); got != 0 {
		t.Fatalf("pass exit=%d", got)
	}
	if got := resultExitCode([]validation.Check{{Status: validation.StatusBlocked}}); got != 3 {
		t.Fatalf("blocked exit=%d", got)
	}
	if got := resultExitCode([]validation.Check{{Status: validation.StatusBlocked}, {Status: validation.StatusFail}}); got != 1 {
		t.Fatalf("fail exit=%d", got)
	}
}

func TestEvidenceCoverageRejectsSplicingAndMissingMetrics(t *testing.T) {
	w := validation.Workload{Warmups: 1, Iterations: 5}
	required := []string{
		validation.EventProcessStart, validation.EventInteractive,
		validation.EventHydrateStart, validation.EventHydrateReady,
		validation.EventInput, validation.EventPaint, validation.EventPresent,
		validation.EventFrameWork, validation.EventFramePresented,
		validation.EventReconnectStart, validation.EventReconnectReady,
		validation.EventShutdownStart, validation.EventProcessExit,
	}
	var events []validation.Event
	var samples []validation.Sample
	for i := 0; i < w.Warmups+w.Iterations; i++ {
		warmup := i < w.Warmups
		iteration := i
		if warmup {
			iteration = 0
		} else {
			iteration -= w.Warmups
		}
		run := fmt.Sprintf("run-%t-%d", warmup, iteration)
		for _, name := range required {
			events = append(events, validation.Event{RunID: run, Iteration: iteration, Warmup: warmup, Name: name})
		}
		samples = append(samples,
			validation.Sample{RunID: run, Iteration: iteration, Warmup: warmup, WindowStartNS: 0, WindowEndNS: 1},
			validation.Sample{RunID: run, Iteration: iteration, Warmup: warmup, WindowStartNS: 1, WindowEndNS: 2},
		)
	}
	if err := validateEvidenceCoverage(events, samples, w); err != nil {
		t.Fatal(err)
	}

	spliced := append([]validation.Sample(nil), samples...)
	spliced[len(spliced)-1].RunID = "foreign-run"
	if err := validateEvidenceCoverage(events, spliced, w); err == nil {
		t.Fatal("expected sample run splice rejection")
	}

	missing := append([]validation.Event(nil), events...)
	for i, event := range missing {
		if !event.Warmup && event.Iteration == 4 && event.Name == validation.EventFramePresented {
			missing = append(missing[:i], missing[i+1:]...)
			break
		}
	}
	if err := validateEvidenceCoverage(missing, samples, w); err == nil {
		t.Fatal("expected missing per-iteration metric rejection")
	}
	warmMissing := append([]validation.Event(nil), events...)
	for i, event := range warmMissing {
		if event.Warmup && event.Name == validation.EventReconnectReady {
			warmMissing = append(warmMissing[:i], warmMissing[i+1:]...)
			break
		}
	}
	if err := validateEvidenceCoverage(warmMissing, samples, w); err == nil {
		t.Fatal("expected incomplete warmup metric rejection")
	}
}
