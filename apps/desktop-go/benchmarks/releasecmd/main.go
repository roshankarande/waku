package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/roshankarande/waku/apps/desktop-go/benchmarks"
	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
	"os"
)

func main() {
	baseline := flag.String("baseline", "", "Rust baseline executable")
	candidate := flag.String("candidate", "", "Go candidate executable")
	budgetsPath := flag.String("budgets", "", "comparison budgets JSON")
	outputRoot := flag.String("output-root", "", "evidence output root")
	guiBlocker := flag.String("gui-blocker", "", "concrete GUI blocker")
	guiObserver := flag.String("gui-observer", "", "external native GUI observer executable")
	baselineBlocker := flag.String("baseline-blocker", "", "concrete Rust build blocker")
	flag.Parse()
	var budgets validation.Budgets
	if *budgetsPath != "" {
		data, err := os.ReadFile(*budgetsPath)
		if err != nil {
			fatal(err)
		}
		if err := json.Unmarshal(data, &budgets); err != nil {
			fatal(err)
		}
	}
	report, err := benchmarks.RunReleaseBenchmarks(context.Background(), benchmarks.ReleaseRunConfig{BaselineExecutable: *baseline, CandidateExecutable: *candidate, GUIBlocker: *guiBlocker, GUIObserverExecutable: *guiObserver, BaselineBlocker: *baselineBlocker, Budgets: budgets, OutputRoot: *outputRoot})
	if err != nil {
		fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		fatal(err)
	}
	if report.Status == validation.StatusFail {
		os.Exit(1)
	}
	if report.Status != validation.StatusPass {
		os.Exit(3)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "release benchmark:", err); os.Exit(2) }
