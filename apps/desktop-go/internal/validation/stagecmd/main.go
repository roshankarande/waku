package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
)

func main() {
	specPath := flag.String("spec", "", "release staging spec JSON")
	stage := flag.String("stage", "", "new staging directory")
	archive := flag.String("archive", "", "new output archive")
	reportPath := flag.String("report", "", "output report JSON")
	flag.Parse()
	if *specPath == "" || *stage == "" || *archive == "" || *reportPath == "" {
		fmt.Fprintln(os.Stderr, "usage: stagecmd -spec SPEC -stage DIR -archive FILE -report FILE")
		os.Exit(2)
	}
	data, err := os.ReadFile(*specPath)
	if err != nil {
		fatal(err)
	}
	var spec validation.ReleaseStageSpec
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&spec); err != nil {
		fatal(err)
	}
	report, err := validation.StageUnsignedGoRelease(*stage, *archive, spec)
	if err != nil {
		fatal(err)
	}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	encoded = append(encoded, '\n')
	if err = os.WriteFile(*reportPath, encoded, 0644); err != nil {
		fatal(err)
	}
	if report.Status == validation.StatusBlocked {
		os.Exit(3)
	}
	if report.Status != validation.StatusPass {
		os.Exit(1)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "release staging:", err); os.Exit(1) }
