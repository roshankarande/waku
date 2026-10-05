package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
)

func main() {
	root := flag.String("root", "", "unpacked distribution prefix")
	archive := flag.String("archive", "", "release archive to safely extract and inspect")
	archiveSHA256 := flag.String("archive-sha256", "", "expected release archive SHA-256")
	manifestPath := flag.String("manifest", "", "distribution manifest JSON")
	flag.Parse()
	if (*root == "") == (*archive == "") || *manifestPath == "" {
		fmt.Fprintln(os.Stderr, "usage: inspectcmd (-root DIR | -archive FILE -archive-sha256 HEX) -manifest FILE")
		os.Exit(2)
	}
	manifest, err := validation.LoadDistributionManifest(*manifestPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var report any
	status := validation.StatusFail
	if *archive != "" {
		archiveReport, inspectErr := validation.InspectDistributionArchive(*archive, *archiveSHA256, manifest)
		if inspectErr != nil {
			fmt.Fprintln(os.Stderr, inspectErr)
			os.Exit(1)
		}
		report, status = archiveReport, archiveReport.Status
	} else {
		distributionReport := validation.InspectDistribution(*root, manifest)
		report, status = distributionReport, distributionReport.Status
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if status != validation.StatusPass {
		os.Exit(1)
	}
}
