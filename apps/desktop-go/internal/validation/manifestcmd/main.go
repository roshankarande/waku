package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
	"os"
)

func main() {
	root := flag.String("root", "", "staged release root")
	specPath := flag.String("spec", "", "manifest input specification")
	out := flag.String("out", "", "output manifest JSON")
	flag.Parse()
	if *root == "" || *specPath == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: manifestcmd -root DIR -spec SPEC.json -out MANIFEST.json")
		os.Exit(2)
	}
	data, err := os.ReadFile(*specPath)
	if err != nil {
		fatal(err)
	}
	var spec validation.ManifestSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		fatal(err)
	}
	manifest, err := validation.GenerateDistributionManifest(*root, spec)
	if err != nil {
		fatal(err)
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(*out, encoded, 0644); err != nil {
		fatal(err)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "manifest generation:", err); os.Exit(1) }
