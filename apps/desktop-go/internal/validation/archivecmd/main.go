package main

import (
	"encoding/json"
	"fmt"
	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
	"io"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: archivecmd ARCHIVE")
		os.Exit(2)
	}
	entries, err := validation.ArchiveEntries(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeArchiveReport(os.Stdout, len(entries)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func writeArchiveReport(writer io.Writer, entries int) error {
	return json.NewEncoder(writer).Encode(struct {
		Status  validation.Status `json:"status"`
		Check   string            `json:"check"`
		Entries int               `json:"entries"`
	}{validation.StatusPass, "archive-shape-only", entries})
}
