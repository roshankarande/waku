package main

import (
	"errors"
	"testing"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("stdout closed") }

func TestWriteArchiveReportPropagatesStdoutFailure(t *testing.T) {
	if err := writeArchiveReport(failingWriter{}, 3); err == nil {
		t.Fatal("expected JSON stdout error")
	}
}
