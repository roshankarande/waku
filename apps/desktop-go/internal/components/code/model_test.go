package code

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
)

func TestDiffPositionsHighlightsSearchAndSelection(t *testing.T) {
	patch := "diff --git a/main.go b/main.go\n--- a/main.go\n+++ b/main.go\n@@ -10,2 +10,2 @@\n-const old = 1\n+const answer = 2\n context\n"
	s := PrepareDiff(patch, 4)
	if s.Generation != 4 {
		t.Fatalf("generation %d", s.Generation)
	}
	var add, del *Line
	for i := range s.Lines {
		switch s.Lines[i].Kind {
		case Addition:
			add = &s.Lines[i]
		case Deletion:
			del = &s.Lines[i]
		}
	}
	if add == nil || del == nil || add.NewLine != 10 || del.OldLine != 10 {
		t.Fatalf("positions add=%#v del=%#v", add, del)
	}
	if len(add.Tokens) == 0 || add.Tokens[0].Kind != Keyword {
		t.Fatalf("not highlighted off-view: %#v", add.Tokens)
	}
	if got := s.Search("answer"); len(got) != 1 || got[0].LineID != add.ID {
		t.Fatalf("search %#v", got)
	}
	text, err := s.Copy(Selection{Point{del.ID, 0}, Point{add.ID, len(add.Text)}})
	if err != nil {
		t.Fatal(err)
	}
	if text != "const old = 1\nconst answer = 2" {
		t.Fatalf("copied %q", text)
	}
}

func TestDuplicateRowsHaveUniqueIDsAndUnicodeSearchOffsets(t *testing.T) {
	s := PrepareDiff("@@ -1,2 +1,2 @@\n same\n same\n+İstanbul\n", 1)
	seen := map[uint64]bool{}
	for _, l := range s.Lines {
		if seen[l.ID] {
			t.Fatalf("duplicate row id %d for %q", l.ID, l.Text)
		}
		seen[l.ID] = true
	}
	if got := s.Search("i"); len(got) != 0 {
		t.Fatalf("Unicode simple fold incorrectly matched dotted I: %#v", got)
	}
	m := s.Search("İ")
	if len(m) != 1 {
		t.Fatalf("matches %#v", m)
	}
	l := lineByID(s, m[0].LineID)
	if l == nil || l.Text[m[0].Range.Start:m[0].Range.End] != "İ" {
		t.Fatalf("casefold range %#v line %#v", m, l)
	}
}

func TestCodeSimpleFoldRangeValidationAndMatchCap(t *testing.T) {
	s := PrepareDiff("+Σ ς σ "+strings.Repeat("a ", MaxSearchMatches+20), 1)
	if got := s.Search("σ"); len(got) != 3 {
		t.Fatalf("sigma matches %d", len(got))
	}
	if got := s.Search("a"); len(got) != MaxSearchMatches {
		t.Fatalf("cap %d", len(got))
	}
	id := s.Lines[0].ID
	if _, err := s.Copy(Selection{Point{id, 1}, Point{id, 2}}); err == nil {
		t.Fatal("accepted split UTF-8 range")
	}
}

func TestAsyncDiffSearchRejectsStaleResult(t *testing.T) {
	w := NewSearchWorker(1, 2)
	defer w.Close()
	old := PrepareDiff("+old target", 1)
	ch, err := w.Submit(context.Background(), old, "target")
	if err != nil {
		t.Fatal(err)
	}
	result := <-ch
	newer := PrepareDiff("+new target", 2)
	state := &ViewState{Query: "new"}
	if state.ApplySearchResult(newer, result) {
		t.Fatal("stale result committed")
	}
	state.Query = "target"
	ch, err = w.Submit(context.Background(), newer, "target")
	if err != nil {
		t.Fatal(err)
	}
	if !state.ApplySearchResult(newer, <-ch) {
		t.Fatal("current result rejected")
	}
}

func TestIncrementalDiffAppendKeepsStreamedRowIdentity(t *testing.T) {
	s := PrepareDiff("@@ -1,1 +1,2 @@\n+hel", 1)
	id := s.Lines[len(s.Lines)-1].ID
	tailStart := s.tailStart
	next := s.Append("lo\n+next")
	if next.Lines[1].ID != id || next.Lines[1].Text != "hello" {
		t.Fatalf("streamed row changed: %#v -> %#v", s.Lines[len(s.Lines)-1], next.Lines[1])
	}
	if next.tailStart <= tailStart || next.Lines[len(next.Lines)-1].Text != "next" {
		t.Fatalf("append did not advance bounded tail: start %d -> %d lines=%#v", tailStart, next.tailStart, next.Lines)
	}
	third := next.Append(" value")
	if third.Lines[len(third.Lines)-1].ID != next.Lines[len(next.Lines)-1].ID || third.Lines[len(third.Lines)-1].Text != "next value" {
		t.Fatalf("active row identity/text changed: %#v", third.Lines[len(third.Lines)-1])
	}
}

func TestDiffSearchContextCancellation(t *testing.T) {
	s := PrepareDiff("+"+strings.Repeat("haystack ", 7000), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SearchContext(ctx, "needle"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled search: %v", err)
	}
}

type checkpointContext struct {
	context.Context
	calls atomic.Int32
	after int32
}

func (c *checkpointContext) Err() error {
	if c.calls.Add(1) >= c.after {
		return context.Canceled
	}
	return nil
}

func TestDiffSearchCancellationUsesRuneCounterCheckpoints(t *testing.T) {
	s := PrepareDiff("+"+strings.Repeat("aé", 30_000), 1)
	ctx := &checkpointContext{Context: context.Background(), after: 3}
	if _, err := s.SearchContext(ctx, "absent"); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-scan cancellation missed after %d checks: %v", ctx.calls.Load(), err)
	}
}

func TestDiffSearchCloseCompletesEveryAcceptedRequest(t *testing.T) {
	w := NewSearchWorker(1, 64)
	s := PrepareDiff("+"+strings.Repeat("searchable text ", 50_000), 1)
	results := make([]<-chan SearchResult, 0, 64)
	for range 64 {
		ch, err := w.Submit(context.Background(), s, "not present")
		if errors.Is(err, ErrSearchQueueFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, ch)
	}
	w.Close()
	for i, ch := range results {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("accepted request %d was abandoned by Close", i)
		}
	}
	if _, err := w.Submit(context.Background(), s, "searchable"); !errors.Is(err, context.Canceled) {
		t.Fatalf("submit after close: %v", err)
	}
}

func TestDiffPreparationCapsInputAndLineBeforeHighlight(t *testing.T) {
	huge := "+" + strings.Repeat("é", MaxCodeLineBytes) + "\n" + strings.Repeat("x", MaxDiffInputBytes)
	s := PrepareDiff(huge, 1)
	if !s.Truncated {
		t.Fatal("oversized diff not marked truncated")
	}
	for i, l := range s.Lines {
		if len(l.Text) > MaxCodeLineBytes {
			t.Fatalf("line %d has %d bytes", i, len(l.Text))
		}
	}
}

func TestLargeDiffIsCappedAndViewVirtualizes(t *testing.T) {
	var p strings.Builder
	p.WriteString("diff --git a/generated.go b/generated.go\n@@ -1,1 +1,60000 @@\n")
	for i := 0; i < 60_000; i++ {
		fmt.Fprintf(&p, "+const line%d = %d\n", i, i)
	}
	s := PrepareDiff(p.String(), 1)
	if !s.Truncated || len(s.Lines) != MaxDiffRows || s.TotalLines <= MaxDiffRows {
		t.Fatalf("large diff rows=%d total=%d truncated=%v", len(s.Lines), s.TotalLines, s.Truncated)
	}
	state := new(ViewState)
	tt := ui.NewTester(func(c *ui.Context) { DiffView(c, s, state).Height(180) }, 700, 180)
	if !tt.HasText("const line0") {
		t.Fatalf("first diff row absent: %q", tt.Texts())
	}
	if len(tt.Texts()) > 60 {
		t.Fatalf("built %d labels for %d-row diff", len(tt.Texts()), len(s.Lines))
	}
	// Copy does not depend on either endpoint being mounted.
	first, last := s.Lines[2], s.Lines[len(s.Lines)-1]
	got, err := s.Copy(Selection{Point{first.ID, 0}, Point{last.ID, len(last.Text)}})
	if err != nil || !strings.Contains(got, "line49997") {
		t.Fatalf("offscreen copy len=%d err=%v", len(got), err)
	}
}

func TestNativeEditorIMEComposition(t *testing.T) {
	session := &EditorSession{Model: EditorModel{Text: "prefix "}}
	tt := ui.NewTester(func(c *ui.Context) { EditorView(c, session).AutoFocus() }, 480, 180)
	if _, ok := tt.TextCaret(); !ok {
		t.Fatal("native editor did not expose an IME caret")
	}
	tt.Compose("日本", 2)
	// Composition is provisional; committing text goes through native input.
	if session.Model.Text != "prefix " {
		t.Fatalf("composition committed early: %q", session.Model.Text)
	}
	tt.Type("日本")
	if session.Model.Text != "prefix 日本" || !session.Model.Dirty || session.Model.Generation == 0 {
		t.Fatalf("IME commit produced %#v", session.Model)
	}
	tt.Key(ui.Cmd, ui.KeyA)
	tt.Command("copy")
	if tt.Clipboard() != session.Model.Text {
		t.Fatalf("clipboard %q", tt.Clipboard())
	}
}

func TestDiffSearchHighlightNavigationAndCopyInteraction(t *testing.T) {
	var p strings.Builder
	p.WriteString("@@ -1,1 +1,300 @@\n")
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&p, "+const row%d = %d\n", i, i)
	}
	s := PrepareDiff(p.String(), 1)
	state := new(ViewState)
	tt := ui.NewTester(func(c *ui.Context) { DiffView(c, s, state).Height(220) }, 760, 220)
	if err := tt.Click("Search diff"); err != nil {
		t.Fatal(err)
	}
	tt.Type("row299")
	if !state.Searching {
		t.Fatal("search request not surfaced")
	}
	state.ApplySearchResult(s, SearchResult{Generation: s.Generation, Query: state.Query, Matches: s.Search(state.Query)})
	tt.Frame()
	if !tt.HasText("const row299 = 299") {
		t.Fatalf("match not visible: %q", tt.Texts())
	}
	if err := tt.Click("Copy diff match"); err != nil {
		t.Fatal(err)
	}
	if tt.Clipboard() != "row299" {
		t.Fatalf("match copy %q", tt.Clipboard())
	}
	selectAll(s, state)
	tt.Frame()
	if err := tt.Click("Copy code selection"); err != nil {
		t.Fatal(err)
	}
	want, _ := s.Copy(*state.Selection)
	if tt.Clipboard() != want {
		t.Fatalf("line copy %q want %q", tt.Clipboard(), want)
	}
}

func TestCodePointerSelectionCapabilityGateAndKeyboardAcrossRows(t *testing.T) {
	var p strings.Builder
	p.WriteString("@@ -1,1 +1,300 @@\n")
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&p, "+row-%03d-abcdef\n", i)
	}
	s := PrepareDiff(p.String(), 1)
	state := new(ViewState)
	tt := ui.NewTester(func(c *ui.Context) { DiffView(c, s, state).Height(220) }, 700, 220)
	r, ok := tt.Find("row-000-abcdef")
	if !ok {
		t.Fatalf("first row absent %q", tt.Texts())
	}
	tt.ClickAt(r.X+3*7.5, r.Y+r.H/2)
	if PrecisePointerSelection || state.Selection != nil {
		t.Fatal("inexact pointer selection was exposed")
	}
	tt.Key(ui.Shift, ui.KeyRight)
	if state.Selection == nil {
		t.Fatal("Shift+Right did not create a reachable anchor")
	}
	tt.Key(ui.Shift, ui.KeyDown)
	if state.Selection.Anchor.LineID == state.Selection.Head.LineID {
		t.Fatal("keyboard selection did not span rows")
	}
	want, err := s.Copy(*state.Selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := tt.Click("Copy code selection"); err != nil {
		t.Fatal(err)
	}
	if tt.Clipboard() != want {
		t.Fatalf("copy mismatch %d != %d", len(tt.Clipboard()), len(want))
	}
}
