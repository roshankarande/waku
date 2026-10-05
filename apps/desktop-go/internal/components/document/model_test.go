package document

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/egoist/mygo/ui"
)

func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "document", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMixedDocumentAndStableStreamingOffsets(t *testing.T) {
	s := Prepare(fixture(t, "mixed.md"), 7)
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	want := map[Kind]bool{Heading: false, Paragraph: false, Table: false, CodeBlock: false, Math: false}
	for _, b := range s.Blocks {
		want[b.Kind] = true
	}
	for kind, found := range want {
		if !found {
			t.Errorf("missing block kind %d", kind)
		}
	}
	old := append([]Block(nil), s.Blocks...)
	next := s.Append("\nA streamed tail")
	for i, b := range old {
		if next.Blocks[i].ID != b.ID || next.Blocks[i].Source != b.Source {
			t.Fatalf("block %d moved across append: %#v -> %#v", i, b, next.Blocks[i])
		}
	}
	if next.Generation != 8 {
		t.Errorf("generation %d", next.Generation)
	}
	matches := next.Search("answer")
	if len(matches) != 2 {
		t.Fatalf("answer matches: %#v", matches)
	}
	for _, m := range matches {
		if !m.Source.validFor(next.Source) {
			t.Errorf("bad source match %#v", m)
		}
	}
}

func TestActiveStreamBlockKeepsIdentityAndUnicodeSearchOffsets(t *testing.T) {
	s := Prepare("İstanbul", 1)
	id := s.Blocks[0].ID
	next := s.Append(" grows")
	if next.Blocks[0].ID != id {
		t.Fatalf("active block identity changed: %d -> %d", id, next.Blocks[0].ID)
	}
	if got := next.Search("i"); len(got) != 0 {
		t.Fatalf("Unicode simple fold incorrectly matched dotted I: %#v", got)
	}
	matches := next.Search("İ")
	if len(matches) != 1 || next.Blocks[0].Text[matches[0].Text.Start:matches[0].Text.End] != "İ" {
		t.Fatalf("casefold offsets %#v", matches)
	}
}

func TestStreamIdentitySurvivesKindReclassification(t *testing.T) {
	paragraph := Prepare("a | b", 1)
	id := paragraph.Blocks[0].ID
	table := paragraph.Append("\n---|---\n1|2")
	if table.Blocks[0].Kind != Table || table.Blocks[0].ID != id {
		t.Fatalf("table reclassification %#v -> %#v", paragraph.Blocks[0], table.Blocks[0])
	}
	plain := Prepare("$", 1)
	id = plain.Blocks[0].ID
	math := plain.Append("$\nx+y\n$$")
	if math.Blocks[0].Kind != Math || math.Blocks[0].ID != id {
		t.Fatalf("math reclassification %#v -> %#v", plain.Blocks[0], math.Blocks[0])
	}
}

func TestLargeFirstLineCanStillReclassifyAsTable(t *testing.T) {
	header := "a|" + strings.Repeat("x", MaxBlockTextBytes+100) + "|b"
	s := Prepare(header, 1)
	firstID := s.Blocks[0].ID
	if len(s.Blocks) < 2 || s.Blocks[0].Kind != Paragraph {
		t.Fatalf("large header preparation: %#v", s.Blocks)
	}
	next := s.Append("\n---|---\n1|2")
	if next.Blocks[0].ID != firstID {
		t.Fatal("reclassification changed logical block identity")
	}
	for i, block := range next.Blocks {
		if block.Kind != Table {
			t.Fatalf("block %d froze as %v instead of table", i, block.Kind)
		}
	}
}

func TestUnicodeSimpleFoldAndMatchCap(t *testing.T) {
	s := Prepare("Σ ς σ "+strings.Repeat("a ", MaxSearchMatches+20), 1)
	if got := s.Search("σ"); len(got) != 3 {
		t.Fatalf("sigma fold matches %d: %#v", len(got), got)
	}
	if got := s.Search("a"); len(got) != MaxSearchMatches {
		t.Fatalf("match cap %d", len(got))
	}
}

func TestCopyRejectsInvalidRanges(t *testing.T) {
	s := Prepare("éclair", 1)
	id := s.Blocks[0].ID
	for _, sel := range []Selection{{Point{id, -1}, Point{id, 1}}, {Point{id, 1}, Point{id, 2}}, {Point{id, 0}, Point{id, 99}}} {
		if _, err := s.Copy(sel); err == nil {
			t.Fatalf("accepted invalid selection %#v", sel)
		}
	}
}

func TestCopyAcrossArtificialPartsHasNoInventedSeparator(t *testing.T) {
	source := strings.Repeat("x", MaxBlockTextBytes*2+17)
	s := Prepare(source, 1)
	if len(s.Blocks) < 3 {
		t.Fatalf("expected split blocks: %d", len(s.Blocks))
	}
	first, last := s.Blocks[0], s.Blocks[len(s.Blocks)-1]
	got, err := s.Copy(Selection{Point{first.ID, 0}, Point{last.ID, len(last.Text)}})
	if err != nil || got != source {
		t.Fatalf("split copy len=%d want=%d err=%v", len(got), len(source), err)
	}
	next := s.Append("tail")
	if next.Blocks[len(next.Blocks)-1].ID != last.ID {
		t.Fatal("bounded active paragraph part changed identity")
	}
}

func TestSearchContextCancellation(t *testing.T) {
	s := Prepare(strings.Repeat("search haystack ", 10_000), 1)
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

func TestSearchCancellationUsesRuneCounterCheckpoints(t *testing.T) {
	// Mixed one- and two-byte runes produce byte offsets that do not reliably
	// hit a power-of-two mask. Cancellation must follow iteration count.
	s := Prepare(strings.Repeat("aé", 40_000), 1)
	ctx := &checkpointContext{Context: context.Background(), after: 3}
	if _, err := s.SearchContext(ctx, "absent"); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-scan cancellation missed after %d checks: %v", ctx.calls.Load(), err)
	}
}

func TestPreparationBoundsHugeSourceAndLayouts(t *testing.T) {
	s := Prepare(strings.Repeat("é", MaxDocumentSourceBytes), 1)
	if !s.Truncated {
		t.Fatal("oversized source was not marked truncated")
	}
	for i, b := range s.Blocks {
		if len(b.Text) > MaxBlockTextBytes+4 {
			t.Fatalf("block %d has %d bytes", i, len(b.Text))
		}
		if !utf8.ValidString(b.Text) {
			t.Fatalf("block %d is invalid UTF-8", i)
		}
	}
}

func TestPreparationSplitsHugeTableAndMathLayouts(t *testing.T) {
	cell := strings.Repeat("x", MaxBlockTextBytes+100)
	table := Prepare("a|b\n---|---\n"+cell+"|ok", 1)
	math := Prepare("$$\n"+strings.Repeat("y", MaxBlockTextBytes+100)+"\n$$", 1)
	for name, s := range map[string]*Snapshot{"table": table, "math": math} {
		if err := s.Validate(); err != nil {
			t.Fatalf("%s validation: %v", name, err)
		}
		if len(s.Blocks) < 2 {
			t.Fatalf("%s not split", name)
		}
		for _, b := range s.Blocks {
			if len(b.Text) > MaxBlockTextBytes {
				t.Fatalf("%s block %d bytes", name, len(b.Text))
			}
			for _, row := range b.Rows {
				for _, cell := range row {
					if len(cell) > MaxBlockTextBytes {
						t.Fatalf("%s retained an unbounded table cell: %d bytes", name, len(cell))
					}
				}
			}
		}
	}
}

func TestAsyncSearchRejectsStaleGenerationAndQuery(t *testing.T) {
	w := NewSearchWorker(1, 2)
	defer w.Close()
	old := Prepare("old target", 1)
	ch, err := w.Submit(context.Background(), old, "target")
	if err != nil {
		t.Fatal(err)
	}
	result := <-ch
	newer := old.Append("\n\nnew target")
	state := &ViewState{Query: "new"}
	if state.ApplySearchResult(newer, result) {
		t.Fatal("stale generation/query committed")
	}
	state.Query = "target"
	ch, err = w.Submit(context.Background(), newer, "target")
	if err != nil {
		t.Fatal(err)
	}
	if !state.ApplySearchResult(newer, <-ch) {
		t.Fatal("current async result rejected")
	}
}

func TestSearchCloseCompletesEveryAcceptedRequest(t *testing.T) {
	w := NewSearchWorker(1, 64)
	s := Prepare(strings.Repeat("searchable text ", 50_000), 1)
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

func TestSelectionCopiesAcrossUnmountedBlocks(t *testing.T) {
	s := Prepare(fixture(t, "mixed.md"), 1)
	a, b := s.Blocks[0], s.Blocks[len(s.Blocks)-1]
	got, err := s.Copy(Selection{Anchor: Point{a.ID, 0}, Head: Point{b.ID, len(b.Text)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"Streaming proof", "Provider\tReady", "func answer", "\\frac{a}{b}"} {
		if !strings.Contains(got, part) {
			t.Errorf("copy missing %q in %q", part, got)
		}
	}
	// Copy accepts only explicit UTF-8 boundaries.
	u := Prepare("éclair", 1)
	got, err = u.Copy(Selection{Point{u.Blocks[0].ID, 2}, Point{u.Blocks[0].ID, 7}})
	if err != nil || got != "clair" {
		t.Fatalf("UTF-8 copy %q, %v", got, err)
	}
}

func TestDocumentViewIsHeadlessAndVirtualized(t *testing.T) {
	var src strings.Builder
	for i := 0; i < 10_000; i++ {
		src.WriteString("row\n\n")
	}
	s := Prepare(src.String(), 1)
	state := new(ViewState)
	tt := ui.NewTester(func(c *ui.Context) { View(c, s, state).Height(180) }, 420, 180)
	if !tt.HasText("row") {
		t.Fatal("no visible document row")
	}
	if len(tt.Texts()) > 40 {
		t.Fatalf("built %d labels for 10,000 rows", len(tt.Texts()))
	}
}

func TestDocumentSearchNavigationAndCopyInteraction(t *testing.T) {
	var src strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&src, "block %03d\n\n", i)
	}
	s := Prepare(src.String()+"TARGET İSTANBUL", 1)
	state := new(ViewState)
	tt := ui.NewTester(func(c *ui.Context) { View(c, s, state).Height(220) }, 640, 220)
	if err := tt.Click("Search document"); err != nil {
		t.Fatal(err)
	}
	tt.Type("target")
	if !state.Searching {
		t.Fatal("typing did not request off-view search")
	}
	state.ApplySearchResult(s, SearchResult{Generation: s.Generation, Query: state.Query, Matches: s.Search(state.Query)})
	tt.Frame()
	if !tt.HasText("TARGET İSTANBUL") {
		t.Fatalf("match was not scrolled into view: %q", tt.Texts())
	}
	if err := tt.Click("Copy match"); err != nil {
		t.Fatal(err)
	}
	if tt.Clipboard() != "TARGET" {
		t.Fatalf("match clipboard %q", tt.Clipboard())
	}
	selectAll(s, state)
	tt.Frame()
	if err := tt.Click("Copy document selection"); err != nil {
		t.Fatal(err)
	}
	want, _ := s.Copy(*state.Selection)
	if tt.Clipboard() != want {
		t.Fatalf("selection clipboard %q want %q", tt.Clipboard(), want)
	}
}

func TestPointerSelectionCapabilityGateAndKeyboardAcrossVirtualRows(t *testing.T) {
	var src strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&src, "row-%03d-abcdef\n\n", i)
	}
	s := Prepare(src.String(), 1)
	state := new(ViewState)
	tt := ui.NewTester(func(c *ui.Context) { View(c, s, state).Height(220) }, 640, 220)
	r, ok := tt.Find("row-000-abcdef")
	if !ok {
		t.Fatal("first row absent")
	}
	tt.ClickAt(r.X+8+3*7.5, r.Y+r.H/2)
	if PrecisePointerSelection || state.Selection != nil {
		t.Fatal("inexact pointer selection was exposed")
	}
	tt.Key(ui.Shift, ui.KeyRight)
	if state.Selection == nil {
		t.Fatal("Shift+Right did not create a reachable anchor")
	}
	tt.Key(ui.Shift, ui.KeyDown)
	if state.Selection.Anchor.BlockID == state.Selection.Head.BlockID {
		t.Fatal("keyboard selection did not span rows")
	}
	before := state.Selection.Head.Offset
	tt.Key(ui.Shift, ui.KeyLeft)
	if state.Selection.Head.Offset >= before {
		t.Fatal("keyboard did not contract endpoint")
	}
	want, err := s.Copy(*state.Selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := tt.Click("Copy document selection"); err != nil {
		t.Fatal(err)
	}
	if tt.Clipboard() != want {
		t.Fatalf("virtual selection copy mismatch %d != %d", len(tt.Clipboard()), len(want))
	}
}

func TestMathServiceBoundsQueueCachesAndKeepsFallback(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	r := MathRendererFunc(func(_ context.Context, source string, _ bool) ([]byte, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		if source == "bad" {
			return nil, errors.New("invalid latex")
		}
		return []byte("draw:" + source), nil
	})
	s := NewMathService(r, MathOptions{Workers: 1, Queue: 1, CacheEntries: 2, CacheBytes: 128})
	defer s.Close()
	if got, err := s.Request(Formula{Source: "one"}); err != nil || got.Status != MathPending {
		t.Fatalf("first %#v %v", got, err)
	}
	<-started
	if _, err := s.Request(Formula{Source: "two"}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Request(Formula{Source: "three"}); !errors.Is(err, ErrMathQueueFull) || got.Source != "three" {
		t.Fatalf("queue fallback %#v %v", got, err)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.Request(Formula{Source: "one"})
		if got.Status == MathReady && string(got.Output) == "draw:one" {
			s.mu.Lock()
			n := s.lru.Len()
			s.mu.Unlock()
			if n > 2 {
				t.Fatalf("unbounded cache: %d", n)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("math result did not become ready")
}

func TestMathInvalidAndOversizeRemainSelectable(t *testing.T) {
	s := NewMathService(MathRendererFunc(func(context.Context, string, bool) ([]byte, error) { return nil, errors.New("bad formula") }), MathOptions{Workers: 1})
	defer s.Close()
	formula := Formula{Source: strings.Repeat("x", MaxMathSourceBytes+1)}
	got, err := s.Request(formula)
	if err != nil || got.Status != MathInvalid || got.Source != formula.Source {
		t.Fatalf("oversize %#v %v", got, err)
	}

	doc := Prepare(fixture(t, "invalid-math.md"), 1)
	invalid := doc.Blocks[len(doc.Blocks)-1].Formula
	if got, err = s.Request(invalid); err != nil || got.Status != MathPending || got.Source != invalid.Source {
		t.Fatalf("pending fallback %#v %v", got, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ = s.Request(invalid)
		if got.Status == MathInvalid {
			if got.Source != invalid.Source || got.Err == "" {
				t.Fatalf("invalid fallback %#v", got)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("invalid math result did not complete")
}

func TestMathCloseCancelsRendererAndConcurrentRequests(t *testing.T) {
	started := make(chan struct{})
	s := NewMathService(MathRendererFunc(func(ctx context.Context, _ string, _ bool) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), MathOptions{Workers: 1})
	if _, err := s.Request(Formula{Source: "slow"}); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel renderer")
	}
	got, err := s.Request(Formula{Source: "after close"})
	if err != nil || got.Status != MathInvalid {
		t.Fatalf("closed request %#v %v", got, err)
	}
	// Close remains idempotent under a second caller.
	s.Close()
}

func TestMathRequestCancellationAndImmutableCacheHit(t *testing.T) {
	started := make(chan struct{}, 1)
	s := NewMathService(MathRendererFunc(func(ctx context.Context, source string, _ bool) ([]byte, error) {
		if source == "cancel" {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []byte(strings.Repeat("z", 4096)), nil
	}), MathOptions{Workers: 1})
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := s.RequestContext(ctx, Formula{Source: "cancel"}); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		_, pending := s.pending[mathKey{"cancel", false}]
		s.mu.Unlock()
		if !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelled request stayed pending")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := s.Request(Formula{Source: "cached"}); err != nil {
		t.Fatal(err)
	}
	var first MathResult
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		first, _ = s.Request(Formula{Source: "cached"})
		if first.Status == MathReady {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if first.Status != MathReady {
		t.Fatal("cache result not ready")
	}
	second, _ := s.Request(Formula{Source: "cached"})
	if unsafe.StringData(first.Output) != unsafe.StringData(second.Output) {
		t.Fatal("cache hit cloned output")
	}
}

func TestMathDedupeKeepsRenderingForRemainingWaiter(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	s := NewMathService(MathRendererFunc(func(ctx context.Context, _ string, _ bool) ([]byte, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}), MathOptions{Workers: 1})
	defer s.Close()
	one, cancelOne := context.WithCancel(context.Background())
	two, cancelTwo := context.WithCancel(context.Background())
	if _, err := s.RequestContext(one, Formula{Source: "shared"}); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := s.RequestContext(two, Formula{Source: "shared"}); err != nil {
		t.Fatal(err)
	}
	cancelOne()
	select {
	case <-cancelled:
		t.Fatal("one cancelled waiter stopped shared render")
	case <-time.After(20 * time.Millisecond):
	}
	cancelTwo()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("last waiter did not cancel shared render")
	}
}

func TestMathCloseDoesNotTrustRendererCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s := NewMathService(MathRendererFunc(func(context.Context, string, bool) ([]byte, error) {
		close(started)
		<-release // deliberately ignores cancellation
		return nil, nil
	}), MathOptions{Workers: 1})
	_, _ = s.Request(Formula{Source: "noncooperative"})
	<-started
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on noncooperative renderer")
	}
	close(release)
}

func TestMathNoncooperativeRenderersKeepBoundedAdmission(t *testing.T) {
	release := make(chan struct{})
	var calls, active, maximum atomic.Int32
	s := NewMathService(MathRendererFunc(func(context.Context, string, bool) ([]byte, error) {
		calls.Add(1)
		n := active.Add(1)
		for {
			old := maximum.Load()
			if n <= old || maximum.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		active.Add(-1)
		return nil, nil
	}), MathOptions{Workers: 2, Queue: 32})
	var cancels []context.CancelFunc
	for i := range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		_, err := s.RequestContext(ctx, Formula{Source: fmt.Sprintf("stuck-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 2 {
		t.Fatalf("initial renderer slots were not occupied: %d", calls.Load())
	}
	for _, cancel := range cancels {
		cancel()
	}
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got > 2 {
		t.Fatalf("noncooperative renderer admission grew to %d", got)
	}
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close waited for bounded renderer slots")
	}
	close(release)
	if maximum.Load() > 2 {
		t.Fatalf("renderer concurrency reached %d", maximum.Load())
	}
}
