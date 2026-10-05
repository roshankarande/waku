package code

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
)

type memoryFiles struct {
	mu    sync.Mutex
	files map[string]LoadedFile
}

type blockingFiles struct{ started chan struct{} }

func (b *blockingFiles) Load(ctx context.Context, path string) (LoadedFile, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return LoadedFile{}, ctx.Err()
}

type gatedFiles struct {
	gate chan struct{}
	file LoadedFile
}

func (g *gatedFiles) Load(ctx context.Context, _ string) (LoadedFile, error) {
	select {
	case <-g.gate:
		return g.file, nil
	case <-ctx.Done():
		return LoadedFile{}, ctx.Err()
	}
}
func (g *gatedFiles) Save(ctx context.Context, q SaveFileRequest) (SavedFile, error) {
	select {
	case <-g.gate:
		return SavedFile{Revision: "saved"}, nil
	case <-ctx.Done():
		return SavedFile{}, ctx.Err()
	}
}
func (b *blockingFiles) Save(ctx context.Context, q SaveFileRequest) (SavedFile, error) {
	<-ctx.Done()
	return SavedFile{}, ctx.Err()
}

func TestFileWorkerBoundedQueueCloseAndCancellation(t *testing.T) {
	b := &blockingFiles{started: make(chan struct{}, 1)}
	w := NewFileWorker(b, 1)
	results := []<-chan FileResult{w.Load(context.Background(), "a", 1)}
	<-b.started
	results = append(results, w.Load(context.Background(), "b", 2), w.Load(context.Background(), "c", 3))
	full := w.Load(context.Background(), "d", 4)
	if r := <-full; !errors.Is(r.Err, ErrFileQueueFull) {
		t.Fatalf("saturation %v", r.Err)
	}
	done := make(chan struct{})
	go func() { w.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel and wait for repository work")
	}
	for i, ch := range results {
		select {
		case r := <-ch:
			if !errors.Is(r.Err, context.Canceled) {
				t.Fatalf("result %d: %v", i, r.Err)
			}
		case <-time.After(time.Second):
			t.Fatalf("result %d abandoned", i)
		}
	}
	if r := <-w.Load(context.Background(), "after", 5); !errors.Is(r.Err, ErrFileWorkerClosed) {
		t.Fatalf("post-close %v", r.Err)
	}
}

func (m *memoryFiles) Load(_ context.Context, path string) (LoadedFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[path]
	if !ok {
		return LoadedFile{}, errors.New("missing")
	}
	return f, nil
}
func (m *memoryFiles) Save(_ context.Context, q SaveFileRequest) (SavedFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.files[q.Path]
	if f.Revision != q.ExpectedRevision {
		return SavedFile{}, ErrRevisionConflict
	}
	f.Text = q.Text
	f.Revision = fmt.Sprint("r", len(f.Text))
	m.files[q.Path] = f
	return SavedFile{Revision: f.Revision}, nil
}

func TestAsyncLoadSaveGenerationAndConflict(t *testing.T) {
	repo := &memoryFiles{files: map[string]LoadedFile{"x.go": {Path: "x.go", Revision: "r1", Text: "old"}}}
	w := NewFileWorker(repo, 1)
	defer w.Close()
	e := &EditorModel{Generation: 7}
	load := <-w.Load(context.Background(), "x.go", 7)
	if !e.ApplyLoad(load) || e.Text != "old" || e.Generation != 8 {
		t.Fatalf("load %#v editor %#v", load, e)
	}
	if e.ApplyLoad(load) {
		t.Fatal("accepted stale load")
	}
	if err := e.Select(0, 3); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyEdit(EditorEdit{Generation: 8, Start: 0, End: 3, Text: "new"}); err != nil {
		t.Fatal(err)
	}
	req, gen := e.SaveRequest()
	// An external writer wins before our save.
	repo.mu.Lock()
	repo.files["x.go"] = LoadedFile{Path: "x.go", Revision: "other", Text: "external"}
	repo.mu.Unlock()
	conflict := <-w.Save(context.Background(), req, gen)
	if !errors.Is(conflict.Err, ErrRevisionConflict) || e.ApplySave(conflict) || e.Text != "new" || !e.Dirty {
		t.Fatalf("conflict lost editor state: %#v %#v", conflict, e)
	}
	// A completion for an older edit generation is rejected even when successful.
	repo.mu.Lock()
	repo.files["x.go"] = LoadedFile{Path: "x.go", Revision: e.Revision, Text: "old"}
	repo.mu.Unlock()
	result := <-w.Save(context.Background(), req, gen)
	if err := e.ApplyEdit(EditorEdit{Generation: gen, Start: 3, End: 3, Text: "er"}); err != nil {
		t.Fatal(err)
	}
	if e.ApplySave(result) {
		t.Fatal("accepted stale save completion")
	}
}

func TestEditorSessionWiresAsyncLoadAndConflict(t *testing.T) {
	repo := &memoryFiles{files: map[string]LoadedFile{"x": {Path: "x", Revision: "1", Text: "one"}}}
	w := NewFileWorker(repo, 1)
	defer w.Close()
	s := &EditorSession{Worker: w}
	if err := s.BeginLoad(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	for !s.Poll() {
		time.Sleep(time.Millisecond)
	}
	if s.Model.Text != "one" || s.LastError != nil {
		t.Fatalf("load %#v %v", s.Model, s.LastError)
	}
	if err := s.Model.ApplyEdit(EditorEdit{Generation: s.Model.Generation, Start: 3, End: 3, Text: "!"}); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	repo.files["x"] = LoadedFile{Path: "x", Revision: "2", Text: "external"}
	repo.mu.Unlock()
	if err := s.BeginSave(context.Background()); err != nil {
		t.Fatal(err)
	}
	for !s.Poll() {
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(s.LastError, ErrRevisionConflict) || !s.Model.Dirty || s.Model.Text != "one!" {
		t.Fatalf("conflict model=%#v err=%v", s.Model, s.LastError)
	}
}

func TestEditorViewAsyncCompletionInvalidatesWithoutPollLoop(t *testing.T) {
	repo := &gatedFiles{gate: make(chan struct{}), file: LoadedFile{Path: "x", Revision: "1", Text: "loaded text"}}
	w := NewFileWorker(repo, 1)
	defer w.Close()
	s := &EditorSession{Worker: w}
	defer s.Close()
	wake := make(chan struct{}, 4)
	tt := ui.NewTester(func(c *ui.Context) {
		s.SetInvalidator(func() {
			c.Invalidate()
			select {
			case wake <- struct{}{}:
			default:
			}
		})
		EditorView(c, s)
	}, 480, 180)
	if err := s.BeginLoad(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	close(repo.gate)
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("load completion did not invalidate")
	}
	tt.Frame() // headless host services the frame requested by Invalidate.
	if s.Model.Text != "loaded text" || s.LastError != nil {
		t.Fatalf("load was not applied by woken view: %#v %v", s.Model, s.LastError)
	}
	select {
	case <-wake:
		t.Fatal("completion produced unbounded duplicate invalidations")
	default:
	}
	if err := s.Model.ApplyEdit(EditorEdit{Generation: s.Model.Generation, Start: 11, End: 11, Text: "!"}); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginSave(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("save completion did not invalidate")
	}
	tt.Frame()
	if s.Model.Dirty || s.Model.Revision != "saved" {
		t.Fatalf("save was not applied: %#v", s.Model)
	}
}

func TestEditorSessionCloseSuppressesLateInvalidation(t *testing.T) {
	b := &blockingFiles{started: make(chan struct{}, 1)}
	w := NewFileWorker(b, 1)
	defer w.Close()
	s := &EditorSession{Worker: w}
	calls := atomic.Int32{}
	s.SetInvalidator(func() { calls.Add(1) })
	if err := s.BeginLoad(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	<-b.started
	s.Close()
	if calls.Load() != 0 {
		t.Fatalf("callback after close: %d", calls.Load())
	}
}

func TestEditorSelectionInputContextAndKeyboardEdits(t *testing.T) {
	e := &EditorModel{Generation: 3, Text: strings.Repeat("a", 2000) + "日本" + strings.Repeat("z", 2000)}
	if err := e.Select(2000, 2002); err != nil {
		t.Fatal(err)
	}
	c := e.InputContext()
	if c.Generation != 3 || c.Start != 512 || c.End != 514 || len([]rune(c.Text)) > 2048 {
		t.Fatalf("context %#v runes=%d", c, len([]rune(c.Text)))
	}
	if err := e.ApplyEdit(EditorEdit{Generation: 3, Start: e.Anchor, End: e.Caret, Text: "語"}); err != nil {
		t.Fatal(err)
	}
	if !e.Dirty || e.Anchor != 2001 || e.Caret != 2001 || !strings.Contains(e.Text, "a語z") {
		t.Fatalf("edit %#v", e)
	}
	if err := e.ApplyEdit(EditorEdit{Generation: 3, Start: 0, End: 0, Text: "stale"}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale edit %v", err)
	}
	ctx := e.InputContext()
	if err := e.ApplyNativeEdit(e.Generation, ctx.Base+ctx.Start, ctx.Base+ctx.End, "!"); err != nil {
		t.Fatal(err)
	}
}

func TestInputContextBoundariesAbsoluteNativeOffsetsAndSelectionGate(t *testing.T) {
	e := &EditorModel{Generation: 9, Text: strings.Repeat("é", 700) + "SELECT" + strings.Repeat("日", 700)}
	if err := e.Select(700, 706); err != nil {
		t.Fatal(err)
	}
	c := e.InputContext()
	if c.Base != 188 || c.Start != 512 || c.End != 518 || len([]rune(c.Text)) != 1030 {
		t.Fatalf("context %#v runes=%d", c, len([]rune(c.Text)))
	}
	if err := e.ApplyNativeEdit(9, c.Base+c.Start, c.Base+c.End, "X"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(e.Text, "éX日") {
		t.Fatal("absolute native replacement was double-offset")
	}
	if err := e.Select(0, 1025); err == nil {
		t.Fatal("oversized selection advertised to MyGo")
	}
}

func TestLargeReviewNavigationContextCommentsAndIntents(t *testing.T) {
	var p strings.Builder
	p.WriteString("diff --git a/x.go b/x.go\n")
	for h := 0; h < 1000; h++ {
		fmt.Fprintf(&p, "@@ -%d,1 +%d,1 @@\n-old%d\n+new%d\n", h+1, h+1, h, h)
	}
	s := PrepareDiff(p.String(), 44)
	r := PrepareReview(s)
	if len(r.Hunks) != 1000 {
		t.Fatalf("hunks %d", len(r.Hunks))
	}
	h, ok := r.NavigateHunk(r.Hunks[0].ID, 999)
	if !ok || h.ID != r.Hunks[999].ID {
		t.Fatalf("nav %#v", h)
	}
	state := new(ViewState)
	if id, ok := r.FocusHunk(s, state, r.Hunks[0].ID, 999); !ok || id != h.ID || state.Caret.LineID != h.ID {
		t.Fatalf("virtual focus id=%d caret=%#v", id, state.Caret)
	}
	changed, ok := r.NavigateChangedLine(s, s.Lines[h.Header].ID, 1)
	if !ok || changed.Kind != Deletion {
		t.Fatalf("line nav %#v", changed)
	}
	q, err := r.ContextIntent(h.ID, 20, 20)
	if err != nil || q.Generation != 44 {
		t.Fatalf("context %#v %v", q, err)
	}
	if _, ok := r.ApplyContext(ContextExpansion{Generation: 43, HunkID: h.ID, Patch: ""}); ok {
		t.Fatal("accepted stale context")
	}
	c, err := r.AddComment(s, changed.ID, OldSide, "Please retain this.")
	if err != nil || c.ID == 0 {
		t.Fatalf("comment %#v %v", c, err)
	}
	stage, err := r.Stage([]string{"x.go"}, []uint64{h.ID})
	if err != nil {
		t.Fatal(err)
	}
	commit, err := r.Commit(stage, "Refine x")
	if err != nil || commit.Message != "Refine x" {
		t.Fatalf("commit %#v %v", commit, err)
	}
	// The intent owns its slices; callers cannot mutate a queued action.
	stage.Paths[0] = "changed"
	if commit.Stage.Paths[0] != "x.go" {
		t.Fatal("commit intent aliased stage input")
	}
}

func TestReviewGenerationNavigationEdgesAndValidation(t *testing.T) {
	s := PrepareDiff("@@ -1 +1 @@\n-old\n+new\n", 3)
	r := PrepareReview(s)
	if h, ok := r.NavigateHunk(0, 1); !ok || h.ID != r.Hunks[0].ID {
		t.Fatalf("initial next %#v", h)
	}
	if h, ok := r.NavigateHunk(0, -1); !ok || h.ID != r.Hunks[0].ID {
		t.Fatalf("initial previous %#v", h)
	}
	stale := PrepareDiff("@@ -1 +1 @@\n-a\n+b\n", 4)
	if _, ok := r.NavigateChangedLine(stale, 0, 1); ok {
		t.Fatal("navigated stale snapshot")
	}
	add := s.Lines[2]
	if _, err := r.AddComment(s, add.ID, OldSide, "wrong side"); err == nil {
		t.Fatal("accepted unavailable old side")
	}
	if _, err := r.AddComment(s, add.ID, ReviewSide(9), "bad side"); err == nil {
		t.Fatal("accepted invalid side")
	}
	if _, err := r.ContextIntent(r.Hunks[0].ID, int(^uint(0)>>1), 1); err == nil {
		t.Fatal("accepted overflowing context")
	}
}

func TestReviewIntentCopiesInputs(t *testing.T) {
	s := PrepareDiff("@@ -1 +1 @@\n-old\n+new\n", 1)
	r := PrepareReview(s)
	paths := []string{"x"}
	ids := []uint64{r.Hunks[0].ID}
	stage, err := r.Stage(paths, ids)
	if err != nil {
		t.Fatal(err)
	}
	paths[0] = "mutated"
	ids[0] = 0
	if stage.Paths[0] != "x" || stage.HunkIDs[0] != r.Hunks[0].ID {
		t.Fatalf("aliased intent %#v", stage)
	}
}
