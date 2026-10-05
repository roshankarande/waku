package code

import (
	"context"
	"errors"
	"sync"
	"unicode/utf8"
)

const (
	MaxEditableBytes     = 16 << 20
	MaxInputContextRunes = 512
)

var (
	ErrFileTooLarge         = errors.New("code: editable file exceeds limit")
	ErrRevisionConflict     = errors.New("code: file revision conflict")
	ErrStaleGeneration      = errors.New("code: stale generation")
	ErrFileQueueFull        = errors.New("code: file operation queue full")
	ErrFileWorkerClosed     = errors.New("code: file worker closed")
	ErrFileOperationPending = errors.New("code: file operation pending")
)

// EditorSession connects prepared editor state to its asynchronous repository
// boundary. BeginLoad/BeginSave are called by controllers; Poll is nonblocking
// and safe to call from EditorView.
type EditorSession struct {
	Model       EditorModel
	Worker      *FileWorker
	LastError   error
	mu          sync.Mutex
	pending     bool
	ready       *FileResult
	saving      bool
	invalidate  func()
	wakePending bool
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	wg          sync.WaitGroup
}

// SetInvalidator installs the host/window callback. MyGo integrations pass
// Context.Invalidate; other hosts may schedule their equivalent UI turn.
func (s *EditorSession) SetInvalidator(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidate = fn
	if fn != nil && s.ready != nil && !s.wakePending && !s.closed {
		s.wakePending = true
		fn()
	}
}

// Close cancels this session's outstanding operation and guarantees that its
// invalidator is not called after Close returns. It does not close a possibly
// shared FileWorker.
func (s *EditorSession) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.closed = true
	s.invalidate = nil
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *EditorSession) initLocked() {
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(context.Background())
	}
}

func (s *EditorSession) BeginLoad(ctx context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Worker == nil {
		return errors.New("code: no file worker")
	}
	if s.closed {
		return ErrFileWorkerClosed
	}
	if s.pending {
		return ErrFileOperationPending
	}
	s.initLocked()
	opctx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(ctx, cancel)
	ch := s.Worker.Load(opctx, path, s.Model.Generation)
	s.pending = true
	s.saving = false
	s.LastError = nil
	s.watchLocked(ch, stop, cancel)
	return nil
}
func (s *EditorSession) BeginSave(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Worker == nil {
		return errors.New("code: no file worker")
	}
	if s.closed {
		return ErrFileWorkerClosed
	}
	if s.pending {
		return ErrFileOperationPending
	}
	req, g := s.Model.SaveRequest()
	s.initLocked()
	opctx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(ctx, cancel)
	ch := s.Worker.Save(opctx, req, g)
	s.pending = true
	s.saving = true
	s.LastError = nil
	s.watchLocked(ch, stop, cancel)
	return nil
}
func (s *EditorSession) watchLocked(ch <-chan FileResult, stop func() bool, cancel context.CancelFunc) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer stop()
		defer cancel()
		r, ok := <-ch
		s.mu.Lock()
		defer s.mu.Unlock()
		s.pending = false
		if !ok || s.closed {
			return
		}
		s.ready = &r
		if s.invalidate != nil && !s.wakePending {
			s.wakePending = true
			s.invalidate()
		}
	}()
}
func (s *EditorSession) Poll() bool {
	s.mu.Lock()
	if s.ready == nil {
		s.mu.Unlock()
		return false
	}
	r := *s.ready
	s.ready = nil
	s.wakePending = false
	saving := s.saving
	s.mu.Unlock()
	s.LastError = r.Err
	if r.Err == nil {
		applied := false
		if saving {
			applied = s.Model.ApplySave(r)
		} else {
			applied = s.Model.ApplyLoad(r)
		}
		if !applied {
			s.LastError = ErrStaleGeneration
		}
	}
	return true
}

// FileRepository is implemented by the workspace layer. Revisions are opaque;
// Save must atomically reject a request whose ExpectedRevision is no longer
// current. Components never access a filesystem directly.
type FileRepository interface {
	Load(context.Context, string) (LoadedFile, error)
	Save(context.Context, SaveFileRequest) (SavedFile, error)
}

type LoadedFile struct{ Path, Revision, Text string }
type SaveFileRequest struct{ Path, ExpectedRevision, Text string }
type SavedFile struct{ Revision string }

type FileResult struct {
	Generation uint64
	File       LoadedFile
	Saved      *SavedFile
	Err        error
}

// FileWorker runs repository operations away from view builds. Every result is
// tagged with the caller's generation so stale completion is unambiguous.
type FileWorker struct {
	repo   FileRepository
	jobs   chan fileJob
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

type fileJob struct {
	ctx        context.Context
	generation uint64
	path       string
	save       *SaveFileRequest
	out        chan FileResult
}

func NewFileWorker(repo FileRepository, parallel int) *FileWorker {
	if parallel < 1 {
		parallel = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &FileWorker{repo: repo, jobs: make(chan fileJob, parallel*2), ctx: ctx, cancel: cancel}
	w.wg.Add(parallel)
	for range parallel {
		go w.run()
	}
	return w
}
func (w *FileWorker) Close() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		w.cancel()
		close(w.jobs)
	}
	w.mu.Unlock()
	w.wg.Wait()
}
func (w *FileWorker) Load(ctx context.Context, path string, generation uint64) <-chan FileResult {
	out := make(chan FileResult, 1)
	w.submit(fileJob{ctx: ctx, generation: generation, path: path, out: out})
	return out
}
func (w *FileWorker) Save(ctx context.Context, req SaveFileRequest, generation uint64) <-chan FileResult {
	out := make(chan FileResult, 1)
	if len(req.Text) > MaxEditableBytes {
		out <- FileResult{Generation: generation, Err: ErrFileTooLarge}
		close(out)
		return out
	}
	w.submit(fileJob{ctx: ctx, generation: generation, save: &req, out: out})
	return out
}
func (w *FileWorker) submit(j fileJob) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		j.out <- FileResult{Generation: j.generation, Err: ErrFileWorkerClosed}
		close(j.out)
		return
	}
	select {
	case w.jobs <- j:
	default:
		j.out <- FileResult{Generation: j.generation, Err: ErrFileQueueFull}
		close(j.out)
	}
}
func (w *FileWorker) run() {
	defer w.wg.Done()
	for j := range w.jobs {
		ctx, cancel := context.WithCancel(w.ctx)
		stop := context.AfterFunc(j.ctx, cancel)
		var r FileResult
		r.Generation = j.generation
		if err := ctx.Err(); err != nil {
			r.Err = err
		} else if j.save != nil {
			s, err := w.repo.Save(ctx, *j.save)
			r.Saved = &s
			r.Err = err
		} else {
			f, err := w.repo.Load(ctx, j.path)
			if err == nil && len(f.Text) > MaxEditableBytes {
				err = ErrFileTooLarge
				f.Text = ""
			}
			r.File = f
			r.Err = err
		}
		stop()
		cancel()
		j.out <- r
		close(j.out)
	}
}

// EditorModel is mutable UI state, but all I/O remains expressed as requests.
// Selection offsets are rune offsets to match native IME APIs.
type EditorModel struct {
	Generation            uint64
	Path, Revision, Text  string
	Anchor, Caret         int
	Dirty                 bool
	anchorByte, caretByte int
}

func (e *EditorModel) ApplyLoad(r FileResult) bool {
	if r.Err != nil || r.Generation != e.Generation || r.Saved != nil {
		return false
	}
	e.Path, e.Revision, e.Text = r.File.Path, r.File.Revision, r.File.Text
	e.Anchor, e.Caret, e.Dirty = 0, 0, false
	e.anchorByte, e.caretByte = 0, 0
	e.Generation++
	return true
}
func (e *EditorModel) SaveRequest() (SaveFileRequest, uint64) {
	return SaveFileRequest{Path: e.Path, ExpectedRevision: e.Revision, Text: e.Text}, e.Generation
}
func (e *EditorModel) ApplySave(r FileResult) bool {
	if r.Err != nil || r.Saved == nil || r.Generation != e.Generation {
		return false
	}
	e.Revision, e.Dirty = r.Saved.Revision, false
	return true
}
func (e *EditorModel) Select(anchor, caret int) error {
	distance := anchor - caret
	if distance < 0 {
		distance = -distance
	}
	if anchor < 0 || caret < 0 || distance > 1024 {
		return errors.New("code: invalid selection")
	}
	ab, ok := runeByteOffset(e.Text, anchor)
	if !ok {
		return errors.New("code: invalid selection")
	}
	cb, ok := runeByteOffset(e.Text, caret)
	if !ok {
		return errors.New("code: invalid selection")
	}
	e.Anchor, e.Caret = anchor, caret
	e.anchorByte, e.caretByte = ab, cb
	return nil
}

type EditorInputContext struct {
	Generation       uint64
	Text             string
	Start, End, Base int
}

// InputContext returns a bounded surrounding-text window and an absolute rune
// base. A platform adapter can map it to MyGo's InputContext without copying an
// entire large file every frame.
func (e *EditorModel) InputContext() EditorInputContext {
	a, b := e.Anchor, e.Caret
	ab, bb := e.anchorByte, e.caretByte
	if a > b {
		a, b = b, a
		ab, bb = bb, ab
	}
	loByte, walked := walkRunesBack(e.Text, ab, MaxInputContextRunes)
	hiByte, _ := walkRunesForward(e.Text, bb, MaxInputContextRunes)
	base := a - walked
	return EditorInputContext{Generation: e.Generation, Text: e.Text[loByte:hiByte], Start: a - base, End: b - base, Base: base}
}

type EditorEdit struct {
	Generation uint64
	Start, End int
	Text       string
}

// ApplyNativeEdit is the IME/platform seam: provisional composition remains
// owned by MyGo, and only a committed replacement is applied here. The caller
// supplies absolute rune offsets from EditorInputContext.Base.
func (e *EditorModel) ApplyNativeEdit(generation uint64, start, end int, text string) error {
	return e.ApplyEdit(EditorEdit{Generation: generation, Start: start, End: end, Text: text})
}

func (e *EditorModel) ApplyEdit(x EditorEdit) error {
	if x.Generation != e.Generation {
		return ErrStaleGeneration
	}
	r := []rune(e.Text)
	if x.Start < 0 || x.End < x.Start || x.End > len(r) {
		return errors.New("code: invalid edit")
	}
	n := len(r) - x.End + x.Start + utf8.RuneCountInString(x.Text)
	if n > MaxEditableBytes || len(e.Text)-len(string(r[x.Start:x.End]))+len(x.Text) > MaxEditableBytes {
		return ErrFileTooLarge
	}
	r = append(append(append([]rune(nil), r[:x.Start]...), []rune(x.Text)...), r[x.End:]...)
	e.Text = string(r)
	e.Generation++
	e.Caret = x.Start + utf8.RuneCountInString(x.Text)
	e.Anchor = e.Caret
	e.anchorByte, _ = runeByteOffset(e.Text, e.Anchor)
	e.caretByte = e.anchorByte
	e.Dirty = true
	return nil
}

func runeByteOffset(s string, n int) (int, bool) {
	if n < 0 {
		return 0, false
	}
	if n == 0 {
		return 0, true
	}
	k := 0
	for i := range s {
		if k == n {
			return i, true
		}
		k++
	}
	if k == n {
		return len(s), true
	}
	return 0, false
}
func walkRunesBack(s string, at, count int) (int, int) {
	base := 0
	for i := 0; i < count && at > 0; i++ {
		_, n := utf8.DecodeLastRuneInString(s[:at])
		at -= n
		base++
	}
	return at, base
}
func walkRunesForward(s string, at, count int) (int, int) {
	nrun := 0
	for nrun < count && at < len(s) {
		_, n := utf8.DecodeRuneInString(s[at:])
		at += n
		nrun++
	}
	return at, nrun
}
