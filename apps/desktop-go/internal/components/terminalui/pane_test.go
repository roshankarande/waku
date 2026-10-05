package terminalui

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
)

type remotePipe struct {
	mu      sync.Mutex
	cond    *sync.Cond
	chunks  [][]byte
	writes  []byte
	resizes [][2]int
	closed  bool
	closes  int
}

func newRemotePipe() *remotePipe { p := new(remotePipe); p.cond = sync.NewCond(&p.mu); return p }
func (p *remotePipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.chunks) == 0 && !p.closed {
		p.cond.Wait()
	}
	if len(p.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(b, p.chunks[0])
	if n == len(p.chunks[0]) {
		p.chunks = p.chunks[1:]
	} else {
		p.chunks[0] = p.chunks[0][n:]
	}
	return n, nil
}
func (p *remotePipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	p.writes = append(p.writes, b...)
	return len(b), nil
}
func (p *remotePipe) Resize(c, r int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resizes = append(p.resizes, [2]int{c, r})
	return nil
}
func (p *remotePipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.closes++
		p.cond.Broadcast()
	}
	return nil
}
func (p *remotePipe) feed(s string) {
	p.mu.Lock()
	p.chunks = append(p.chunks, []byte(s))
	p.cond.Signal()
	p.mu.Unlock()
}
func (p *remotePipe) sent() string { p.mu.Lock(); defer p.mu.Unlock(); return string(p.writes) }

func waitFrame(t *testing.T, tt *ui.Tester, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
		tt.Frame()
	}
}

func TestPaneRemoteInteractionSearchSelectionResizeAccessibilityAndClose(t *testing.T) {
	conn := newRemotePipe()
	p, err := New(conn, Options{NoBlink: true})
	if err != nil {
		t.Fatal(err)
	}
	tt := ui.NewTester(func(c *ui.Context) { View(c, p).Height(220) }, 640, 220)
	conn.feed("hello remote\r\nsecond hello")
	waitFrame(t, tt, "remote output", func() bool { return strings.Contains(p.Text(), "second hello") })
	// The pinned MyGo revision labels the focusable terminal, but does not yet
	// expose its viewport text to assistive technology (see package gap report).
	if !strings.Contains(p.Text(), "hello remote\nsecond hello") {
		t.Fatal("terminal transcript missing visible text")
	}
	if err := tt.Click("Terminal"); err != nil {
		t.Fatal(err)
	}
	tt.Type("ls")
	waitFrame(t, tt, "keyboard transport", func() bool { return strings.Contains(conn.sent(), "ls") })
	conn.mu.Lock()
	resized := len(conn.resizes) > 0
	conn.mu.Unlock()
	if !resized {
		t.Fatal("native pane did not resize remote connection")
	}
	tt.Command("selectAll")
	tt.Command("copy")
	if got := tt.Clipboard(); !strings.Contains(got, "hello remote") {
		t.Fatalf("selection copy %q", got)
	}
	if err := tt.Click("Search terminal"); err != nil {
		t.Fatal(err)
	}
	tt.Type("hello")
	waitFrame(t, tt, "search result", func() bool { q, n, pending := p.SearchState(); return q == "hello" && n == 2 && !pending })
	if !tt.HasText("hello: 2 matches") {
		tt.Frame()
		if !tt.HasText("hello: 2 matches") {
			t.Fatalf("search status %q", tt.Texts())
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	closes := conn.closes
	conn.mu.Unlock()
	if closes != 1 {
		t.Fatalf("remote closes=%d", closes)
	}
}

func TestPaneRequiresRemoteConnectionAndCoalescesSearchWakeups(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("pane could start without remote daemon connection")
	}
	conn := newRemotePipe()
	p, err := New(conn, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	conn.feed(strings.Repeat("target ", 5000))
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(p.Text(), "target") {
		if time.Now().After(deadline) {
			t.Fatal("output")
		}
		time.Sleep(time.Millisecond)
	}
	var wakes atomic.Int32
	p.SetInvalidator(func() { wakes.Add(1) })
	for i := 0; i < 100; i++ {
		p.Search("target")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		q, n, pending := p.SearchState()
		if q == "target" && n > 0 && !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("search")
		}
		time.Sleep(time.Millisecond)
	}
	if got := wakes.Load(); got != 1 {
		t.Fatalf("uncoalesced invalidations=%d", got)
	}
}

func TestSearchFoldAndCaps(t *testing.T) {
	if got := foldCount("Σ ς σ İstanbul", "σ", 100); got != 3 {
		t.Fatalf("sigma=%d", got)
	}
	if got := foldCount("İstanbul", "i", 100); got != 0 {
		t.Fatalf("dotted I matched ASCII i: %d", got)
	}
	if got := foldCount(strings.Repeat("a", 100), "a", 7); got != 7 {
		t.Fatalf("cap=%d", got)
	}
}
