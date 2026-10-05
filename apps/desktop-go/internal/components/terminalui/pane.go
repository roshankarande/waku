// Package terminalui contains Waku's native remote-terminal pane.
package terminalui

import (
	"errors"
	"io"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	myterminal "github.com/egoist/mygo/plugins/terminal"
	"github.com/egoist/mygo/ui"
)

const (
	MaxSearchBytes   = 2 << 20
	MaxSearchMatches = 10_000
	// MyGo does not currently expose terminal cell search highlights or
	// scroll-to-match. Count/search is real; navigation is capability-gated.
	SearchNavigation = false
)

// RemoteConn is the daemon terminal transport contract. Requiring an injected
// connection makes it impossible for this component to start a local shell.
type RemoteConn interface {
	io.ReadWriteCloser
	Resize(cols, rows int) error
}

type Options struct {
	Scrollback int
	NoBlink    bool
}

type searchRequest struct {
	generation uint64
	query      string
}
type searchResult struct {
	generation uint64
	query      string
	matches    int
}

type Pane struct {
	term        *myterminal.Terminal
	conn        RemoteConn
	query       string
	mu          sync.Mutex
	result      searchResult
	ready       bool
	invalidate  func()
	wakePending bool
	closed      bool
	requests    chan searchRequest
	done        chan struct{}
	wg          sync.WaitGroup
	generation  uint64
	closeOnce   sync.Once
}

func New(conn RemoteConn, opts Options) (*Pane, error) {
	if conn == nil {
		return nil, errors.New("terminalui: remote daemon connection is required")
	}
	scrollback := opts.Scrollback
	if scrollback == 0 {
		scrollback = MaxSearchBytes
	}
	if scrollback > MaxSearchBytes {
		scrollback = MaxSearchBytes
	}
	t, err := myterminal.New(myterminal.Options{Conn: conn, Scrollback: scrollback, NoBlink: opts.NoBlink})
	if err != nil {
		return nil, err
	}
	p := &Pane{term: t, conn: conn, requests: make(chan searchRequest, 1), done: make(chan struct{})}
	p.wg.Add(1)
	go p.searchLoop()
	return p, nil
}

func (p *Pane) Terminal() *myterminal.Terminal { return p.term }
func (p *Pane) Text() string                   { return p.term.Text() }
func (p *Pane) SearchState() (query string, matches int, pending bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.query, p.result.matches, p.result.generation != p.generation
}

func (p *Pane) SetInvalidator(fn func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invalidate = fn
	if fn != nil && p.ready && !p.wakePending && !p.closed {
		p.wakePending = true
		fn()
	}
}
func (p *Pane) Search(query string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.generation++
	r := searchRequest{p.generation, query}
	p.query = query
	p.ready = false
	p.mu.Unlock()
	select {
	case p.requests <- r:
	default:
		select {
		case <-p.requests:
		default:
			{
			}
		}
		select {
		case p.requests <- r:
		case <-p.done:
		}
	}
}
func (p *Pane) searchLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		case r := <-p.requests:
			text := p.term.Text()
			if len(text) > MaxSearchBytes {
				text = text[len(text)-MaxSearchBytes:]
				for len(text) > 0 && !utf8.ValidString(text) {
					text = text[1:]
				}
			}
			matches := foldCount(text, r.query, MaxSearchMatches)
			p.mu.Lock()
			if !p.closed && r.generation == p.generation {
				p.result = searchResult{r.generation, r.query, matches}
				p.ready = true
				if p.invalidate != nil && !p.wakePending {
					p.wakePending = true
					p.invalidate()
				}
			}
			p.mu.Unlock()
		}
	}
}
func (p *Pane) applySearch() {
	p.mu.Lock()
	if p.ready {
		p.ready = false
		p.wakePending = false
	}
	p.mu.Unlock()
}

func (p *Pane) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.invalidate = nil
		p.mu.Unlock()
		close(p.done)
		err = p.term.Close()
		p.wg.Wait()
	})
	return err
}

// View composes Waku search controls with MyGo's native terminal. The native
// element owns selection/copy, keyboard/IME, focus, resize and accessibility.
func View(c *ui.Context, p *Pane) *ui.Element {
	p.SetInvalidator(c.Invalidate)
	p.applySearch()
	root := ui.Column(c).MinHeight(0)
	root.Children(func() {
		ui.Row(c).Gap(6).Padding(4, 8).Children(func() {
			if ui.TextInput(c, &p.query).Label("Search terminal").Placeholder("Search terminal").Grow(1).Changed() {
				p.Search(p.query)
			}
			q, n, pending := p.SearchState()
			label := "No terminal search"
			if q != "" {
				label = strings.TrimSpace(q) + ": " + itoa(n) + " matches"
			}
			if pending {
				label = "Terminal search pending"
			}
			ui.Text(c, label)
		})
		myterminal.View(c, p.term).Grow(1).MinHeight(0)
	})
	return root
}

func foldCount(text, query string, cap int) int {
	if query == "" {
		return 0
	}
	tr := []rune(text)
	qr := []rune(query)
	n := 0
	for i := 0; i+len(qr) <= len(tr); i++ {
		ok := true
		for j := range qr {
			if !foldEqual(tr[i+j], qr[j]) {
				ok = false
				break
			}
		}
		if ok {
			n++
			if n >= cap {
				return n
			}
		}
	}
	return n
}
func foldEqual(a, b rune) bool {
	if a == b {
		return true
	}
	for r := unicode.SimpleFold(a); r != a; r = unicode.SimpleFold(r) {
		if r == b {
			return true
		}
	}
	return false
}
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
