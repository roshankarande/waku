package daemon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("daemon actor group is closed")
var ErrActorLimit = errors.New("daemon session actor limit exceeded")

type Command struct {
	SessionID string
	Value     any
}
type Result struct {
	Value any
	Err   error
}
type Handler func(context.Context, Command) (any, error)
type work struct {
	ctx     context.Context
	command Command
	result  chan Result
}
type actor struct {
	sessionID string
	mailbox   chan work
	stop      chan struct{}
	idle      chan struct{}
	refs      int
	running   bool
	retired   bool
}

type ActorGroup struct {
	mu             sync.Mutex
	startGate      sync.RWMutex
	startsClosed   bool
	startClosing   atomic.Bool
	handler        Handler
	mailboxSize    int
	maxActors      int
	actors         map[string]*actor
	ctx            context.Context
	cancel         context.CancelFunc
	closed         bool
	admissions     sync.WaitGroup
	stop           chan struct{}
	stopOnce       sync.Once
	startCloseOnce sync.Once
	startsDone     chan struct{}
	wg             sync.WaitGroup
}

func NewActorGroup(mailboxSize int, handler Handler) *ActorGroup {
	return NewActorGroupWithLimit(mailboxSize, 1024, handler)
}

func NewActorGroupWithLimit(mailboxSize, maxActors int, handler Handler) *ActorGroup {
	if mailboxSize < 1 {
		mailboxSize = 1
	}
	if maxActors < 1 {
		maxActors = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ActorGroup{handler: handler, mailboxSize: mailboxSize, maxActors: maxActors, actors: make(map[string]*actor), ctx: ctx, cancel: cancel, stop: make(chan struct{}), startsDone: make(chan struct{})}
}

// Submit never holds the group mutex while waiting for mailbox capacity.
func (g *ActorGroup) Submit(ctx context.Context, command Command) <-chan Result {
	result := make(chan Result, 1)
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		result <- Result{Err: ErrClosed}
		return result
	}
	g.admissions.Add(1)
	a := g.actors[command.SessionID]
	if a == nil {
		if len(g.actors) >= g.maxActors {
			g.mu.Unlock()
			g.admissions.Done()
			result <- Result{Err: ErrActorLimit}
			return result
		}
		a = &actor{sessionID: command.SessionID, mailbox: make(chan work, g.mailboxSize), stop: make(chan struct{}), idle: make(chan struct{}, 1)}
		g.actors[command.SessionID] = a
		g.wg.Add(1)
		go g.run(a)
	}
	a.refs++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		a.refs--
		g.mu.Unlock()
		select {
		case a.idle <- struct{}{}:
		default:
		}
		g.admissions.Done()
	}()
	select {
	case a.mailbox <- work{ctx: ctx, command: command, result: result}:
	case <-ctx.Done():
		result <- Result{Err: ctx.Err()}
	case <-g.ctx.Done():
		result <- Result{Err: ErrClosed}
	}
	return result
}

func (g *ActorGroup) run(a *actor) {
	defer g.wg.Done()
	for {
		if g.ctx.Err() != nil {
			<-g.stop
			g.drainActor(a)
			return
		}
		select {
		case w := <-a.mailbox:
			g.mu.Lock()
			if a.retired {
				g.mu.Unlock()
				w.result <- Result{Err: ErrClosed}
				return
			}
			a.running = true
			g.mu.Unlock()
			if err := w.ctx.Err(); err != nil {
				retired := g.finishActorWork(w.command.SessionID, a)
				w.result <- Result{Err: err}
				if retired {
					return
				}
				continue
			}
			// Linearize handler start against Shutdown. Once Shutdown marks the
			// group closed, no queued handler is invoked and therefore cannot
			// perform side effects even if its own context is ignored.
			// A read lease spans the complete handler call. Shutdown queues an
			// exclusive lease, so a handler is either wholly before the shutdown
			// boundary or rejected after it; there is no check-to-call window.
			g.startGate.RLock()
			if g.startClosing.Load() || g.startsClosed {
				g.startGate.RUnlock()
				retired := g.finishActorWork(w.command.SessionID, a)
				w.result <- Result{Err: ErrClosed}
				if retired {
					return
				}
				continue
			}
			handlerCtx, cancel := context.WithCancel(w.ctx)
			stop := context.AfterFunc(g.ctx, cancel)
			value, err := g.handler(handlerCtx, w.command)
			stop()
			cancel()
			g.startGate.RUnlock()
			retired := g.finishActorWork(w.command.SessionID, a)
			w.result <- Result{Value: value, Err: err}
			if retired {
				return
			}
		case <-g.stop:
			g.drainActor(a)
			return
		case <-a.stop:
			return
		case <-a.idle:
			g.mu.Lock()
			g.retireActorLocked(a.sessionID, a)
			retired := a.retired
			g.mu.Unlock()
			if retired {
				return
			}
		}
	}
}

func (g *ActorGroup) finishActorWork(sessionID string, a *actor) bool {
	g.mu.Lock()
	a.running = false
	g.retireActorLocked(sessionID, a)
	retired := a.retired
	g.mu.Unlock()
	return retired
}

func (g *ActorGroup) retireActorLocked(sessionID string, a *actor) {
	if a.retired || a.running || a.refs != 0 || len(a.mailbox) != 0 || g.actors[sessionID] != a {
		return
	}
	a.retired = true
	delete(g.actors, sessionID)
	close(a.stop)
}

func (g *ActorGroup) ActorCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.actors)
}

func (g *ActorGroup) drainActor(a *actor) {
	for {
		select {
		case w := <-a.mailbox:
			w.result <- Result{Err: ErrClosed}
		default:
			return
		}
	}
}

// Shutdown stops admission and waits only as long as ctx permits. Handlers
// receive their original contexts; provider handlers must honor cancellation.
func (g *ActorGroup) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		// Publish the start barrier synchronously while admission is closed.
		// The exclusive gate may wait for an already-running handler, but no
		// actor acquiring a read lease after this point can invoke the handler.
		g.startClosing.Store(true)
		g.cancel()
	}
	g.mu.Unlock()
	g.startCloseOnce.Do(func() {
		if g.startGate.TryLock() {
			g.startsClosed = true
			g.startGate.Unlock()
			close(g.startsDone)
			return
		}
		go func() {
			g.startGate.Lock()
			g.startsClosed = true
			g.startGate.Unlock()
			close(g.startsDone)
		}()
	})
	g.stopOnce.Do(func() { go func() { g.admissions.Wait(); close(g.stop) }() })
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		<-g.startsDone
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *ActorGroup) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = g.Shutdown(ctx)
}
