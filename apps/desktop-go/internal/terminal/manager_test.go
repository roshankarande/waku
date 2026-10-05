package terminal

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakePTY struct {
	mu      sync.Mutex
	writes  [][]byte
	resizes [][2]uint16
	closes  int
}

func (p *fakePTY) Write(_ context.Context, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writes = append(p.writes, append([]byte(nil), data...))
	return nil
}
func (p *fakePTY) Resize(_ context.Context, cols, rows uint16) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resizes = append(p.resizes, [2]uint16{cols, rows})
	return nil
}
func (p *fakePTY) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closes++
	return nil
}

type fakeBackend struct {
	mu       sync.Mutex
	pty      *fakePTY
	emit     func(BackendEvent)
	cwd      string
	colsRows [2]uint16
	early    []BackendEvent
	err      error
}

func (b *fakeBackend) Open(_ context.Context, cwd string, cols, rows uint16, emit func(BackendEvent)) (PTY, error) {
	b.mu.Lock()
	b.cwd, b.colsRows, b.emit = cwd, [2]uint16{cols, rows}, emit
	pty, early, err := b.pty, append([]BackendEvent(nil), b.early...), b.err
	b.mu.Unlock()
	for _, event := range early {
		emit(event)
	}
	if err != nil {
		return nil, err
	}
	return pty, nil
}

func managerLimits() ManagerLimits {
	return ManagerLimits{MaxTerminals: 2, MaxQueuedEvents: 4, MaxQueuedBytes: 16, MaxEventsPerTerminal: 3, MaxBytesPerTerminal: 12, MaxInputBytes: 4}
}

func TestManagerLifecycleOrderOwnershipAndLimits(t *testing.T) {
	backend := &fakeBackend{pty: &fakePTY{}, early: []BackendEvent{{Data: []byte("early")}}}
	events := make(chan BackendEvent, 8)
	manager, err := NewManager(backend, func(_ Owner, event BackendEvent) { events <- event }, managerLimits())
	if err != nil {
		t.Fatal(err)
	}
	owner := Owner{SessionID: "session", RuntimeID: "runtime"}
	if err := manager.Open(context.Background(), owner, "/daemon/workspace", 120, 40); err != nil {
		t.Fatal(err)
	}
	backend.emit(BackendEvent{Data: []byte("later")})
	backend.emit(BackendEvent{Exit: true})
	for index, expected := range []string{"early", "later"} {
		select {
		case event := <-events:
			if string(event.Data) != expected {
				t.Fatalf("event %d=%q", index, event.Data)
			}
		case <-time.After(time.Second):
			t.Fatal("terminal event order timed out")
		}
	}
	if event := <-events; !event.Exit {
		t.Fatalf("final event=%+v", event)
	}
	if err := manager.Write(context.Background(), owner, []byte("late")); !errors.Is(err, ErrTerminalNotFound) {
		t.Fatalf("write after exit=%v", err)
	}
	backend.mu.Lock()
	cwd, size := backend.cwd, backend.colsRows
	backend.mu.Unlock()
	if cwd != "/daemon/workspace" || size != [2]uint16{120, 40} {
		t.Fatalf("host open cwd=%q size=%v", cwd, size)
	}
}

func TestManagerSerializesControlsAndEnforcesOwner(t *testing.T) {
	pty := &fakePTY{}
	backend := &fakeBackend{pty: pty}
	manager, _ := NewManager(backend, func(Owner, BackendEvent) {}, managerLimits())
	owner := Owner{SessionID: "s", RuntimeID: "r"}
	if err := manager.Open(context.Background(), owner, "/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	wrong := Owner{SessionID: "s", RuntimeID: "other"}
	if err := manager.Write(context.Background(), wrong, []byte("x")); !errors.Is(err, ErrTerminalOwnership) {
		t.Fatalf("cross-runtime write=%v", err)
	}
	if err := manager.Write(context.Background(), owner, []byte("12345")); !errors.Is(err, ErrTerminalOverload) {
		t.Fatalf("oversized write=%v", err)
	}
	if err := manager.Write(context.Background(), owner, []byte("in")); err != nil {
		t.Fatal(err)
	}
	if err := manager.Resize(context.Background(), owner, 100, 30); err != nil {
		t.Fatal(err)
	}
	if err := manager.CloseTerminal(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	pty.mu.Lock()
	defer pty.mu.Unlock()
	if len(pty.writes) != 1 || string(pty.writes[0]) != "in" || !reflect.DeepEqual(pty.resizes, [][2]uint16{{100, 30}}) || pty.closes != 1 {
		t.Fatalf("pty=%+v", pty)
	}
}

func TestManagerDisconnectsOnlyOverflowingTerminalAfterOrderedDrain(t *testing.T) {
	pty := &fakePTY{}
	backend := &fakeBackend{pty: pty}
	entered, release := make(chan struct{}), make(chan struct{})
	events := make(chan BackendEvent, 8)
	manager, _ := NewManager(backend, func(_ Owner, event BackendEvent) {
		if string(event.Data) == "a" {
			close(entered)
			<-release
		}
		events <- event
	}, ManagerLimits{MaxTerminals: 1, MaxQueuedEvents: 2, MaxQueuedBytes: 2, MaxEventsPerTerminal: 2, MaxBytesPerTerminal: 2, MaxInputBytes: 4})
	owner := Owner{SessionID: "s", RuntimeID: "r"}
	if err := manager.Open(context.Background(), owner, "/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	backend.emit(BackendEvent{Data: []byte("a")})
	<-entered
	backend.emit(BackendEvent{Data: []byte("b")})
	backend.emit(BackendEvent{Data: []byte("c")})
	close(release)
	for _, expected := range []string{"a", "b"} {
		if event := <-events; string(event.Data) != expected {
			t.Fatalf("ordered drain=%+v want=%q", event, expected)
		}
	}
	if event := <-events; !errors.Is(event.Err, ErrTerminalOverload) {
		t.Fatalf("overflow final=%+v", event)
	}
	deadline := time.Now().Add(time.Second)
	for {
		pty.mu.Lock()
		closed := pty.closes
		pty.mu.Unlock()
		if closed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("overflowed PTY was not closed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestManagerGatesUnsupportedBackendAndBoundsTerminalCount(t *testing.T) {
	manager, _ := NewManager(UnsupportedBackend{}, func(Owner, BackendEvent) {}, managerLimits())
	if err := manager.Open(context.Background(), Owner{"s", "r"}, "/remote", 80, 24); !errors.Is(err, ErrBackendUnsupported) {
		t.Fatalf("unsupported backend=%v", err)
	}
	backend := &fakeBackend{pty: &fakePTY{}}
	one := managerLimits()
	one.MaxTerminals = 1
	manager, _ = NewManager(backend, func(Owner, BackendEvent) {}, one)
	if err := manager.Open(context.Background(), Owner{"s1", "r1"}, "/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	if err := manager.Open(context.Background(), Owner{"s2", "r2"}, "/remote", 80, 24); !errors.Is(err, ErrTerminalOverload) {
		t.Fatalf("terminal count bound=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerReapsHandleAfterSynchronousOpenOverflow(t *testing.T) {
	pty := &fakePTY{}
	backend := &fakeBackend{pty: pty, early: []BackendEvent{{Data: []byte("ab")}}}
	limits := managerLimits()
	limits.MaxBytesPerTerminal = 1
	manager, _ := NewManager(backend, func(Owner, BackendEvent) {}, limits)
	if err := manager.Open(context.Background(), Owner{"s", "r"}, "/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		pty.mu.Lock()
		closed := pty.closes
		pty.mu.Unlock()
		if closed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PTY returned after synchronous open overflow was not reaped")
		}
		time.Sleep(time.Millisecond)
	}
}

type blockedPTY struct{ entered chan struct{} }

func (p *blockedPTY) Write(context.Context, []byte) error          { close(p.entered); select {} }
func (p *blockedPTY) Resize(context.Context, uint16, uint16) error { select {} }
func (p *blockedPTY) Close(context.Context) error                  { select {} }

func TestManagerBoundsMisbehavingControlAndShutdown(t *testing.T) {
	pty := &blockedPTY{entered: make(chan struct{})}
	backend := &blockingBackend{pty: pty}
	manager, _ := NewManager(backend, func(Owner, BackendEvent) {}, managerLimits())
	owner := Owner{"s", "r"}
	if err := manager.Open(context.Background(), owner, "/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWrite()
	if err := manager.Write(writeCtx, owner, []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked write=%v", err)
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelShutdown()
	if err := manager.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked shutdown=%v", err)
	}
}

type blockingBackend struct{ pty PTY }

func (b *blockingBackend) Open(context.Context, string, uint16, uint16, func(BackendEvent)) (PTY, error) {
	return b.pty, nil
}

func TestManagerFinalThenOpenErrorDoesNotHangOrLeakBudget(t *testing.T) {
	backend := &fakeBackend{pty: &fakePTY{}, early: []BackendEvent{{Err: errors.New("failed")}}, err: errors.New("open failed")}
	manager, _ := NewManager(backend, func(Owner, BackendEvent) {}, managerLimits())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Open(ctx, Owner{"s", "r"}, "/remote", 80, 24); err == nil || err.Error() != "open failed" {
		t.Fatalf("open=%v", err)
	}
	manager.mu.Lock()
	events, bytes := manager.events, manager.bytes
	manager.mu.Unlock()
	if events != 0 || bytes != 0 {
		t.Fatalf("failed open leaked queue accounting: %d/%d", events, bytes)
	}
}

type gatedOpenBackend struct {
	entered chan struct{}
	release chan struct{}
	pty     PTY
}

func (b *gatedOpenBackend) Open(context.Context, string, uint16, uint16, func(BackendEvent)) (PTY, error) {
	close(b.entered)
	<-b.release
	return b.pty, nil
}

func TestManagerShutdownWaitsForProvisionalOpenAndReap(t *testing.T) {
	pty := &fakePTY{}
	backend := &gatedOpenBackend{entered: make(chan struct{}), release: make(chan struct{}), pty: pty}
	limits := managerLimits()
	limits.OperationTimeout = time.Second
	manager, _ := NewManager(backend, func(Owner, BackendEvent) {}, limits)
	opened := make(chan error, 1)
	go func() { opened <- manager.Open(context.Background(), Owner{"s", "r"}, "/remote", 80, 24) }()
	<-backend.entered
	shutdown := make(chan error, 1)
	go func() { shutdown <- manager.Shutdown(context.Background()) }()
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown crossed provisional open: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(backend.release)
	if err := <-opened; err == nil {
		t.Fatal("open succeeded after shutdown boundary")
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	pty.mu.Lock()
	defer pty.mu.Unlock()
	if pty.closes != 1 {
		t.Fatalf("provisional PTY closes=%d", pty.closes)
	}
}

func TestManagerBoundsFinalErrorPayloadAndUncooperativeFinalClose(t *testing.T) {
	pty := &blockedPTY{entered: make(chan struct{})}
	backend := &fakeEmittingBackend{pty: pty}
	limits := managerLimits()
	limits.MaxBytesPerTerminal = 5
	limits.MaxQueuedBytes = 5
	limits.OperationTimeout = 10 * time.Millisecond
	events := make(chan BackendEvent, 1)
	manager, _ := NewManager(backend, func(_ Owner, event BackendEvent) { events <- event }, limits)
	if err := manager.Open(context.Background(), Owner{"s", "r"}, "/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	backend.emit(BackendEvent{Err: errors.New("an arbitrarily large backend error")})
	event := <-events
	if event.Err == nil || len(event.Err.Error()) > 5 {
		t.Fatalf("unbounded final error=%q", event.Err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	if err := manager.Shutdown(ctx); err == nil {
		t.Fatal("shutdown reported success with an unreaped PTY")
	}
	if time.Since(started) > 200*time.Millisecond {
		t.Fatal("uncooperative PTY pinned terminal shutdown")
	}
}

type fakeEmittingBackend struct {
	pty  PTY
	emit func(BackendEvent)
}

func (b *fakeEmittingBackend) Open(_ context.Context, _ string, _, _ uint16, emit func(BackendEvent)) (PTY, error) {
	b.emit = emit
	return b.pty, nil
}
