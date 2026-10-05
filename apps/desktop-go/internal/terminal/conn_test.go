package terminal

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeControl struct {
	mu       sync.Mutex
	ids      []string
	input    [][]byte
	resize   [][2]int
	closes   int
	closeErr error
}

func (f *fakeControl) Input(_ context.Context, id string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.input = append(f.input, append([]byte(nil), data...))
	return nil
}
func (f *fakeControl) Resize(_ context.Context, id string, cols, rows int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.resize = append(f.resize, [2]int{cols, rows})
	return nil
}
func (f *fakeControl) Close(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.closes++
	return f.closeErr
}

func TestConnBridgesDaemonPTYWithoutStartingProcess(t *testing.T) {
	control := &fakeControl{}
	conn, err := NewConn(context.Background(), "remote-terminal", control)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("typed")); err != nil {
		t.Fatal(err)
	}
	if err := conn.Resize(120, 40); err != nil {
		t.Fatal(err)
	}
	read := make(chan string, 1)
	go func() { buffer := make([]byte, 16); n, _ := conn.Read(buffer); read <- string(buffer[:n]) }()
	select {
	case <-read:
		t.Fatal("read returned before daemon output")
	case <-time.After(20 * time.Millisecond):
	}
	if !conn.FeedOutput([]byte("rendered")) {
		t.Fatal("output rejected")
	}
	select {
	case got := <-read:
		if got != "rendered" {
			t.Fatalf("read %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon output was not delivered")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.closes != 1 || !reflect.DeepEqual(control.resize, [][2]int{{120, 40}}) || string(control.input[0]) != "typed" {
		t.Fatalf("control = %#v", control)
	}
	for _, id := range control.ids {
		if id != "remote-terminal" {
			t.Fatalf("wrong terminal id %q", id)
		}
	}
}

func TestConnDrainsOutputBeforeExit(t *testing.T) {
	conn, _ := NewConn(context.Background(), "id", &fakeControl{})
	conn.FeedOutput([]byte("last"))
	terminalErr := errors.New("remote PTY failed")
	conn.FeedExit(terminalErr)
	buffer := make([]byte, 8)
	n, err := conn.Read(buffer)
	if err != nil || string(buffer[:n]) != "last" {
		t.Fatalf("first read = %q, %v", buffer[:n], err)
	}
	n, err = conn.Read(buffer)
	if n != 0 || !errors.Is(err, terminalErr) {
		t.Fatalf("exit read = %d, %v", n, err)
	}
	if _, err := conn.Write([]byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late write = %v", err)
	}
}

func TestConnRejectsInvalidRemoteConfiguration(t *testing.T) {
	if _, err := NewConn(context.Background(), "", &fakeControl{}); err == nil {
		t.Fatal("accepted empty remote id")
	}
	if _, err := NewConn(context.Background(), "id", nil); err == nil {
		t.Fatal("accepted nil daemon control")
	}
}

func TestConnDisconnectsOnBoundedOutputBackpressure(t *testing.T) {
	control := &fakeControl{}
	conn, err := NewConnWithLimits(context.Background(), "id", control, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !conn.FeedOutput([]byte("abc")) {
		t.Fatal("first bounded chunk rejected")
	}
	if conn.FeedOutput([]byte("d")) {
		t.Fatal("overflowing chunk accepted")
	}
	buffer := make([]byte, 8)
	n, err := conn.Read(buffer)
	if err != nil || string(buffer[:n]) != "abc" {
		t.Fatalf("drain = %q, %v", buffer[:n], err)
	}
	if _, err := conn.Read(buffer); !errors.Is(err, ErrOutputBackpressure) {
		t.Fatalf("overflow read = %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.closes != 1 {
		t.Fatalf("remote close count = %d", control.closes)
	}
}

func TestConnContextCancellationWakesReadAndCloseErrorIsRetained(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	closeErr := errors.New("close failed")
	control := &fakeControl{closeErr: closeErr}
	conn, _ := NewConn(ctx, "id", control)
	read := make(chan error, 1)
	go func() { _, err := conn.Read(make([]byte, 1)); read <- err }()
	cancel()
	select {
	case err := <-read:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled context did not wake read")
	}
	if err := conn.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("first close = %v", err)
	}
	if err := conn.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("second close lost error: %v", err)
	}
}

type overlapControl struct {
	active     atomic.Int32
	overlapped atomic.Bool
}

func (c *overlapControl) operation() {
	if c.active.Add(1) != 1 {
		c.overlapped.Store(true)
	}
	time.Sleep(time.Millisecond)
	c.active.Add(-1)
}
func (c *overlapControl) Input(context.Context, string, []byte) error    { c.operation(); return nil }
func (c *overlapControl) Resize(context.Context, string, int, int) error { c.operation(); return nil }
func (c *overlapControl) Close(context.Context, string) error            { c.operation(); return nil }

func TestConnSerializesInputResizeAndClose(t *testing.T) {
	control := &overlapControl{}
	conn, _ := NewConn(context.Background(), "id", control)
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			if i%2 == 0 {
				_, _ = conn.Write([]byte("x"))
			} else {
				_ = conn.Resize(80+i, 24)
			}
		}(i)
	}
	group.Wait()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if control.overlapped.Load() {
		t.Fatal("terminal control operations overlapped")
	}
}

type blockingControl struct{}

func (blockingControl) Input(context.Context, string, []byte) error    { select {} }
func (blockingControl) Resize(context.Context, string, int, int) error { select {} }
func (blockingControl) Close(context.Context, string) error            { select {} }

func TestConnBoundsControlCallsAndShutdown(t *testing.T) {
	conn, err := NewConnWithLimitsAndTimeout(context.Background(), "id", blockingControl{}, 4, 1024, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := conn.Write([]byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("input = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("input was unbounded: %v", elapsed)
	}
	started = time.Now()
	if err := conn.Resize(80, 24); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("resize = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("resize was unbounded: %v", elapsed)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started = time.Now()
	if err := conn.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("shutdown was unbounded: %v", elapsed)
	}
}

type barrierControl struct {
	firstEntered chan struct{}
	releaseFirst chan struct{}
	mu           sync.Mutex
	inputs       int
	closes       int
}

func (c *barrierControl) Input(context.Context, string, []byte) error {
	c.mu.Lock()
	c.inputs++
	call := c.inputs
	c.mu.Unlock()
	if call == 1 {
		close(c.firstEntered)
		<-c.releaseFirst
	}
	return nil
}
func (*barrierControl) Resize(context.Context, string, int, int) error { return nil }
func (c *barrierControl) Close(context.Context, string) error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return nil
}

func TestQueuedInputCannotCrossCloseBoundary(t *testing.T) {
	control := &barrierControl{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	conn, err := NewConnWithLimitsAndTimeout(context.Background(), "id", control, 4, 1024, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, err := conn.Write([]byte("first")); first <- err }()
	<-control.firstEntered
	second := make(chan error, 1)
	go func() { _, err := conn.Write([]byte("must-not-run")); second <- err }()
	closed := make(chan error, 1)
	go func() { closed <- conn.Close() }()
	deadline := time.Now().Add(time.Second)
	for !conn.isClosed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !conn.isClosed() {
		t.Fatal("close boundary was not established")
	}
	if conn.FeedOutput([]byte("after-close-boundary")) {
		t.Fatal("output crossed close boundary")
	}
	close(control.releaseFirst)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("queued input=%v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.inputs != 1 || control.closes != 1 {
		t.Fatalf("inputs=%d closes=%d", control.inputs, control.closes)
	}
}
