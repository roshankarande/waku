// Package terminal adapts daemon-owned PTY streams to MyGo terminal.Conn.
package terminal

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var ErrOutputBackpressure = errors.New("remote terminal output exceeded bounded client buffer")

type Control interface {
	Input(context.Context, string, []byte) error
	Resize(context.Context, string, int, int) error
	Close(context.Context, string) error
}

type Conn struct {
	ctx                 context.Context
	id                  string
	control             Control
	mu                  sync.Mutex
	ready               *sync.Cond
	queue               [][]byte
	offset, queuedBytes int
	maxChunks, maxBytes int
	closed              bool
	closing             bool
	readErr             error
	stopped             chan struct{}
	ops                 chan struct{}
	controlTimeout      time.Duration
	closeOnce           sync.Once
	closeDone           chan struct{}
	closeErr            error
}

const (
	defaultMaxChunks      = 1024
	defaultMaxBytes       = 4 << 20
	defaultControlTimeout = 5 * time.Second
)

func NewConn(ctx context.Context, terminalID string, control Control) (*Conn, error) {
	return NewConnWithLimitsAndTimeout(ctx, terminalID, control, defaultMaxChunks, defaultMaxBytes, defaultControlTimeout)
}

func NewConnWithLimits(ctx context.Context, terminalID string, control Control, maxChunks, maxBytes int) (*Conn, error) {
	return NewConnWithLimitsAndTimeout(ctx, terminalID, control, maxChunks, maxBytes, defaultControlTimeout)
}

func NewConnWithLimitsAndTimeout(ctx context.Context, terminalID string, control Control, maxChunks, maxBytes int, timeout time.Duration) (*Conn, error) {
	if terminalID == "" || control == nil {
		return nil, errors.New("remote terminal requires an id and daemon control")
	}
	if maxChunks < 1 || maxBytes < 1 {
		return nil, errors.New("remote terminal buffer limits must be positive")
	}
	if timeout <= 0 {
		return nil, errors.New("remote terminal control timeout must be positive")
	}
	c := &Conn{ctx: ctx, id: terminalID, control: control, maxChunks: maxChunks, maxBytes: maxBytes, stopped: make(chan struct{}), ops: make(chan struct{}, 1), controlTimeout: timeout, closeDone: make(chan struct{})}
	c.ops <- struct{}{}
	c.ready = sync.NewCond(&c.mu)
	if err := ctx.Err(); err != nil {
		c.mu.Lock()
		c.finishLocked(err)
		c.mu.Unlock()
		go c.closeRemote(context.Background())
		return c, nil
	}
	go func() {
		select {
		case <-ctx.Done():
			c.terminate(ctx.Err(), true)
		case <-c.stopped:
		}
	}()
	return c, nil
}

func (c *Conn) FeedOutput(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	c.mu.Lock()
	if c.closed || c.closing {
		c.mu.Unlock()
		return false
	}
	if len(c.queue) >= c.maxChunks || c.queuedBytes+len(data) > c.maxBytes {
		c.finishLocked(ErrOutputBackpressure)
		c.mu.Unlock()
		go c.closeRemote(context.Background())
		return false
	}
	copyData := append([]byte(nil), data...)
	c.queue = append(c.queue, copyData)
	c.queuedBytes += len(copyData)
	c.ready.Signal()
	c.mu.Unlock()
	return true
}

func (c *Conn) FeedExit(err error) { c.terminate(err, false) }

func (c *Conn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.queue) == 0 && !c.closed {
		c.ready.Wait()
	}
	if len(c.queue) > 0 {
		chunk := c.queue[0]
		n := copy(p, chunk[c.offset:])
		c.offset += n
		c.queuedBytes -= n
		if c.offset == len(chunk) {
			c.queue = c.queue[1:]
			c.offset = 0
		}
		return n, nil
	}
	if c.readErr != nil {
		return 0, c.readErr
	}
	return 0, io.EOF
}

func (c *Conn) Write(p []byte) (int, error) {
	if c.isClosed() {
		return 0, io.ErrClosedPipe
	}
	data := append([]byte(nil), p...)
	opCtx, cancel := context.WithTimeout(c.ctx, c.controlTimeout)
	defer cancel()
	if err := c.withOperation(opCtx, false, func() error { return c.control.Input(opCtx, c.id, data) }); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Conn) Resize(cols, rows int) error {
	if cols < 1 || rows < 1 {
		return errors.New("terminal dimensions must be positive")
	}
	if c.isClosed() {
		return io.ErrClosedPipe
	}
	opCtx, cancel := context.WithTimeout(c.ctx, c.controlTimeout)
	defer cancel()
	return c.withOperation(opCtx, false, func() error { return c.control.Resize(opCtx, c.id, cols, rows) })
}

func (c *Conn) Close() error { return c.Shutdown(context.Background()) }
func (c *Conn) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.terminate(nil, false)
	return c.closeRemote(ctx)
}

func (c *Conn) terminate(err error, notifyRemote bool) {
	c.mu.Lock()
	if !c.closed {
		c.finishLocked(err)
	}
	c.mu.Unlock()
	if notifyRemote {
		_ = c.closeRemote(context.Background())
	}
}

func (c *Conn) finishLocked(err error) {
	c.closed = true
	c.readErr = err
	close(c.stopped)
	c.ready.Broadcast()
}

func (c *Conn) closeRemote(parent context.Context) error {
	c.closeOnce.Do(func() {
		go func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), c.controlTimeout)
			defer cancel()
			c.closeErr = c.withOperation(closeCtx, true, func() error { return c.control.Close(closeCtx, c.id) })
			close(c.closeDone)
		}()
	})
	select {
	case <-parent.Done():
		return parent.Err()
	case <-c.closeDone:
		return c.closeErr
	}
}

func (c *Conn) withOperation(ctx context.Context, allowClosed bool, operation func() error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ops:
	}
	// Admission is rechecked only after owning the serialization token. A
	// Write/Resize queued before Close therefore cannot cross the close
	// boundary after the earlier optimistic isClosed check.
	c.mu.Lock()
	reject := !allowClosed && (c.closed || c.closing)
	c.mu.Unlock()
	if reject {
		c.ops <- struct{}{}
		return io.ErrClosedPipe
	}
	result := make(chan error, 1)
	go func() {
		err := operation()
		c.ops <- struct{}{}
		result <- err
	}()
	select {
	case <-ctx.Done():
		// A broken transport may leak its operation goroutine, but it retains
		// serialization and cannot block this caller or overlap later controls.
		return ctx.Err()
	case err := <-result:
		return err
	}
}

func (c *Conn) isClosed() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.closed || c.closing }

var _ io.ReadWriteCloser = (*Conn)(nil)
var _ interface{ Resize(int, int) error } = (*Conn)(nil)
