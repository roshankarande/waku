package terminal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrBackendUnsupported = errors.New("daemon terminal backend is not supported on this platform")
	ErrTerminalNotFound   = errors.New("daemon terminal is not open for this session runtime")
	ErrTerminalOwnership  = errors.New("daemon terminal belongs to a different runtime")
	ErrTerminalOverload   = errors.New("daemon terminal output exceeded bounded queues")
)

type Owner struct{ SessionID, RuntimeID string }

type BackendEvent struct {
	Data []byte
	Err  error
	Exit bool
}

type PTY interface {
	Write(context.Context, []byte) error
	Resize(context.Context, uint16, uint16) error
	Close(context.Context) error
}

// Backend must not retain cwd for interpretation outside the daemon host.
// Emit may be called concurrently after Open returns and until PTY.Close.
type Backend interface {
	Open(context.Context, string, uint16, uint16, func(BackendEvent)) (PTY, error)
}

type EventSink func(Owner, BackendEvent)

type ManagerLimits struct {
	MaxTerminals, MaxQueuedEvents, MaxQueuedBytes int
	MaxEventsPerTerminal, MaxBytesPerTerminal     int
	MaxInputBytes                                 int
	OperationTimeout                              time.Duration
}

func DefaultManagerLimits() ManagerLimits {
	return ManagerLimits{MaxTerminals: 32, MaxQueuedEvents: 2048, MaxQueuedBytes: 16 << 20, MaxEventsPerTerminal: 512, MaxBytesPerTerminal: 4 << 20, MaxInputBytes: 64 << 10, OperationTimeout: 5 * time.Second}
}

type Manager struct {
	backend    Backend
	sink       EventSink
	limits     ManagerLimits
	mu         sync.Mutex
	entries    map[string]*managedTerminal
	events     int
	bytes      int
	closed     bool
	cleanupErr error
	wg         sync.WaitGroup
}

type managedTerminal struct {
	owner        Owner
	pty          PTY
	queue        chan BackendEvent
	queuedEvents int
	queuedBytes  int
	final        *BackendEvent
	finalBytes   int
	ops          sync.Mutex
	closeOnce    sync.Once
	closeErr     error
}

func NewManager(backend Backend, sink EventSink, limits ManagerLimits) (*Manager, error) {
	defaults := DefaultManagerLimits()
	if limits.MaxTerminals == 0 {
		limits = defaults
	}
	if limits.OperationTimeout == 0 {
		limits.OperationTimeout = defaults.OperationTimeout
	}
	if backend == nil || sink == nil {
		return nil, errors.New("terminal manager requires a backend and event sink")
	}
	if limits.MaxTerminals < 1 || limits.MaxQueuedEvents < 1 || limits.MaxQueuedBytes < 1 || limits.MaxEventsPerTerminal < 1 || limits.MaxBytesPerTerminal < 1 || limits.MaxInputBytes < 1 || limits.OperationTimeout < 1 {
		return nil, errors.New("terminal manager limits must be positive")
	}
	return &Manager{backend: backend, sink: sink, limits: limits, entries: make(map[string]*managedTerminal)}, nil
}

func (m *Manager) HasSession(sessionID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[sessionID] != nil
}

func (m *Manager) Open(ctx context.Context, owner Owner, cwd string, cols, rows uint16) error {
	if owner.SessionID == "" || owner.RuntimeID == "" || !filepath.IsAbs(cwd) || cols < 1 || rows < 1 {
		return errors.New("terminal owner, cwd, columns, and rows are required")
	}
	entry := &managedTerminal{owner: owner, queue: make(chan BackendEvent, m.limits.MaxEventsPerTerminal)}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("terminal manager is closed")
	}
	if len(m.entries) >= m.limits.MaxTerminals {
		m.mu.Unlock()
		return ErrTerminalOverload
	}
	if m.entries[owner.SessionID] != nil {
		m.mu.Unlock()
		return errors.New("terminal is already open for this session runtime")
	}
	m.entries[owner.SessionID] = entry // provisional: accepts early backend output safely
	// Add while holding the same mutex Shutdown uses before Wait. This covers
	// Backend.Open itself and prevents an Add-vs-Wait race.
	m.wg.Add(1)
	m.mu.Unlock()

	pty, err := m.backend.Open(ctx, cwd, cols, rows, func(event BackendEvent) { m.enqueue(owner, entry, event) })
	if err != nil {
		m.mu.Lock()
		if m.entries[owner.SessionID] == entry {
			delete(m.entries, owner.SessionID)
		}
		m.discardLocked(entry)
		m.releaseFinalLocked(entry)
		m.mu.Unlock()
		m.wg.Done()
		return err
	}
	entry.ops.Lock()
	entry.pty = pty
	entry.ops.Unlock()
	m.mu.Lock()
	stopped := m.closed && m.entries[owner.SessionID] != entry
	m.mu.Unlock()
	go m.dispatch(entry)
	if stopped {
		return errors.New("terminal manager closed during backend open")
	}
	return nil
}

func (m *Manager) Write(ctx context.Context, owner Owner, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) > m.limits.MaxInputBytes {
		return fmt.Errorf("terminal input is %d bytes (maximum %d): %w", len(data), m.limits.MaxInputBytes, ErrTerminalOverload)
	}
	return m.control(ctx, owner, func(pty PTY) error { return pty.Write(ctx, append([]byte(nil), data...)) })
}

func (m *Manager) Resize(ctx context.Context, owner Owner, cols, rows uint16) error {
	if cols < 1 || rows < 1 {
		return errors.New("terminal dimensions must be positive")
	}
	return m.control(ctx, owner, func(pty PTY) error { return pty.Resize(ctx, cols, rows) })
}

func (m *Manager) control(ctx context.Context, owner Owner, operation func(PTY) error) error {
	ctx, cancel := m.boundedContext(ctx)
	defer cancel()
	m.mu.Lock()
	entry := m.entries[owner.SessionID]
	m.mu.Unlock()
	if entry == nil {
		return ErrTerminalNotFound
	}
	if entry.owner != owner {
		return ErrTerminalOwnership
	}
	done := make(chan error, 1)
	go func() {
		entry.ops.Lock()
		defer entry.ops.Unlock()
		m.mu.Lock()
		active := m.entries[owner.SessionID] == entry && entry.owner == owner && entry.pty != nil
		m.mu.Unlock()
		if !active {
			done <- ErrTerminalNotFound
			return
		}
		done <- operation(entry.pty)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) CloseTerminal(ctx context.Context, owner Owner) error {
	ctx, cancel := m.boundedContext(ctx)
	defer cancel()
	m.mu.Lock()
	entry := m.entries[owner.SessionID]
	if entry == nil {
		m.mu.Unlock()
		return ErrTerminalNotFound
	}
	if entry.owner != owner {
		m.mu.Unlock()
		return ErrTerminalOwnership
	}
	delete(m.entries, owner.SessionID)
	close(entry.queue)
	m.mu.Unlock()
	return m.closePTYContext(ctx, entry)
}

func (m *Manager) enqueue(owner Owner, entry *managedTerminal, event BackendEvent) {
	event.Data = append([]byte(nil), event.Data...)
	m.mu.Lock()
	if m.entries[owner.SessionID] != entry || entry.owner != owner || entry.final != nil {
		m.mu.Unlock()
		return
	}
	if event.Err != nil || event.Exit {
		delete(m.entries, owner.SessionID)
		m.setFinalLocked(entry, event, nil)
		close(entry.queue)
		m.mu.Unlock()
		return
	}
	if len(event.Data) == 0 {
		m.mu.Unlock()
		return
	}
	if entry.queuedEvents+1 > m.limits.MaxEventsPerTerminal || entry.queuedBytes+len(event.Data) > m.limits.MaxBytesPerTerminal || m.events+1 > m.limits.MaxQueuedEvents || m.bytes+len(event.Data) > m.limits.MaxQueuedBytes {
		delete(m.entries, owner.SessionID)
		m.setFinalLocked(entry, BackendEvent{Err: ErrTerminalOverload}, ErrTerminalOverload)
		close(entry.queue)
		m.mu.Unlock()
		go m.closePTY(entry)
		return
	}
	entry.queuedEvents++
	entry.queuedBytes += len(event.Data)
	m.events++
	m.bytes += len(event.Data)
	entry.queue <- event // capacity equals the stricter per-terminal count bound
	m.mu.Unlock()
}

func (m *Manager) dispatch(entry *managedTerminal) {
	defer m.wg.Done()
	for event := range entry.queue {
		m.sink(entry.owner, event)
		m.mu.Lock()
		entry.queuedEvents--
		entry.queuedBytes -= len(event.Data)
		m.events--
		m.bytes -= len(event.Data)
		m.mu.Unlock()
	}
	m.mu.Lock()
	final := entry.final
	m.mu.Unlock()
	if final != nil {
		m.sink(entry.owner, *final)
		m.mu.Lock()
		entry.queuedBytes -= entry.finalBytes
		m.bytes -= entry.finalBytes
		entry.finalBytes = 0
		m.mu.Unlock()
	}
	// Dispatch owns the lifecycle token through bounded cleanup. Explicit
	// close and shutdown may race this call; closeOnce keeps it idempotent.
	m.closePTY(entry)
}

func (m *Manager) closePTY(entry *managedTerminal) {
	ctx, cancel := context.WithTimeout(context.Background(), m.limits.OperationTimeout)
	defer cancel()
	if err := m.closePTYContext(ctx, entry); err != nil {
		m.mu.Lock()
		m.cleanupErr = errors.Join(m.cleanupErr, fmt.Errorf("terminal %s was not reaped: %w", entry.owner.SessionID, err))
		m.mu.Unlock()
	}
}

func (m *Manager) closePTYContext(ctx context.Context, entry *managedTerminal) error {
	done := make(chan error, 1)
	go func() {
		entry.ops.Lock()
		defer entry.ops.Unlock()
		// Open callbacks can fail an entry before Backend.Open returns its PTY.
		// Do not consume closeOnce until there is a handle to reap.
		if entry.pty == nil {
			done <- nil
			return
		}
		entry.closeOnce.Do(func() { entry.closeErr = entry.pty.Close(ctx) })
		done <- entry.closeErr
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) discardLocked(entry *managedTerminal) {
	for {
		select {
		case event, ok := <-entry.queue:
			if !ok {
				return
			}
			entry.queuedEvents--
			entry.queuedBytes -= len(event.Data)
			m.events--
			m.bytes -= len(event.Data)
		default:
			return
		}
	}
}

func (m *Manager) releaseFinalLocked(entry *managedTerminal) {
	entry.queuedBytes -= entry.finalBytes
	m.bytes -= entry.finalBytes
	entry.finalBytes = 0
}

type boundedFinalError struct {
	message string
	cause   error
}

func (e boundedFinalError) Error() string { return e.message }
func (e boundedFinalError) Unwrap() error { return e.cause }

func (m *Manager) setFinalLocked(entry *managedTerminal, event BackendEvent, cause error) {
	if event.Err != nil {
		available := min(m.limits.MaxBytesPerTerminal-entry.queuedBytes, m.limits.MaxQueuedBytes-m.bytes)
		if available < 0 {
			available = 0
		}
		message := event.Err.Error()
		if len(message) > available {
			message = message[:available]
			for !utf8.ValidString(message) {
				message = message[:len(message)-1]
			}
		}
		event.Err = boundedFinalError{message: message, cause: cause}
		entry.finalBytes = len(message)
		entry.queuedBytes += entry.finalBytes
		m.bytes += entry.finalBytes
	}
	entry.final = &event
}

func (m *Manager) boundedContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, m.limits.OperationTimeout)
}

func (m *Manager) Shutdown(ctx context.Context) error {
	ctx, cancel := m.boundedContext(ctx)
	defer cancel()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	entries := make([]*managedTerminal, 0, len(m.entries))
	for sessionID, entry := range m.entries {
		delete(m.entries, sessionID)
		close(entry.queue)
		entries = append(entries, entry)
	}
	m.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		var joined error
		for _, entry := range entries {
			joined = errors.Join(joined, m.closePTYContext(ctx, entry))
		}
		m.wg.Wait()
		m.mu.Lock()
		joined = errors.Join(joined, m.cleanupErr)
		m.mu.Unlock()
		done <- joined
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case joined := <-done:
		return joined
	}
}

type UnsupportedBackend struct{}

func (UnsupportedBackend) Open(context.Context, string, uint16, uint16, func(BackendEvent)) (PTY, error) {
	return nil, ErrBackendUnsupported
}
