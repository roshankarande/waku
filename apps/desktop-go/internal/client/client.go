// Package client owns Waku's protocol transport, correlation and replay state.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

const maxBufferedEventsPerRuntime = 4096
const maxBufferedEventBytesPerRuntime = 64 * 1024 * 1024
const maxRetainedEvents = 4096
const maxRetainedEventBytes = 64 * 1024 * 1024
const maxTrackedRuntimes = 4096
const maxTaskSubscriptions = 128
const maxPendingRequests = 128
const maxPendingRequestBytes = 64 * 1024 * 1024
const maxOutboundMessages = 128
const maxOutboundBytes = 64 * 1024 * 1024

type runtimeKey struct{ session, runtime protocol.UUID }
type lastSequence struct {
	epoch    protocol.UUID
	sequence uint64
}
type responseResult struct {
	payload json.RawMessage
	err     error
}
type pendingRequest struct {
	result chan responseResult
	bytes  int
}
type outboundMessage struct {
	payload []byte
	bytes   int
}

type queuedEvent struct {
	event protocol.SequencedEvent
	bytes int
}

type eventSubscription struct {
	mu        sync.Mutex
	queue     []queuedEvent
	bytes     int
	out       chan protocol.SequencedEvent
	terminal  chan TerminalEvent
	wake      chan struct{}
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
	delivered func(protocol.SequencedEvent, int)
}

func newTerminalEventSubscription(callback func(protocol.SequencedEvent, int)) *eventSubscription {
	s := &eventSubscription{terminal: make(chan TerminalEvent), wake: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}), delivered: callback}
	go s.run()
	return s
}

func newEventSubscription(callback ...func(protocol.SequencedEvent, int)) *eventSubscription {
	s := &eventSubscription{out: make(chan protocol.SequencedEvent), wake: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{})}
	if len(callback) != 0 {
		s.delivered = callback[0]
	}
	go s.run()
	return s
}

func (s *eventSubscription) enqueue(event protocol.SequencedEvent) bool {
	b, err := protocol.Marshal(event)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) >= maxBufferedEventsPerRuntime || s.bytes+len(b) > maxBufferedEventBytesPerRuntime {
		return false
	}
	s.queue = append(s.queue, queuedEvent{event: event, bytes: len(b)})
	s.bytes += len(b)
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return true
}

func (s *eventSubscription) run() {
	if s.out != nil {
		defer close(s.out)
	}
	if s.terminal != nil {
		defer close(s.terminal)
	}
	defer close(s.stopped)
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
				continue
			case <-s.done:
				return
			}
		}
		item := s.queue[0]
		s.mu.Unlock()
		var delivered bool
		if s.terminal != nil {
			value, terminal := terminalEvent(item.event.Event)
			if !terminal {
				delivered = true
			} else {
				select {
				case s.terminal <- value:
					delivered = true
				case <-s.done:
					return
				}
			}
		} else {
			select {
			case s.out <- item.event:
				delivered = true
			case <-s.done:
				return
			}
		}
		if delivered {
			s.mu.Lock()
			if len(s.queue) > 0 {
				s.queue = s.queue[1:]
				s.bytes -= item.bytes
			}
			s.mu.Unlock()
			if s.delivered != nil {
				s.delivered(item.event, item.bytes)
			}
		}
	}
}
func (s *eventSubscription) close() { s.closeOnce.Do(func() { close(s.done); <-s.stopped }) }
func (s *eventSubscription) closeAndDrain() []queuedEvent {
	s.close()
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]queuedEvent(nil), s.queue...)
}

type Client struct {
	conn      *wsConn
	write     chan outboundMessage
	done      chan struct{}
	closed    atomic.Bool
	closeOnce sync.Once

	mu            sync.Mutex
	subMu         sync.Mutex
	pending       map[protocol.UUID]pendingRequest
	pendingBytes  int
	outboundCount int
	outboundBytes int
	subs          map[runtimeKey]*eventSubscription
	buffered      map[runtimeKey][]protocol.SequencedEvent
	bufferedBytes map[runtimeKey]int
	last          map[runtimeKey]lastSequence
	accepted      map[runtimeKey]lastSequence
	retainedCount int
	retainedBytes int
	taskSubs      []chan uint64
	err           error
}

type ConnectOptions struct {
	Address          string
	Token            string
	Origin           string
	ClientID         protocol.UUID
	ResumeFrom       []protocol.ReplayCursor
	HandshakeTimeout time.Duration
}

func Connect(ctx context.Context, opts ConnectOptions) (*Client, error) {
	if strings.TrimSpace(opts.Token) == "" {
		return nil, errors.New("daemon authentication token is empty")
	}
	if opts.Origin != "" {
		origins, err := ParseAllowedOrigins(opts.Origin)
		if err != nil {
			return nil, err
		}
		if len(origins) != 1 {
			return nil, errors.New("exactly one origin is required")
		}
		opts.Origin = origins[0]
	}
	if opts.ClientID == "" {
		generated, err := newUUID()
		if err != nil {
			return nil, fmt.Errorf("generate client ID: %w", err)
		}
		opts.ClientID = generated
	}
	if !opts.ClientID.Valid() {
		return nil, errors.New("client ID is not a UUID")
	}
	if opts.HandshakeTimeout == 0 {
		opts.HandshakeTimeout = 5 * time.Second
	}
	hello, err := protocol.MarshalHello(protocol.NewHello(opts.Token, opts.ClientID, opts.ResumeFrom))
	if err != nil {
		return nil, err
	}
	hctx, cancel := context.WithTimeout(ctx, opts.HandshakeTimeout)
	defer cancel()
	conn, err := dialWebSocket(hctx, opts.Address, opts.Origin)
	if err != nil {
		return nil, err
	}
	err = conn.writeJSON(hello)
	if err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetReadDeadline(time.Now().Add(opts.HandshakeTimeout))
	raw, err := conn.readJSON(protocol.MaxHandshakeBytes)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read daemon hello: %w", err)
	}
	var reply protocol.ServerMessage
	if err := protocol.Decode(raw, &reply); err != nil {
		conn.Close()
		return nil, fmt.Errorf("decode daemon hello: %w", err)
	}
	if reply.Type == "rejected" {
		conn.Close()
		return nil, fmt.Errorf("daemon rejected connection: %s", reply.Message)
	}
	if reply.Type != "hello" {
		conn.Close()
		return nil, fmt.Errorf("daemon sent %q instead of hello", reply.Type)
	}
	if reply.ProtocolVersion != protocol.Version {
		conn.Close()
		return nil, fmt.Errorf("daemon protocol %d does not match desktop protocol %d", reply.ProtocolVersion, protocol.Version)
	}
	c := &Client{conn: conn, write: make(chan outboundMessage, maxOutboundMessages), done: make(chan struct{}), pending: make(map[protocol.UUID]pendingRequest), subs: make(map[runtimeKey]*eventSubscription), buffered: make(map[runtimeKey][]protocol.SequencedEvent), bufferedBytes: make(map[runtimeKey]int), last: make(map[runtimeKey]lastSequence), accepted: make(map[runtimeKey]lastSequence)}
	for _, cursor := range opts.ResumeFrom {
		key := runtimeKey{cursor.SessionID, cursor.RuntimeID}
		c.last[key] = lastSequence{cursor.Epoch, cursor.Sequence}
		c.accepted[key] = lastSequence{cursor.Epoch, cursor.Sequence}
	}
	go c.writeLoop()
	go c.readLoop()
	return c, nil
}

func (c *Client) Request(ctx context.Context, sessionID, runtimeID protocol.UUID, command any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, err := newUUID()
	if err != nil {
		return nil, fmt.Errorf("generate request ID: %w", err)
	}
	message, err := protocol.NewRequest(id, sessionID, runtimeID, command)
	if err != nil {
		return nil, err
	}
	payload, err := protocol.Marshal(message)
	if err != nil {
		return nil, err
	}
	result := make(chan responseResult, 1)
	c.mu.Lock()
	if c.closed.Load() {
		err := c.err
		c.mu.Unlock()
		if err == nil {
			err = io.ErrClosedPipe
		}
		return nil, err
	}
	if len(c.pending) >= maxPendingRequests || c.pendingBytes+len(payload) > maxPendingRequestBytes {
		c.mu.Unlock()
		return nil, errors.New("daemon pending request limit exceeded")
	}
	if err := c.admitOutboundLocked(payload); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	// Correlation is installed only after the message passed bounded outbound
	// admission. The mutex prevents a fast response from overtaking insertion.
	c.pending[id] = pendingRequest{result: result, bytes: len(payload)}
	c.pendingBytes += len(payload)
	c.mu.Unlock()
	select {
	case got := <-result:
		return got.payload, got.err
	case <-ctx.Done():
		c.removePending(id)
		return nil, ctx.Err()
	case <-c.done:
		c.removePending(id)
		return nil, c.Err()
	}
}

func (c *Client) Notify(ctx context.Context, sessionID, runtimeID protocol.UUID, command any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message, err := protocol.NewRequest(protocol.NilUUID, sessionID, runtimeID, command)
	if err != nil {
		return err
	}
	payload, err := protocol.Marshal(message)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return c.errOrClosedLocked()
	}
	return c.admitOutboundLocked(payload)
}

func (c *Client) Subscribe(sessionID, runtimeID protocol.UUID) (<-chan protocol.SequencedEvent, func()) {
	sub := newEventSubscription(c.markDelivered)
	unsubscribe := c.installSubscription(sessionID, runtimeID, sub)
	return sub.out, unsubscribe
}

func (c *Client) installSubscription(sessionID, runtimeID protocol.UUID, sub *eventSubscription) func() {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	key := runtimeKey{sessionID, runtimeID}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		sub.close()
		return func() {}
	}
	if c.subs[key] == nil && (len(c.subs) >= maxTrackedRuntimes || (c.accepted[key].epoch == "" && len(c.accepted) >= maxTrackedRuntimes)) {
		c.mu.Unlock()
		sub.close()
		return func() {}
	}
	old := c.subs[key]
	delete(c.subs, key)
	c.mu.Unlock()
	var retained []queuedEvent
	if old != nil {
		retained = old.closeAndDrain()
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		sub.close()
		return func() {}
	}
	for _, item := range retained {
		_ = sub.enqueue(item.event)
	}
	for _, event := range c.buffered[key] {
		_ = sub.enqueue(event)
	}
	c.subs[key] = sub
	delete(c.buffered, key)
	delete(c.bufferedBytes, key)
	c.mu.Unlock()
	return func() {
		c.subMu.Lock()
		defer c.subMu.Unlock()
		c.mu.Lock()
		if c.subs[key] == sub {
			delete(c.subs, key)
			c.mu.Unlock()
			left := sub.closeAndDrain()
			c.mu.Lock()
			if c.closed.Load() {
				c.mu.Unlock()
				return
			}
			if len(left) != 0 {
				events := make([]protocol.SequencedEvent, 0, len(left)+len(c.buffered[key]))
				bytes := c.bufferedBytes[key]
				for _, item := range left {
					events = append(events, item.event)
					bytes += item.bytes
				}
				events = append(events, c.buffered[key]...)
				c.buffered[key], c.bufferedBytes[key] = events, bytes
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) SubscribeTaskState() (<-chan uint64, func()) {
	ch := make(chan uint64, 1)
	c.mu.Lock()
	if c.closed.Load() {
		close(ch)
		c.mu.Unlock()
		return ch, func() {}
	}
	if len(c.taskSubs) >= maxTaskSubscriptions {
		close(ch)
		c.mu.Unlock()
		return ch, func() {}
	}
	c.taskSubs = append(c.taskSubs, ch)
	c.mu.Unlock()
	return ch, func() {
		c.mu.Lock()
		for i, candidate := range c.taskSubs {
			if candidate == ch {
				c.taskSubs = append(c.taskSubs[:i], c.taskSubs[i+1:]...)
				close(ch)
				break
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) LastSequences() []protocol.ReplayCursor {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]protocol.ReplayCursor, 0, len(c.last))
	for k, v := range c.last {
		result = append(result, protocol.ReplayCursor{SessionID: k.session, RuntimeID: k.runtime, Epoch: v.epoch, Sequence: v.sequence})
	}
	return result
}

func (c *Client) Done() <-chan struct{} { return c.done }
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	return io.ErrClosedPipe
}

// CloseTransport disconnects without requesting daemon shutdown.
func (c *Client) CloseTransport() error { c.fail(io.ErrClosedPipe); return nil }

// RequestShutdown is reserved for a daemon owned by this desktop.
func (c *Client) RequestShutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := protocol.Marshal(protocol.ShutdownMessage())
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return c.errOrClosedLocked()
	}
	return c.admitOutboundLocked(payload)
}

func (c *Client) writeLoop() {
	for {
		select {
		case message := <-c.write:
			err := c.conn.writeJSON(message.payload)
			c.mu.Lock()
			c.outboundCount--
			c.outboundBytes -= message.bytes
			c.mu.Unlock()
			if err != nil {
				c.fail(err)
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) readLoop() {
	for {
		raw, err := c.conn.readJSON(protocol.MaxWireMessageBytes)
		if err != nil {
			c.fail(err)
			return
		}
		var message protocol.ServerMessage
		if err := protocol.Decode(raw, &message); err != nil {
			c.fail(fmt.Errorf("malformed daemon message: %w", err))
			return
		}
		if err := c.handle(message); err != nil {
			c.fail(err)
			return
		}
	}
}

func (c *Client) handle(message protocol.ServerMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch message.Type {
	case "response":
		pending, matched := c.takePendingLocked(message.RequestID)
		if matched {
			if message.Outcome == nil {
				pending.result <- responseResult{err: errors.New("daemon returned a malformed response without an outcome")}
			} else if message.Outcome.Status == "ok" && message.Outcome.Payload != nil {
				pending.result <- responseResult{payload: message.Outcome.Payload}
			} else if message.Outcome.Status == "error" && message.Outcome.Error != nil {
				text := "daemon request failed"
				if message.Outcome.Error.Message != "" {
					text = message.Outcome.Error.Message
				}
				pending.result <- responseResult{err: errors.New(text)}
			} else {
				pending.result <- responseResult{err: errors.New("daemon returned a malformed response outcome")}
			}
		}
	case "event":
		if message.Event == nil {
			return nil
		}
		event := *message.Event
		key := runtimeKey{event.SessionID, event.RuntimeID}
		if _, found := c.accepted[key]; !found && len(c.accepted) >= maxTrackedRuntimes {
			return errors.New("daemon runtime tracking limit exceeded")
		}
		prior, found := c.accepted[key]
		if found && prior.epoch == event.Epoch && event.Sequence <= prior.sequence {
			return nil
		}
		if event.Sequence == 0 {
			return errors.New("daemon event sequence is zero")
		}
		if found && prior.epoch == event.Epoch && event.Sequence != prior.sequence+1 {
			return fmt.Errorf("daemon event sequence gap: got %d after %d", event.Sequence, prior.sequence)
		}
		if found && prior.epoch != event.Epoch && event.Sequence != 1 {
			return fmt.Errorf("daemon event epoch reset began at sequence %d", event.Sequence)
		}
		encoded, err := protocol.Marshal(event)
		if err != nil {
			return err
		}
		if c.retainedCount >= maxRetainedEvents || c.retainedBytes+len(encoded) > maxRetainedEventBytes {
			return errors.New("daemon retained event limit exceeded")
		}
		if sub := c.subs[key]; sub != nil {
			if sub.enqueue(event) {
				c.retainedCount++
				c.retainedBytes += len(encoded)
				c.accepted[key] = lastSequence{event.Epoch, event.Sequence}
			} else {
				// Force replay from the preceding cursor instead of blocking the
				// socket reader or silently losing an event for a slow consumer.
				return errors.New("event subscriber queue is full")
			}
		} else {
			if len(c.buffered[key]) >= maxBufferedEventsPerRuntime || c.bufferedBytes[key]+len(encoded) > maxBufferedEventBytesPerRuntime {
				return errors.New("daemon pre-subscription event buffer is full")
			}
			c.buffered[key] = append(c.buffered[key], event)
			c.bufferedBytes[key] += len(encoded)
			c.retainedCount++
			c.retainedBytes += len(encoded)
			c.accepted[key] = lastSequence{event.Epoch, event.Sequence}
		}
	case "taskStateChanged":
		alive := c.taskSubs[:0]
		for _, sub := range c.taskSubs {
			select {
			case <-sub:
			default:
			}
			select {
			case sub <- message.Revision:
				alive = append(alive, sub)
			default:
			}
		}
		c.taskSubs = alive
	case "shuttingDown":
		c.conn.Close()
	}
	return nil
}

func appendBoundedEvents(events []protocol.SequencedEvent, bytes int, event protocol.SequencedEvent, maxCount, maxBytes int) ([]protocol.SequencedEvent, int) {
	encoded, _ := protocol.Marshal(event)
	if len(events) >= maxCount || bytes+len(encoded) > maxBytes {
		return events, bytes
	}
	return append(events, event), bytes + len(encoded)
}

func (c *Client) markDelivered(event protocol.SequencedEvent, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := runtimeKey{event.SessionID, event.RuntimeID}
	previous, found := c.last[key]
	if !found || previous.epoch != event.Epoch || event.Sequence == previous.sequence+1 {
		c.last[key] = lastSequence{event.Epoch, event.Sequence}
	}
	c.retainedCount--
	c.retainedBytes -= size
	if c.retainedCount < 0 {
		c.retainedCount = 0
	}
	if c.retainedBytes < 0 {
		c.retainedBytes = 0
	}
}

func (c *Client) fail(err error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.conn.Close()
		c.mu.Lock()
		c.err = err
		for id, pending := range c.pending {
			pending.result <- responseResult{err: fmt.Errorf("Waku daemon disconnected: %w", err)}
			delete(c.pending, id)
		}
		c.pendingBytes = 0
		subs := make([]*eventSubscription, 0, len(c.subs))
		for key, sub := range c.subs {
			subs = append(subs, sub)
			delete(c.subs, key)
		}
		for _, sub := range c.taskSubs {
			close(sub)
		}
		c.taskSubs = nil
		c.mu.Unlock()
		for _, sub := range subs {
			sub.close()
		}
		c.mu.Lock()
		c.retainedCount, c.retainedBytes = 0, 0
		c.buffered = make(map[runtimeKey][]protocol.SequencedEvent)
		c.bufferedBytes = make(map[runtimeKey]int)
		close(c.done)
		c.mu.Unlock()
	})
}

func (c *Client) removePending(id protocol.UUID) {
	c.mu.Lock()
	_, _ = c.takePendingLocked(id)
	c.mu.Unlock()
}

func (c *Client) takePendingLocked(id protocol.UUID) (pendingRequest, bool) {
	pending, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
		c.pendingBytes -= pending.bytes
	}
	return pending, ok
}

func (c *Client) admitOutboundLocked(payload []byte) error {
	if c.outboundCount >= maxOutboundMessages || c.outboundBytes+len(payload) > maxOutboundBytes {
		return errors.New("daemon outbound queue limit exceeded")
	}
	message := outboundMessage{payload: payload, bytes: len(payload)}
	select {
	case c.write <- message:
		c.outboundCount++
		c.outboundBytes += message.bytes
		return nil
	default:
		return errors.New("daemon outbound queue limit exceeded")
	}
}

func (c *Client) errOrClosedLocked() error {
	if c.err != nil {
		return c.err
	}
	return io.ErrClosedPipe
}

func newUUID() (protocol.UUID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var text [36]byte
	hex.Encode(text[0:8], b[0:4])
	text[8] = '-'
	hex.Encode(text[9:13], b[4:6])
	text[13] = '-'
	hex.Encode(text[14:18], b[6:8])
	text[18] = '-'
	hex.Encode(text[19:23], b[8:10])
	text[23] = '-'
	hex.Encode(text[24:36], b[10:16])
	return protocol.UUID(text[:]), nil
}
