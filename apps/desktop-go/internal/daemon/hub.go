package daemon

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
)

// Event is an opaque, already-encoded provider event. Protocol owns its
// payload; the daemon owns only ordering, replay, and delivery limits.
type Event struct {
	SessionID string
	RuntimeID string
	Epoch     string
	Sequence  uint64
	Payload   []byte
}

type streamKey struct{ session, runtime string }
type replayRef struct {
	key      streamKey
	sequence uint64
}

type Hub struct {
	mu                 sync.Mutex
	maxReplayCount     int
	maxReplayBytes     int
	maxSubscriberCount int
	maxSubscriberBytes int
	nextSequence       map[streamKey]uint64
	activeRuntime      map[string]string
	replay             map[streamKey][]Event
	replayBytes        map[streamKey]int
	replayOrder        []replayRef
	totalReplayCount   int
	totalReplayBytes   int
	subscribers        map[uint64]*subscriber
	nextSubscriber     uint64
	epoch              string
	closed             bool
}

type subscriber struct {
	ch     chan Event
	bytes  int
	closed bool
}

type Subscription struct {
	hub  *Hub
	id   uint64
	sub  *subscriber
	gaps []ReplayGap
}

type ReplayGap struct {
	SessionID, RuntimeID      string
	Requested, FirstAvailable uint64
}

func NewHub(replayCount, replayBytes, subscriberCount, subscriberBytes int) (*Hub, error) {
	return newHub(replayCount, replayBytes, subscriberCount, subscriberBytes, rand.Reader)
}

func newHub(replayCount, replayBytes, subscriberCount, subscriberBytes int, randomSource io.Reader) (*Hub, error) {
	var random [16]byte
	if _, err := io.ReadFull(randomSource, random[:]); err != nil {
		return nil, fmt.Errorf("generate hub epoch: %w", err)
	}
	// RFC 4122 UUID v4, matching protocol UUID strings.
	random[6] = random[6]&0x0f | 0x40
	random[8] = random[8]&0x3f | 0x80
	return &Hub{
		maxReplayCount: replayCount, maxReplayBytes: replayBytes,
		maxSubscriberCount: subscriberCount, maxSubscriberBytes: subscriberBytes,
		nextSequence: make(map[streamKey]uint64), replay: make(map[streamKey][]Event),
		activeRuntime: make(map[string]string), replayBytes: make(map[streamKey]int), subscribers: make(map[uint64]*subscriber),
		epoch: fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", random[0:4], random[4:6], random[6:8], random[8:10], random[10:16]),
	}, nil
}

type Cursor struct {
	SessionID string
	RuntimeID string
	Epoch     string
	Sequence  uint64
}

func (h *Hub) Epoch() string { h.mu.Lock(); defer h.mu.Unlock(); return h.epoch }

// BeginRuntime makes runtimeID authoritative and discards replay belonging to
// an older incarnation of the same session.
func (h *Hub) BeginRuntime(sessionID, runtimeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.activeRuntime[sessionID] = runtimeID
	for key := range h.nextSequence {
		if key.session == sessionID {
			delete(h.nextSequence, key)
		}
	}
	for key := range h.replay {
		if key.session == sessionID {
			h.removeReplayLocked(key)
		}
	}
}

func (h *Hub) EndRuntime(sessionID, runtimeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if h.activeRuntime[sessionID] != runtimeID {
		return
	}
	delete(h.activeRuntime, sessionID)
	key := streamKey{sessionID, runtimeID}
	delete(h.nextSequence, key)
	h.removeReplayLocked(key)
}

// Emit never waits for a subscriber. A subscriber that exceeds either queue
// limit is disconnected and can later resume from replay.
func (h *Hub) Emit(sessionID, runtimeID string, payload []byte, replayable bool) Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.activeRuntime[sessionID] != runtimeID {
		return Event{}
	}
	k := streamKey{sessionID, runtimeID}
	h.nextSequence[k]++
	e := Event{SessionID: sessionID, RuntimeID: runtimeID, Epoch: h.epoch, Sequence: h.nextSequence[k], Payload: append([]byte(nil), payload...)}
	if replayable && h.maxReplayCount > 0 && h.maxReplayBytes > 0 && len(payload) <= h.maxReplayBytes {
		h.replay[k] = append(h.replay[k], e)
		h.replayBytes[k] += len(e.Payload)
		h.totalReplayCount++
		h.totalReplayBytes += len(e.Payload)
		h.replayOrder = append(h.replayOrder, replayRef{key: k, sequence: e.Sequence})
		for h.totalReplayCount > h.maxReplayCount || h.totalReplayBytes > h.maxReplayBytes {
			h.evictOldestReplayLocked()
		}
	}
	for id, sub := range h.subscribers {
		if sub.closed || len(sub.ch) >= h.maxSubscriberCount || sub.bytes+len(e.Payload) > h.maxSubscriberBytes {
			h.dropLocked(id, sub)
			continue
		}
		delivery := e
		delivery.Payload = append([]byte(nil), e.Payload...)
		select {
		case sub.ch <- delivery:
			sub.bytes += len(e.Payload)
		default:
			h.dropLocked(id, sub)
		}
	}
	return e
}

func (h *Hub) evictOldestReplayLocked() {
	for len(h.replayOrder) > 0 {
		ref := h.replayOrder[0]
		h.replayOrder = h.replayOrder[1:]
		events := h.replay[ref.key]
		if len(events) == 0 || events[0].Sequence != ref.sequence {
			continue
		}
		event := events[0]
		h.replay[ref.key] = events[1:]
		h.replayBytes[ref.key] -= len(event.Payload)
		h.totalReplayCount--
		h.totalReplayBytes -= len(event.Payload)
		if len(h.replay[ref.key]) == 0 {
			delete(h.replay, ref.key)
			delete(h.replayBytes, ref.key)
		}
		return
	}
}

func (h *Hub) removeReplayLocked(key streamKey) {
	events := h.replay[key]
	for _, event := range events {
		h.totalReplayCount--
		h.totalReplayBytes -= len(event.Payload)
	}
	delete(h.replay, key)
	delete(h.replayBytes, key)
	if len(events) != 0 {
		filtered := h.replayOrder[:0]
		for _, ref := range h.replayOrder {
			if ref.key != key {
				filtered = append(filtered, ref)
			}
		}
		h.replayOrder = filtered
	}
}

func (h *Hub) ReplayStats() (streams, count, bytes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.replay), h.totalReplayCount, h.totalReplayBytes
}

// Subscribe registers before enqueueing replay while holding the hub lock, so
// a live event cannot overtake replay. Each cursor is the last applied event
// for one runtime; omitted runtimes replay from their bounded journal start.
func (h *Hub) Subscribe(cursors ...Cursor) *Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextSubscriber++
	capacity := h.maxSubscriberCount
	if capacity < 1 {
		capacity = 1
	}
	sub := &subscriber{ch: make(chan Event, capacity)}
	id := h.nextSubscriber
	h.subscribers[id] = sub
	if h.closed {
		h.dropLocked(id, sub)
		return &Subscription{hub: h, id: id, sub: sub}
	}
	var gaps []ReplayGap
	for key, latest := range h.nextSequence {
		events := h.replay[key]
		after := uint64(0)
		for _, cursor := range cursors {
			if cursor.SessionID == key.session && cursor.RuntimeID == key.runtime && cursor.Epoch == h.epoch {
				after = cursor.Sequence
				break
			}
		}
		if len(events) > 0 && after+1 < events[0].Sequence {
			gaps = append(gaps, ReplayGap{SessionID: key.session, RuntimeID: key.runtime, Requested: after, FirstAvailable: events[0].Sequence})
		} else if len(events) == 0 && after < latest {
			gaps = append(gaps, ReplayGap{SessionID: key.session, RuntimeID: key.runtime, Requested: after, FirstAvailable: latest + 1})
		}
		for _, e := range events {
			if e.Sequence <= after {
				continue
			}
			if len(sub.ch) >= h.maxSubscriberCount || sub.bytes+len(e.Payload) > h.maxSubscriberBytes {
				h.dropLocked(id, sub)
				return &Subscription{hub: h, id: id, sub: sub, gaps: gaps}
			}
			delivery := e
			delivery.Payload = append([]byte(nil), e.Payload...)
			sub.ch <- delivery
			sub.bytes += len(e.Payload)
		}
	}
	return &Subscription{hub: h, id: id, sub: sub, gaps: gaps}
}

func (s *Subscription) Gaps() []ReplayGap { return append([]ReplayGap(nil), s.gaps...) }

func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, sub := range h.subscribers {
		h.dropLocked(id, sub)
	}
}

func (s *Subscription) Receive(ctx context.Context) (Event, bool) {
	select {
	case <-ctx.Done():
		return Event{}, false
	case e, ok := <-s.sub.ch:
		if !ok {
			return Event{}, false
		}
		s.hub.mu.Lock()
		s.sub.bytes = max(0, s.sub.bytes-len(e.Payload))
		s.hub.mu.Unlock()
		return e, true
	}
}

func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if current, ok := s.hub.subscribers[s.id]; ok {
		s.hub.dropLocked(s.id, current)
	}
}

func (h *Hub) dropLocked(id uint64, sub *subscriber) {
	delete(h.subscribers, id)
	if !sub.closed {
		sub.closed = true
		for {
			select {
			case <-sub.ch:
				continue
			default:
				close(sub.ch)
				sub.bytes = 0
				return
			}
		}
	}
}
