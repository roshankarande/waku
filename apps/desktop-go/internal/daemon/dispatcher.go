package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
)

var ErrOverloaded = errors.New("daemon request admission limit exceeded")
var ErrRequestCollision = errors.New("request UUID was reused for a different operation")

type Request struct {
	ID, SessionID, RuntimeID string
	Payload                  []byte
}
type Response struct {
	Payload []byte
	Err     error
	Cached  bool
}
type RequestHandler func(context.Context, Request) ([]byte, error)
type pendingResponse struct {
	done        chan struct{}
	response    Response
	bytes       int
	fingerprint [sha256.Size]byte
}

type Dispatcher struct {
	actors                             *ActorGroup
	cache                              *ByteCache
	mu                                 sync.Mutex
	inflight                           map[string]*pendingResponse
	inflightBytes                      int
	maxInflightCount, maxInflightBytes int
	closed                             bool
	operations                         sync.WaitGroup
}

func NewDispatcher(mailboxSize, cacheCount, cacheBytes int, handler RequestHandler) *Dispatcher {
	return NewDispatcherWithLimits(mailboxSize, cacheCount, cacheBytes, cacheCount, cacheBytes, handler)
}

func NewDispatcherWithLimits(mailboxSize, cacheCount, cacheBytes, inflightCount, inflightBytes int, handler RequestHandler) *Dispatcher {
	if inflightCount < 1 {
		inflightCount = 1
	}
	if inflightBytes < 1 {
		inflightBytes = 1
	}
	d := &Dispatcher{cache: NewByteCache(cacheCount, cacheBytes), inflight: make(map[string]*pendingResponse), maxInflightCount: inflightCount, maxInflightBytes: inflightBytes}
	d.actors = NewActorGroup(mailboxSize, func(ctx context.Context, command Command) (any, error) { return handler(ctx, command.Value.(Request)) })
	return d
}

func (d *Dispatcher) Dispatch(waiterCtx context.Context, request Request) <-chan Response {
	result := make(chan Response, 1)
	fingerprint := requestFingerprint(request)
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		result <- Response{Err: ErrClosed}
		return result
	}
	if payload, ok := d.cache.Get(request.ID); ok {
		d.mu.Unlock()
		if len(payload) < sha256.Size || !equalFingerprint(payload[:sha256.Size], fingerprint[:]) {
			result <- Response{Err: ErrRequestCollision}
		} else {
			result <- decodeCached(payload[sha256.Size:])
		}
		return result
	}
	pending := d.inflight[request.ID]
	launch := false
	if pending != nil && pending.fingerprint != fingerprint {
		d.mu.Unlock()
		result <- Response{Err: ErrRequestCollision}
		return result
	}
	if pending == nil {
		requestBytes := len(request.ID) + len(request.SessionID) + len(request.RuntimeID) + len(request.Payload)
		if len(d.inflight) >= d.maxInflightCount || requestBytes > d.maxInflightBytes || d.inflightBytes+requestBytes > d.maxInflightBytes {
			d.mu.Unlock()
			result <- Response{Err: ErrOverloaded}
			return result
		}
		pending = &pendingResponse{done: make(chan struct{}), bytes: requestBytes, fingerprint: fingerprint}
		d.inflight[request.ID] = pending
		d.inflightBytes += requestBytes
		d.operations.Add(1)
		launch = true
	}
	d.mu.Unlock()
	if launch {
		// The operation belongs to the daemon, not the first socket/waiter.
		// ActorGroup cancellation still stops it during daemon shutdown.
		go func() {
			defer d.operations.Done()
			resolved := <-d.actors.Submit(context.Background(), Command{SessionID: request.SessionID, Value: request})
			response := Response{Err: resolved.Err}
			if resolved.Err == nil {
				response.Payload = append([]byte(nil), resolved.Value.([]byte)...)
			}
			d.mu.Lock()
			// Cache admission and close are one critical section: Shutdown cannot
			// establish closed between the decision and ByteCache mutation.
			if !d.closed {
				if resolved.Err == nil {
					d.cache.Put(request.ID, cacheValue(fingerprint, 0, response.Payload))
				} else if !errors.Is(resolved.Err, context.Canceled) && !errors.Is(resolved.Err, context.DeadlineExceeded) && !errors.Is(resolved.Err, ErrClosed) {
					d.cache.Put(request.ID, cacheValue(fingerprint, 1, []byte(resolved.Err.Error())))
				}
			}
			pending.response = response
			delete(d.inflight, request.ID)
			d.inflightBytes -= pending.bytes
			close(pending.done)
			d.mu.Unlock()
		}()
	}
	go func() {
		select {
		case <-waiterCtx.Done():
			result <- Response{Err: waiterCtx.Err()}
		case <-pending.done:
			response := pending.response
			response.Payload = append([]byte(nil), response.Payload...)
			result <- response
		}
	}()
	return result
}

func requestFingerprint(request Request) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(request.SessionID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(request.RuntimeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(request.Payload)
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], h.Sum(nil))
	return fingerprint
}

func equalFingerprint(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var different byte
	for i := range a {
		different |= a[i] ^ b[i]
	}
	return different == 0
}

func cacheValue(fingerprint [sha256.Size]byte, status byte, payload []byte) []byte {
	value := make([]byte, 0, sha256.Size+1+len(payload))
	value = append(value, fingerprint[:]...)
	value = append(value, status)
	return append(value, payload...)
}

func decodeCached(payload []byte) Response {
	response := Response{Cached: true}
	if len(payload) > 0 && payload[0] == 1 {
		response.Err = errors.New(string(payload[1:]))
	} else if len(payload) > 0 {
		response.Payload = append([]byte(nil), payload[1:]...)
	}
	return response
}

func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	actorErr := d.actors.Shutdown(ctx)
	drained := make(chan struct{})
	go func() { d.operations.Wait(); close(drained) }()
	select {
	case <-drained:
		return actorErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
