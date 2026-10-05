// Package daemon contains protocol-independent daemon runtime primitives.
package daemon

import "sync"

// ByteCache is a FIFO response cache bounded by both entry count and bytes.
// Values are copied on insertion and lookup so callers cannot mutate the
// accounting after admission.
type ByteCache struct {
	mu       sync.Mutex
	maxCount int
	maxBytes int
	bytes    int
	entries  []cacheEntry
}

type cacheEntry struct {
	key   string
	value []byte
}

func NewByteCache(maxCount, maxBytes int) *ByteCache {
	return &ByteCache{maxCount: maxCount, maxBytes: maxBytes}
}

func (c *ByteCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.entries {
		if c.entries[i].key == key {
			c.bytes -= len(c.entries[i].value)
			c.entries = append(c.entries[:i], c.entries[i+1:]...)
			break
		}
	}
	if c.maxCount <= 0 || c.maxBytes <= 0 || len(value) > c.maxBytes {
		return
	}
	copyValue := append([]byte(nil), value...)
	c.entries = append(c.entries, cacheEntry{key: key, value: copyValue})
	c.bytes += len(copyValue)
	for len(c.entries) > c.maxCount || c.bytes > c.maxBytes {
		c.bytes -= len(c.entries[0].value)
		c.entries = c.entries[1:]
	}
}

func (c *ByteCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.entries) - 1; i >= 0; i-- {
		if c.entries[i].key == key {
			return append([]byte(nil), c.entries[i].value...), true
		}
	}
	return nil, false
}

func (c *ByteCache) Stats() (count, bytes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.bytes
}
