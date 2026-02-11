package server

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	hlsStaleTimeout  = 30 * time.Second
	hlsSweepInterval = 10 * time.Second
)

type connEntry struct {
	startTime time.Time
	lastSeen  time.Time
	isHLS     bool
	clientIP  string
}

// ConnectionLimiter tracks active streams and enforces the provider's max connections limit.
type ConnectionLimiter struct {
	mu             sync.RWMutex
	active         map[string]*connEntry // key: "clientIP:streamID"
	maxConnections int                   // 0 = unlimited
	done           chan struct{}
}

// NewConnectionLimiter creates a new limiter. If max is 0, no limit is enforced.
func NewConnectionLimiter(max int) *ConnectionLimiter {
	cl := &ConnectionLimiter{
		active:         make(map[string]*connEntry),
		maxConnections: max,
		done:           make(chan struct{}),
	}
	go cl.sweepStaleHLS()
	return cl
}

// Stop shuts down the background sweep goroutine.
func (cl *ConnectionLimiter) Stop() {
	close(cl.done)
}

func connKey(clientIP, streamID string) string {
	return clientIP + ":" + streamID
}

// Acquire reserves a connection slot for a long-lived stream.
// Returns nil if the slot was acquired (or already held), or an error if the limit is reached.
func (cl *ConnectionLimiter) Acquire(clientIP, streamID string) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	key := connKey(clientIP, streamID)

	// Already tracking this exact stream — no-op
	if _, exists := cl.active[key]; exists {
		return nil
	}

	if err := cl.checkLimit(clientIP); err != nil {
		return err
	}

	now := time.Now()
	cl.active[key] = &connEntry{
		startTime: now,
		lastSeen:  now,
		isHLS:     false,
		clientIP:  clientIP,
	}
	log.Printf("[iptv-proxy] Connection acquired: %s (active: %d/%d)", key, len(cl.active), cl.maxConnections)
	return nil
}

// Release frees a connection slot for a long-lived stream.
func (cl *ConnectionLimiter) Release(clientIP, streamID string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	key := connKey(clientIP, streamID)
	if _, exists := cl.active[key]; exists {
		delete(cl.active, key)
		log.Printf("[iptv-proxy] Connection released: %s (active: %d/%d)", key, len(cl.active), cl.maxConnections)
	}
}

// Touch registers or refreshes an HLS connection slot. HLS connections are cleaned up
// by the background sweeper when they haven't been seen for hlsStaleTimeout.
func (cl *ConnectionLimiter) Touch(clientIP, streamID string) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	key := connKey(clientIP, streamID)

	// Already tracking — just refresh timestamp
	if entry, exists := cl.active[key]; exists {
		entry.lastSeen = time.Now()
		return nil
	}

	if err := cl.checkLimit(clientIP); err != nil {
		return err
	}

	now := time.Now()
	cl.active[key] = &connEntry{
		startTime: now,
		lastSeen:  now,
		isHLS:     true,
		clientIP:  clientIP,
	}
	log.Printf("[iptv-proxy] HLS connection acquired: %s (active: %d/%d)", key, len(cl.active), cl.maxConnections)
	return nil
}

// ActiveCount returns the number of currently active connections.
func (cl *ConnectionLimiter) ActiveCount() int {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return len(cl.active)
}

// checkLimit checks whether a new connection from clientIP is allowed.
// Must be called with cl.mu held.
func (cl *ConnectionLimiter) checkLimit(clientIP string) error {
	if cl.maxConnections <= 0 {
		return nil
	}

	if len(cl.active) < cl.maxConnections {
		return nil
	}

	// Grace period: if the same IP already has an active stream, allow max+1 briefly.
	// This handles channel switching where the old stream hasn't disconnected yet.
	if cl.ipHasActiveStream(clientIP) {
		return nil
	}

	return fmt.Errorf("max connections reached (%d/%d)", len(cl.active), cl.maxConnections)
}

// ipHasActiveStream returns true if the given IP has at least one active stream.
// Must be called with cl.mu held.
func (cl *ConnectionLimiter) ipHasActiveStream(clientIP string) bool {
	for _, entry := range cl.active {
		if entry.clientIP == clientIP {
			return true
		}
	}
	return false
}

func (cl *ConnectionLimiter) sweepStaleHLS() {
	ticker := time.NewTicker(hlsSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cl.done:
			return
		case <-ticker.C:
			cl.mu.Lock()
			now := time.Now()
			for key, entry := range cl.active {
				if entry.isHLS && now.Sub(entry.lastSeen) > hlsStaleTimeout {
					delete(cl.active, key)
					log.Printf("[iptv-proxy] HLS connection expired: %s (active: %d/%d)", key, len(cl.active), cl.maxConnections)
				}
			}
			cl.mu.Unlock()
		}
	}
}
