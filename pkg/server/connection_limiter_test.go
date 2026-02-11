package server

import (
	"sync"
	"testing"
	"time"
)

func TestNewConnectionLimiter(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	if cl.maxConnections != 2 {
		t.Errorf("expected maxConnections=2, got %d", cl.maxConnections)
	}
	if cl.ActiveCount() != 0 {
		t.Errorf("expected 0 active, got %d", cl.ActiveCount())
	}
}

func TestAcquireAndRelease(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	if err := cl.Acquire("10.0.0.1", "100"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active, got %d", cl.ActiveCount())
	}

	if err := cl.Acquire("10.0.0.2", "200"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.ActiveCount() != 2 {
		t.Errorf("expected 2 active, got %d", cl.ActiveCount())
	}

	cl.Release("10.0.0.1", "100")
	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active after release, got %d", cl.ActiveCount())
	}

	cl.Release("10.0.0.2", "200")
	if cl.ActiveCount() != 0 {
		t.Errorf("expected 0 active after release, got %d", cl.ActiveCount())
	}
}

func TestAcquireDuplicateIsNoop(t *testing.T) {
	cl := NewConnectionLimiter(1)
	defer cl.Stop()

	if err := cl.Acquire("10.0.0.1", "100"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Same key again — should be a no-op, not consume another slot
	if err := cl.Acquire("10.0.0.1", "100"); err != nil {
		t.Fatalf("duplicate acquire should not error: %v", err)
	}
	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active (duplicate should not add), got %d", cl.ActiveCount())
	}
}

func TestMaxConnectionsEnforced(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	cl.Acquire("10.0.0.1", "100")
	cl.Acquire("10.0.0.2", "200")

	// A third IP should be rejected
	err := cl.Acquire("10.0.0.3", "300")
	if err == nil {
		t.Fatal("expected error when exceeding max connections from a new IP")
	}
	if cl.ActiveCount() != 2 {
		t.Errorf("expected 2 active (rejected should not add), got %d", cl.ActiveCount())
	}
}

func TestGracePeriodSameIP(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	// Two IPs at capacity
	cl.Acquire("10.0.0.1", "100")
	cl.Acquire("10.0.0.2", "200")

	// Same IP (10.0.0.1) switching channels — should be allowed (grace period)
	err := cl.Acquire("10.0.0.1", "101")
	if err != nil {
		t.Fatalf("same-IP grace should allow: %v", err)
	}
	if cl.ActiveCount() != 3 {
		t.Errorf("expected 3 active during grace, got %d", cl.ActiveCount())
	}

	// Simulate old stream ending
	cl.Release("10.0.0.1", "100")
	if cl.ActiveCount() != 2 {
		t.Errorf("expected 2 active after old stream release, got %d", cl.ActiveCount())
	}
}

func TestGraceOnlyAllowsOneExtra(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	// Two IPs at capacity
	cl.Acquire("10.0.0.1", "100")
	cl.Acquire("10.0.0.2", "200")

	// Grace: same IP opens a second stream (channel switch) — allowed, now at 3
	err := cl.Acquire("10.0.0.1", "101")
	if err != nil {
		t.Fatalf("first grace should allow: %v", err)
	}
	if cl.ActiveCount() != 3 {
		t.Errorf("expected 3 active during grace, got %d", cl.ActiveCount())
	}

	// Now at max+1 — another acquire from same IP should be REJECTED
	err = cl.Acquire("10.0.0.1", "102")
	if err == nil {
		t.Fatal("expected rejection: grace should only allow one extra, not unlimited")
	}
	if cl.ActiveCount() != 3 {
		t.Errorf("expected still 3 active (rejected should not add), got %d", cl.ActiveCount())
	}

	// Another IP also rejected
	err = cl.Acquire("10.0.0.2", "201")
	if err == nil {
		t.Fatal("expected rejection for second IP when already over limit")
	}
}

func TestGracePeriodDifferentIPRejected(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	cl.Acquire("10.0.0.1", "100")
	cl.Acquire("10.0.0.2", "200")

	// New IP — no grace, should be rejected
	err := cl.Acquire("10.0.0.3", "300")
	if err == nil {
		t.Fatal("new IP should be rejected when at capacity")
	}
}

func TestUnlimitedMode(t *testing.T) {
	cl := NewConnectionLimiter(0)
	defer cl.Stop()

	// Should allow any number of connections
	for i := 0; i < 100; i++ {
		if err := cl.Acquire("10.0.0.1", string(rune('a'+i))); err != nil {
			t.Fatalf("unlimited mode should not reject: %v", err)
		}
	}
	if cl.ActiveCount() != 100 {
		t.Errorf("expected 100 active, got %d", cl.ActiveCount())
	}
}

func TestTouchHLS(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	if err := cl.Touch("10.0.0.1", "token-abc"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active, got %d", cl.ActiveCount())
	}

	// Touch again — should refresh, not add
	if err := cl.Touch("10.0.0.1", "token-abc"); err != nil {
		t.Fatalf("refresh touch should not error: %v", err)
	}
	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active after refresh, got %d", cl.ActiveCount())
	}
}

func TestTouchHLSMaxEnforced(t *testing.T) {
	cl := NewConnectionLimiter(1)
	defer cl.Stop()

	cl.Touch("10.0.0.1", "token-abc")

	err := cl.Touch("10.0.0.2", "token-def")
	if err == nil {
		t.Fatal("expected error for HLS from new IP when at capacity")
	}
}

func TestHLSSweepCleansStaleEntries(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	cl.Touch("10.0.0.1", "token-abc")

	// Manually age the entry
	cl.mu.Lock()
	key := connKey("10.0.0.1", "token-abc")
	cl.active[key].lastSeen = time.Now().Add(-hlsStaleTimeout - time.Second)
	cl.mu.Unlock()

	// Run one sweep cycle manually
	cl.mu.Lock()
	now := time.Now()
	for k, entry := range cl.active {
		if entry.isHLS && now.Sub(entry.lastSeen) > hlsStaleTimeout {
			delete(cl.active, k)
		}
	}
	cl.mu.Unlock()

	if cl.ActiveCount() != 0 {
		t.Errorf("expected 0 active after sweep, got %d", cl.ActiveCount())
	}
}

func TestHLSSweepKeepsFreshEntries(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	cl.Touch("10.0.0.1", "token-abc")

	// Run sweep — entry is fresh, should survive
	cl.mu.Lock()
	now := time.Now()
	for k, entry := range cl.active {
		if entry.isHLS && now.Sub(entry.lastSeen) > hlsStaleTimeout {
			delete(cl.active, k)
		}
	}
	cl.mu.Unlock()

	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active (fresh HLS should survive sweep), got %d", cl.ActiveCount())
	}
}

func TestSweepDoesNotRemoveLongLived(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	cl.Acquire("10.0.0.1", "100")

	// Age the entry — but it's not HLS, so sweep should not remove it
	cl.mu.Lock()
	key := connKey("10.0.0.1", "100")
	cl.active[key].lastSeen = time.Now().Add(-hlsStaleTimeout - time.Minute)
	cl.mu.Unlock()

	cl.mu.Lock()
	now := time.Now()
	for k, entry := range cl.active {
		if entry.isHLS && now.Sub(entry.lastSeen) > hlsStaleTimeout {
			delete(cl.active, k)
		}
	}
	cl.mu.Unlock()

	if cl.ActiveCount() != 1 {
		t.Errorf("expected 1 active (long-lived should survive sweep), got %d", cl.ActiveCount())
	}
}

func TestReleaseNonexistentIsNoop(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	// Should not panic or error
	cl.Release("10.0.0.1", "999")
	if cl.ActiveCount() != 0 {
		t.Errorf("expected 0 active, got %d", cl.ActiveCount())
	}
}

func TestConcurrentAccess(t *testing.T) {
	cl := NewConnectionLimiter(100)
	defer cl.Stop()

	var wg sync.WaitGroup
	errors := make(chan error, 200)

	// 50 goroutines acquiring
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ip := "10.0.0.1"
			id := string(rune(i + 1000))
			if err := cl.Acquire(ip, id); err != nil {
				errors <- err
			}
		}(i)
	}

	// 50 goroutines touching HLS
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ip := "10.0.0.2"
			id := string(rune(i + 2000))
			if err := cl.Touch(ip, id); err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent error: %v", err)
	}

	if cl.ActiveCount() != 100 {
		t.Errorf("expected 100 active, got %d", cl.ActiveCount())
	}
}

func TestMultiviewSameDevice(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	// One device watching two streams — uses 2 slots
	cl.Acquire("10.0.0.1", "100")
	cl.Acquire("10.0.0.1", "200")

	if cl.ActiveCount() != 2 {
		t.Errorf("expected 2 active, got %d", cl.ActiveCount())
	}

	// Same device switching one of the channels — grace period (2 -> 3)
	err := cl.Acquire("10.0.0.1", "300")
	if err != nil {
		t.Fatalf("multiview channel switch should be allowed via grace: %v", err)
	}

	// Can't open yet another — grace only allows one extra
	err = cl.Acquire("10.0.0.1", "400")
	if err == nil {
		t.Fatal("expected rejection: already used grace, can't keep growing")
	}

	// Old stream disconnects — back to max
	cl.Release("10.0.0.1", "100")
	if cl.ActiveCount() != 2 {
		t.Errorf("expected 2 active after release, got %d", cl.ActiveCount())
	}
}

func TestMixedHLSAndLongLived(t *testing.T) {
	cl := NewConnectionLimiter(2)
	defer cl.Stop()

	// One long-lived, one HLS
	cl.Acquire("10.0.0.1", "100")
	cl.Touch("10.0.0.2", "token-abc")

	if cl.ActiveCount() != 2 {
		t.Errorf("expected 2 active, got %d", cl.ActiveCount())
	}

	// Third from new IP — rejected
	err := cl.Acquire("10.0.0.3", "300")
	if err == nil {
		t.Fatal("expected rejection for new IP when at capacity with mixed connections")
	}

	// Release long-lived, now new IP should succeed
	cl.Release("10.0.0.1", "100")
	err = cl.Acquire("10.0.0.3", "300")
	if err != nil {
		t.Fatalf("should succeed after release: %v", err)
	}
}

func TestConnKey(t *testing.T) {
	key := connKey("10.0.0.1", "stream-42")
	expected := "10.0.0.1:stream-42"
	if key != expected {
		t.Errorf("expected %q, got %q", expected, key)
	}
}
