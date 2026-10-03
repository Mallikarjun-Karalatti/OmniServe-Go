package cluster

import (
	"context"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"omniserve/internal/telemetry"
)

// WorkerState describes the operational health of a worker backend.
type WorkerState string

const (
	StateHealthy  WorkerState = "Healthy"
	StateDegraded WorkerState = "Degraded"
	StateDead     WorkerState = "Dead"
)

// CircuitBreaker implements an adaptive circuit breaker for worker health.
type CircuitBreaker struct {
	mu                   sync.RWMutex
	state                WorkerState
	consecutiveFailures  int64
	consecutiveSuccesses int64
	failureThreshold     int64
	recoveryThreshold    int64
	lastStateChange      time.Time
}

// NewCircuitBreaker creates a circuit breaker with specified failure threshold.
func NewCircuitBreaker(failureThreshold int64) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 5
	}
	return &CircuitBreaker{
		state:             StateHealthy,
		failureThreshold:  failureThreshold,
		recoveryThreshold: 2,
		lastStateChange:   time.Now(),
	}
}

// State returns current worker circuit state.
func (cb *CircuitBreaker) State() WorkerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// IsAvailable returns true if worker can accept traffic.
func (cb *CircuitBreaker) IsAvailable() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state != StateDead
}

// IsTripped returns true if circuit breaker has tripped (node is marked Dead).
func (cb *CircuitBreaker) IsTripped() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateDead
}

// RecordSuccess records a successful execution or health check.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses++

	if cb.state != StateHealthy && cb.consecutiveSuccesses >= cb.recoveryThreshold {
		cb.state = StateHealthy
		cb.lastStateChange = time.Now()
	}
}

// RecordFailure records a timeout or execution error.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveSuccesses = 0
	cb.consecutiveFailures++

	if cb.consecutiveFailures >= cb.failureThreshold {
		if cb.state != StateDead {
			cb.state = StateDead
			cb.lastStateChange = time.Now()
		}
	} else if cb.consecutiveFailures >= 2 {
		if cb.state == StateHealthy {
			cb.state = StateDegraded
			cb.lastStateChange = time.Now()
		}
	}
}

// Reset explicitly restores the breaker to healthy.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateHealthy
	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses = 0
	cb.lastStateChange = time.Now()
}

// Trip explicitly marks the worker Dead (used for testing or administrative failover).
func (cb *CircuitBreaker) Trip() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateDead
	cb.consecutiveFailures = cb.failureThreshold
	cb.consecutiveSuccesses = 0
	cb.lastStateChange = time.Now()
}

// WorkerNode represents a single GPU inference backend (vLLM, Ollama, or high-fidelity mock).
type WorkerNode struct {
	WorkerID       string
	URL            string
	MaxConcurrent  int64
	ActiveRequests int64
	TotalRequests  int64
	KVCacheHits    int64
	Breaker        *CircuitBreaker
	IsMock         bool

	mu               sync.RWMutex
	latencySum       float64
	latencyCount     int64
	simulatedFailure bool

	// KV-Cache tracking
	kvMu    sync.RWMutex
	kvCache map[uint64]time.Time
}

// NewWorkerNode creates a worker node representation.
func NewWorkerNode(id string, url string, maxConcurrent int64, isMock bool) *WorkerNode {
	if maxConcurrent <= 0 {
		maxConcurrent = 256
	}
	return &WorkerNode{
		WorkerID:      id,
		URL:           url,
		MaxConcurrent: maxConcurrent,
		Breaker:       NewCircuitBreaker(5),
		IsMock:        isMock,
		kvCache:       make(map[uint64]time.Time),
	}
}

// CheckKVCache tests if the prefix hash is present in worker's simulated KV cache.
func (w *WorkerNode) CheckKVCache(prefixHash uint64) bool {
	if prefixHash == 0 {
		return false
	}
	w.kvMu.RLock()
	defer w.kvMu.RUnlock()
	_, found := w.kvCache[prefixHash]
	return found
}

// StoreKVCache records a prefix hash in worker's KV cache.
func (w *WorkerNode) StoreKVCache(prefixHash uint64) {
	if prefixHash == 0 {
		return
	}
	w.kvMu.Lock()
	defer w.kvMu.Unlock()
	w.kvCache[prefixHash] = time.Now()
}

// IncActive increments active concurrency.
func (w *WorkerNode) IncActive() {
	atomic.AddInt64(&w.ActiveRequests, 1)
	atomic.AddInt64(&w.TotalRequests, 1)
}

// DecActive decrements active requests and updates metrics.
func (w *WorkerNode) DecActive(latencyMs float64, success bool) {
	atomic.AddInt64(&w.ActiveRequests, -1)
	if success {
		w.Breaker.RecordSuccess()
		w.mu.Lock()
		w.latencySum += latencyMs
		w.latencyCount++
		w.mu.Unlock()
	} else {
		w.Breaker.RecordFailure()
	}
}

// RecordKVHit increments the KV cache hits counter.
func (w *WorkerNode) RecordKVHit() {
	atomic.AddInt64(&w.KVCacheHits, 1)
}

// SetSimulatedFailure toggles mock failure injection for fault tolerance testing.
func (w *WorkerNode) SetSimulatedFailure(fail bool) {
	w.mu.Lock()
	w.simulatedFailure = fail
	w.mu.Unlock()

	if fail {
		w.Breaker.Trip()
	} else {
		w.Breaker.Reset()
	}
}

// IsSimulatedFailure returns whether failure injection is active.
func (w *WorkerNode) IsSimulatedFailure() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.simulatedFailure
}

// Ping performs a health check against the worker backend.
func (w *WorkerNode) Ping(ctx context.Context, client *http.Client) bool {
	if w.IsMock {
		w.mu.RLock()
		fail := w.simulatedFailure
		w.mu.RUnlock()
		if fail {
			w.Breaker.RecordFailure()
			return false
		}
		w.Breaker.RecordSuccess()
		return true
	}

	// Real worker health check via HTTP
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.URL+"/health", nil)
	if err != nil {
		w.Breaker.RecordFailure()
		return false
	}

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		w.Breaker.RecordFailure()
		return false
	}
	_ = resp.Body.Close()
	w.Breaker.RecordSuccess()
	return true
}

// Snapshot returns a point-in-time worker status.
func (w *WorkerNode) Snapshot() telemetry.WorkerSnapshot {
	w.mu.RLock()
	var avgLatency float64
	if w.latencyCount > 0 {
		avgLatency = w.latencySum / float64(w.latencyCount)
	}
	w.mu.RUnlock()

	totalReq := atomic.LoadInt64(&w.TotalRequests)
	kvHits := atomic.LoadInt64(&w.KVCacheHits)
	var kvRatio float64
	if totalReq > 0 {
		kvRatio = float64(kvHits) / float64(totalReq)
	}

	state := w.Breaker.State()

	return telemetry.WorkerSnapshot{
		ID:               w.WorkerID,
		URL:              w.URL,
		State:            string(state),
		ActiveRequests:   atomic.LoadInt64(&w.ActiveRequests),
		TotalRequests:    totalReq,
		KVCacheHits:      kvHits,
		KVCacheHitRatio:  math.Round(kvRatio*1000) / 1000,
		AverageLatencyMs: math.Round(avgLatency*10) / 10,
		CircuitTripped:   w.Breaker.IsTripped(),
	}
}
