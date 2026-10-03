package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	"omniserve/internal/telemetry"
)

var (
	// ErrQueueFull is returned when the bounded queue exceeds MaxQueueCapacity.
	ErrQueueFull = errors.New("queue capacity exceeded (429 Too Many Requests)")
	// ErrQueueClosed is returned when operations are attempted on a closed queue.
	ErrQueueClosed = errors.New("request queue closed")
)

// StreamChunk represents a streamed token or generation chunk.
type StreamChunk struct {
	ID         string        `json:"id"`
	Content    string        `json:"content"`
	IsLast     bool          `json:"is_last"`
	Err        error         `json:"error,omitempty"`
	TokenIndex int           `json:"token_index"`
	WorkerID   string        `json:"worker_id,omitempty"`
	TTFT       time.Duration `json:"ttft,omitempty"`
	KVCacheHit bool          `json:"kv_cache_hit,omitempty"`
	TotalTokens int          `json:"total_tokens,omitempty"`
}

// ChatMessage represents a single message in an OpenAI chat completion payload.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Request holds all metadata for an inference job through OmniServe-Go.
type Request struct {
	ID                string
	Prompt            string
	Messages          []ChatMessage
	PrefixHash        uint64
	ResponseChan      chan StreamChunk
	Context           context.Context
	CancelFunc        context.CancelFunc
	Priority          int
	ArrivalTime       time.Time
	MaxTokens         int
	Temperature       float64
	Stream            bool
	PreferredWorkerID string
	WorkerID          string
	RoutingStrategy   string // "prefix-hash", "random", "round-robin"

	// Heap index for container/heap
	index int
}

// priorityHeap implements container/heap.Interface for Request pointers.
type priorityHeap []*Request

func (h priorityHeap) Len() int { return len(h) }

func (h priorityHeap) Less(i, j int) bool {
	// Higher priority integer takes precedence
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	// Tie-break with arrival time (FIFO)
	return h[i].ArrivalTime.Before(h[j].ArrivalTime)
}

func (h priorityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *priorityHeap) Push(x any) {
	n := len(*h)
	item := x.(*Request)
	item.index = n
	*h = append(*h, item)
}

func (h *priorityHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// BoundedQueue is a thread-safe, priority-ordered, bounded task queue.
type BoundedQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	notifyCh chan struct{}
	pq       priorityHeap
	capacity int
	closed   bool
	metrics  *telemetry.MetricsCollector
}

// NewBoundedQueue creates a new bounded queue with backpressure.
func NewBoundedQueue(capacity int, metrics *telemetry.MetricsCollector) *BoundedQueue {
	bq := &BoundedQueue{
		capacity: capacity,
		pq:       make(priorityHeap, 0, capacity),
		notifyCh: make(chan struct{}, 1),
		metrics:  metrics,
	}
	bq.cond = sync.NewCond(&bq.mu)
	heap.Init(&bq.pq)
	return bq
}

// Push enqueues a request. If the queue is at capacity, it drops the request immediately
// and returns ErrQueueFull for HTTP 429 Too Many Requests.
func (q *BoundedQueue) Push(req *Request) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if q.pq.Len() >= q.capacity {
		if q.metrics != nil {
			q.metrics.IncDroppedRequests()
		}
		return ErrQueueFull
	}

	if req.ArrivalTime.IsZero() {
		req.ArrivalTime = time.Now()
	}

	heap.Push(&q.pq, req)

	if q.metrics != nil {
		q.metrics.IncQueueDepth()
	}

	q.cond.Signal()

	select {
	case q.notifyCh <- struct{}{}:
	default:
	}
	return nil
}

// Notify returns a channel that signals when items are pushed to the queue.
func (q *BoundedQueue) Notify() <-chan struct{} {
	return q.notifyCh
}

// Pop extracts the highest priority request. If queue is empty, it blocks until an item arrives,
// the context expires, or the queue is closed.
func (q *BoundedQueue) Pop(ctx context.Context) (*Request, error) {
	// Channel-based notification to support context cancellation while waiting
	notify := make(chan struct{}, 1)

	go func() {
		q.mu.Lock()
		for q.pq.Len() == 0 && !q.closed && ctx.Err() == nil {
			q.cond.Wait()
		}
		q.mu.Unlock()
		select {
		case notify <- struct{}{}:
		default:
		}
	}()

	select {
	case <-ctx.Done():
		// Context expired while waiting
		return nil, ctx.Err()
	case <-notify:
		q.mu.Lock()
		defer q.mu.Unlock()

		if q.closed && q.pq.Len() == 0 {
			return nil, ErrQueueClosed
		}

		if q.pq.Len() == 0 {
			return nil, ctx.Err()
		}

		item := heap.Pop(&q.pq).(*Request)
		if q.metrics != nil {
			q.metrics.DecQueueDepth()
		}
		return item, nil
	}
}

// PopNowait tries to pop immediately without blocking. Returns nil if empty.
func (q *BoundedQueue) PopNowait() *Request {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.pq.Len() == 0 {
		return nil
	}

	item := heap.Pop(&q.pq).(*Request)
	if q.metrics != nil {
		q.metrics.DecQueueDepth()
	}
	return item
}

// Len returns the current queue length.
func (q *BoundedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pq.Len()
}

// Capacity returns the maximum queue capacity.
func (q *BoundedQueue) Capacity() int {
	return q.capacity
}

// Close closes the queue and wakes up blocked waiters.
func (q *BoundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
