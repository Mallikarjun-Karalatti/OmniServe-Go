package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"omniserve/internal/telemetry"
)

// BatchConfig defines the timing and sizing bounds for dynamic batching.
type BatchConfig struct {
	MaxBatchSize int           // Maximum items in a batch (e.g., 16 or 32)
	BatchTimeout time.Duration // Maximum wait time before dispatching (e.g., 8–15ms)
}

// DefaultBatchConfig returns standard production defaults.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxBatchSize: 16,
		BatchTimeout: 10 * time.Millisecond,
	}
}

// BatchHandler defines the function signature called when a batch is ready.
type BatchHandler func(ctx context.Context, batch []*Request) error

// DynamicBatcher gathers incoming queued requests into time- and size-bounded batches.
type DynamicBatcher struct {
	queue     *BoundedQueue
	config    BatchConfig
	handler   BatchHandler
	metrics   *telemetry.MetricsCollector
	slicePool sync.Pool

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
	stopCh    chan struct{}
}

// NewDynamicBatcher creates a continuous dynamic batcher with sync.Pool optimization.
func NewDynamicBatcher(
	queue *BoundedQueue,
	config BatchConfig,
	handler BatchHandler,
	metrics *telemetry.MetricsCollector,
) *DynamicBatcher {
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 16
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 10 * time.Millisecond
	}

	b := &DynamicBatcher{
		queue:   queue,
		config:  config,
		handler: handler,
		metrics: metrics,
		stopCh:  make(chan struct{}),
	}

	// sync.Pool for zero-allocation batch slices
	b.slicePool = sync.Pool{
		New: func() any {
			slice := make([]*Request, 0, config.MaxBatchSize)
			return &slice
		},
	}

	return b
}

// Start launches the background batcher collection loop.
func (b *DynamicBatcher) Start(parentCtx context.Context) {
	if b.running.Swap(true) {
		return
	}
	b.ctx, b.cancel = context.WithCancel(parentCtx)
	b.wg.Add(1)
	go b.runLoop()
}

// Stop gracefully stops the dynamic batcher, draining remaining items.
func (b *DynamicBatcher) Stop() {
	if !b.running.Swap(false) {
		return
	}
	if b.cancel != nil {
		b.cancel()
	}
	close(b.stopCh)
	b.wg.Wait()
}

// UpdateConfig allows hot-reloading batching parameters.
func (b *DynamicBatcher) UpdateConfig(cfg BatchConfig) {
	if cfg.MaxBatchSize > 0 {
		b.config.MaxBatchSize = cfg.MaxBatchSize
	}
	if cfg.BatchTimeout > 0 {
		b.config.BatchTimeout = cfg.BatchTimeout
	}
}

// runLoop is the continuous collection and dispatch loop.
func (b *DynamicBatcher) runLoop() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			b.drainRemaining()
			return
		case <-b.stopCh:
			b.drainRemaining()
			return
		default:
		}

		// Step 1: Wait for at least one item from the queue
		firstReq := b.queue.PopNowait()
		if firstReq == nil {
			select {
			case <-b.ctx.Done():
				return
			case <-b.stopCh:
				return
			case <-b.queue.Notify():
				// An item arrived, pull it
				firstReq = b.queue.PopNowait()
				if firstReq == nil {
					continue
				}
			}
		}

		// Step 2: Acquire a batch slice from sync.Pool
		batchPtr := b.slicePool.Get().(*[]*Request)
		batch := (*batchPtr)[:0]
		batch = append(batch, firstReq)

		// If MaxBatchSize is 1 (immediate forwarding baseline), dispatch right away
		if b.config.MaxBatchSize <= 1 {
			b.dispatchBatch(batch, batchPtr)
			continue
		}

		// Step 3: Accumulate up to MaxBatchSize or until BatchTimeout expires
		timer := time.NewTimer(b.config.BatchTimeout)
		batchFull := false

		for len(batch) < b.config.MaxBatchSize && !batchFull {
			// Drain any immediately available queued requests
			for len(batch) < b.config.MaxBatchSize {
				req := b.queue.PopNowait()
				if req == nil {
					break
				}
				batch = append(batch, req)
			}

			if len(batch) >= b.config.MaxBatchSize {
				batchFull = true
				break
			}

			// Wait for more items or timer expiration
			select {
			case <-b.ctx.Done():
				batchFull = true
			case <-b.stopCh:
				batchFull = true
			case <-timer.C:
				// Batch timeout expired, time to dispatch
				batchFull = true
			case <-b.queue.Notify():
				// More items arrived, continue loop to drain
			}
		}

		// Clean up timer
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		// Step 4: Dispatch the batch
		b.dispatchBatch(batch, batchPtr)
	}
}

// dispatchBatch emits the batch to the handler and recycles the slice back to sync.Pool.
func (b *DynamicBatcher) dispatchBatch(batch []*Request, batchPtr *[]*Request) {
	if len(batch) == 0 {
		*batchPtr = batch[:0]
		b.slicePool.Put(batchPtr)
		return
	}

	batchSize := len(batch)
	if b.metrics != nil {
		b.metrics.RecordBatch(batchSize)
	}

	// Dispatch batch to handler in a separate goroutine or synchronously
	// We handle recycling after execution completes
	b.wg.Add(1)
	go func(items []*Request, ptr *[]*Request) {
		defer b.wg.Done()
		defer func() {
			// Zero out references to avoid memory leaks before returning to pool
			for i := range items {
				items[i] = nil
			}
			*ptr = items[:0]
			b.slicePool.Put(ptr)
		}()

		if b.handler != nil {
			_ = b.handler(b.ctx, items)
		}
	}(batch, batchPtr)
}

// drainRemaining flushes any requests still in the queue during shutdown.
func (b *DynamicBatcher) drainRemaining() {
	for {
		req := b.queue.PopNowait()
		if req == nil {
			break
		}
		batchPtr := b.slicePool.Get().(*[]*Request)
		batch := (*batchPtr)[:0]
		batch = append(batch, req)
		b.dispatchBatch(batch, batchPtr)
	}
}
