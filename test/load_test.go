package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"omniserve/internal/cluster"
	"omniserve/internal/router"
	"omniserve/internal/scheduler"
	"omniserve/internal/telemetry"
)

// TestHighConcurrencyBurst simulates 1,500 concurrent requests and measures TTFT and throughput.
func TestHighConcurrencyBurst(t *testing.T) {
	metrics := telemetry.NewMetricsCollector()
	ring := router.NewHashRing(64)
	reg := cluster.NewClusterRegistry(ring, metrics)
	reg.InitializeMockCluster(3)

	queueCapacity := 4096
	queue := scheduler.NewBoundedQueue(queueCapacity, metrics)
	batcher := scheduler.NewDynamicBatcher(
		queue,
		scheduler.BatchConfig{
			MaxBatchSize: 32,
			BatchTimeout: 10 * time.Millisecond,
		},
		reg.ExecuteBatch,
		metrics,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)
	defer batcher.Stop()

	concurrentRequests := 1500
	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	var successfulRequests int64
	var mu sync.Mutex

	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startSignal

			reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
			defer reqCancel()

			respCh := make(chan scheduler.StreamChunk, 32)
			promptPrefix := "System: You are a high-performance LLM gateway."
			if id%3 == 0 {
				promptPrefix = "System: Specialized code analysis assistant."
			}

			_, prefixHash := router.ExtractAndHash(nil, promptPrefix)

			req := &scheduler.Request{
				ID:              fmt.Sprintf("load-req-%d", id),
				Prompt:          promptPrefix,
				PrefixHash:      prefixHash,
				ResponseChan:    respCh,
				Context:         reqCtx,
				CancelFunc:      reqCancel,
				Priority:        1,
				ArrivalTime:     time.Now(),
				MaxTokens:       4, // Quick simulation of 4 tokens
				Stream:          true,
				RoutingStrategy: "prefix-hash",
			}

			metrics.IncActiveRequests()
			if err := queue.Push(req); err != nil {
				metrics.DecActiveRequests(false)
				return
			}

			// Read stream chunks
			for chunk := range respCh {
				if chunk.IsLast && chunk.Err == nil {
					mu.Lock()
					successfulRequests++
					mu.Unlock()
				}
			}
		}(i)
	}

	startTime := time.Now()
	close(startSignal) // Trigger synthetic burst simultaneously!
	wg.Wait()
	duration := time.Since(startTime)

	snap := metrics.Snapshot()

	t.Logf("=== Load Test Results (1,500 Concurrent Burst) ===")
	t.Logf("Total Time: %v", duration)
	t.Logf("Successful Requests: %d / %d", successfulRequests, concurrentRequests)
	t.Logf("Median TTFT: %.2f ms", snap.MedianTTFTMs)
	t.Logf("p90 TTFT:    %.2f ms", snap.P90TTFTMs)
	t.Logf("p99 TTFT:    %.2f ms", snap.P99TTFTMs)
	t.Logf("KV Cache Hit Ratio: %.2f%%", snap.KVCacheHitRatio*100)
	t.Logf("Allocations per req: %d", snap.AllocationsPerReq)

	if successfulRequests < int64(concurrentRequests*95/100) {
		t.Fatalf("expected at least 95%% success rate, got %d / %d", successfulRequests, concurrentRequests)
	}
}

// TestBoundedQueueBackpressure verifies that HTTP 429 is triggered when queue reaches capacity.
func TestBoundedQueueBackpressure(t *testing.T) {
	metrics := telemetry.NewMetricsCollector()
	capacity := 10
	queue := scheduler.NewBoundedQueue(capacity, metrics)

	// Fill queue to maximum capacity
	for i := 0; i < capacity; i++ {
		err := queue.Push(&scheduler.Request{
			ID:           fmt.Sprintf("req-%d", i),
			ResponseChan: make(chan scheduler.StreamChunk, 1),
			ArrivalTime:  time.Now(),
		})
		if err != nil {
			t.Fatalf("unexpected error pushing req-%d: %v", i, err)
		}
	}

	if queue.Len() != capacity {
		t.Fatalf("expected queue length %d, got %d", capacity, queue.Len())
	}

	// Next push should fail immediately with ErrQueueFull
	err := queue.Push(&scheduler.Request{
		ID:           "overflow-req",
		ResponseChan: make(chan scheduler.StreamChunk, 1),
		ArrivalTime:  time.Now(),
	})
	if err != scheduler.ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	snap := metrics.Snapshot()
	if snap.DroppedRequests != 1 {
		t.Fatalf("expected 1 dropped request, got %d", snap.DroppedRequests)
	}
}

// TestCircuitBreakerFailover verifies that failed workers trip and traffic re-hashes.
func TestCircuitBreakerFailover(t *testing.T) {
	metrics := telemetry.NewMetricsCollector()
	ring := router.NewHashRing(64)
	reg := cluster.NewClusterRegistry(ring, metrics)
	reg.InitializeMockCluster(3)

	worker1, ok := reg.GetWorker("Worker-1")
	if !ok {
		t.Fatal("Worker-1 not found")
	}

	// Verify Worker-1 is initially healthy
	if !worker1.Breaker.IsAvailable() {
		t.Fatal("Worker-1 should initially be healthy")
	}

	// Trip circuit breaker on Worker-1
	worker1.SetSimulatedFailure(true)
	if worker1.Breaker.IsAvailable() {
		t.Fatal("Worker-1 should be marked unavailable after failure trip")
	}

	// Route 100 requests with various prefix hashes
	// None should be routed to Worker-1 because it's tripped!
	for i := 0; i < 100; i++ {
		prefixHash := uint64(i * 1234567)
		worker, err := reg.SelectWorker(prefixHash, "prefix-hash")
		if err != nil {
			t.Fatalf("routing error: %v", err)
		}
		if worker.WorkerID == "Worker-1" {
			t.Fatalf("request routed to tripped Worker-1! Routing failover failed.")
		}
	}

	// Recover Worker-1
	worker1.SetSimulatedFailure(false)
	worker1.Breaker.Reset()
	if !worker1.Breaker.IsAvailable() {
		t.Fatal("Worker-1 should be available after reset")
	}
}
