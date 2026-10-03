package test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"omniserve/internal/cluster"
	"omniserve/internal/proxy"
	"omniserve/internal/router"
	"omniserve/internal/scheduler"
	"omniserve/internal/telemetry"
)

// BenchmarkDirectForwarding measures throughput without batching (batch size = 1).
func BenchmarkDirectForwarding(b *testing.B) {
	metrics := telemetry.NewMetricsCollector()
	ring := router.NewHashRing(64)
	reg := cluster.NewClusterRegistry(ring, metrics)
	reg.InitializeMockCluster(3)

	queue := scheduler.NewBoundedQueue(100000, metrics)
	batcher := scheduler.NewDynamicBatcher(
		queue,
		scheduler.BatchConfig{
			MaxBatchSize: 1, // Direct Forwarding baseline
			BatchTimeout: 0,
		},
		func(ctx context.Context, batch []*scheduler.Request) error {
			for _, req := range batch {
				req.ResponseChan <- scheduler.StreamChunk{
					ID:      req.ID,
					Content: "token",
					IsLast:  true,
				}
				close(req.ResponseChan)
			}
			return nil
		},
		metrics,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)
	defer batcher.Stop()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			respCh := make(chan scheduler.StreamChunk, 2)
			req := &scheduler.Request{
				ID:           "bench-req",
				Prompt:       "Benchmark test prompt",
				ResponseChan: respCh,
				ArrivalTime:  time.Now(),
			}
			_ = queue.Push(req)
			<-respCh
		}
	})
}

// BenchmarkDynamicBatching measures throughput with 10ms dynamic batching.
func BenchmarkDynamicBatching(b *testing.B) {
	metrics := telemetry.NewMetricsCollector()
	ring := router.NewHashRing(64)
	reg := cluster.NewClusterRegistry(ring, metrics)
	reg.InitializeMockCluster(3)

	queue := scheduler.NewBoundedQueue(100000, metrics)
	batcher := scheduler.NewDynamicBatcher(
		queue,
		scheduler.BatchConfig{
			MaxBatchSize: 32,
			BatchTimeout: 10 * time.Millisecond,
		},
		func(ctx context.Context, batch []*scheduler.Request) error {
			for _, req := range batch {
				req.ResponseChan <- scheduler.StreamChunk{
					ID:      req.ID,
					Content: "batched token",
					IsLast:  true,
				}
				close(req.ResponseChan)
			}
			return nil
		},
		metrics,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)
	defer batcher.Stop()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			respCh := make(chan scheduler.StreamChunk, 2)
			req := &scheduler.Request{
				ID:           "bench-batch",
				Prompt:       "Batched prompt query",
				ResponseChan: respCh,
				ArrivalTime:  time.Now(),
			}
			_ = queue.Push(req)
			<-respCh
		}
	})
}

// BenchmarkPrefixConsistentHashRouting measures prefix extraction and hash ring mapping.
func BenchmarkPrefixConsistentHashRouting(b *testing.B) {
	ring := router.NewHashRing(64)
	ring.AddWorker("Worker-1")
	ring.AddWorker("Worker-2")
	ring.AddWorker("Worker-3")

	messages := []scheduler.ChatMessage{
		{Role: "system", Content: "You are an expert system optimizing Go concurrency and dynamic batching."},
		{Role: "user", Content: "How does consistent hashing improve KV cache hits?"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, hash := router.ExtractAndHash(messages, "")
			worker, err := ring.GetWorker(hash, nil)
			if err != nil || worker == "" {
				b.Fatalf("failed routing: %v", err)
			}
		}
	})
}

// BenchmarkHeapChurnSyncPoolBuffer compares sync.Pool byte buffers vs standard allocations.
func BenchmarkHeapChurnSyncPoolBuffer(b *testing.B) {
	b.Run("SyncPoolBuffer", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := proxy.GetBuffer()
			buf.WriteString("data: {\"id\":\"chunk-1\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
			_ = buf.Bytes()
			proxy.PutBuffer(buf)
		}
	})

	b.Run("StandardAllocation", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Naive formatting with heap allocations
			s := fmt.Sprintf("data: {\"id\":\"chunk-%d\",\"choices\":[{\"delta\":{\"content\":\"%s\"}}]}\n\n", i, "hello")
			var buf bytes.Buffer
			buf.WriteString(s)
			_ = buf.Bytes()
		}
	})
}

// TestBatcherCorrectness verifies batch size bounds and timeout triggering.
func TestBatcherCorrectness(t *testing.T) {
	metrics := telemetry.NewMetricsCollector()
	queue := scheduler.NewBoundedQueue(100, metrics)

	var emittedMu sync.Mutex
	emittedBatches := make([][]*scheduler.Request, 0)

	batcher := scheduler.NewDynamicBatcher(
		queue,
		scheduler.BatchConfig{
			MaxBatchSize: 4,
			BatchTimeout: 20 * time.Millisecond,
		},
		func(ctx context.Context, batch []*scheduler.Request) error {
			emittedMu.Lock()
			// Copy slice
			c := make([]*scheduler.Request, len(batch))
			copy(c, batch)
			emittedBatches = append(emittedBatches, c)
			emittedMu.Unlock()

			for _, req := range batch {
				close(req.ResponseChan)
			}
			return nil
		},
		metrics,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher.Start(ctx)
	defer batcher.Stop()

	// Push 4 items -> should trigger by size bound
	for i := 0; i < 4; i++ {
		_ = queue.Push(&scheduler.Request{
			ID:           fmt.Sprintf("req-%d", i),
			ResponseChan: make(chan scheduler.StreamChunk, 1),
			ArrivalTime:  time.Now(),
		})
	}

	time.Sleep(50 * time.Millisecond)

	emittedMu.Lock()
	if len(emittedBatches) == 0 {
		t.Fatalf("expected batch to be emitted, got 0")
	}
	if len(emittedBatches[0]) != 4 {
		t.Errorf("expected batch size 4, got %d", len(emittedBatches[0]))
	}
	emittedMu.Unlock()
}
