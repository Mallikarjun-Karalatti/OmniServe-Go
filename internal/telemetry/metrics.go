package telemetry

import (
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Snapshot represents a point-in-time metrics snapshot for telemetry and API endpoints.
type Snapshot struct {
	Timestamp            time.Time          `json:"timestamp"`
	QueueDepth           int64              `json:"queue_depth"`
	ActiveRequests       int64              `json:"active_requests"`
	TotalRequests        int64              `json:"total_requests"`
	SuccessfulRequests   int64              `json:"successful_requests"`
	DroppedRequests      int64              `json:"dropped_requests"`
	CurrentQPS           float64            `json:"current_qps"`
	TokensPerSecond      float64            `json:"tokens_per_second"`
	TotalTokens          int64              `json:"total_tokens"`
	MedianTTFTMs         float64            `json:"median_ttft_ms"`
	P90TTFTMs            float64            `json:"p90_ttft_ms"`
	P99TTFTMs            float64            `json:"p99_ttft_ms"`
	AverageTTFTMs        float64            `json:"average_ttft_ms"`
	TotalBatches         int64              `json:"total_batches"`
	AverageBatchSize     float64            `json:"average_batch_size"`
	BatchSizeDist        map[string]int64   `json:"batch_size_distribution"`
	KVCacheHits          int64              `json:"kv_cache_hits"`
	KVCacheMisses        int64              `json:"kv_cache_misses"`
	KVCacheHitRatio      float64            `json:"kv_cache_hit_ratio"`
	MemoryAllocMB        float64            `json:"memory_alloc_mb"`
	TotalAllocMB         float64            `json:"total_alloc_mb"`
	NumGC                uint32             `json:"num_gc"`
	HeapInuseMB          float64            `json:"heap_inuse_mb"`
	AllocationsPerReq    uint64             `json:"allocs_per_request"`
	Workers              []WorkerSnapshot   `json:"workers,omitempty"`
}

// WorkerSnapshot represents worker-level telemetry.
type WorkerSnapshot struct {
	ID              string  `json:"id"`
	URL             string  `json:"url"`
	State           string  `json:"state"`
	ActiveRequests  int64   `json:"active_requests"`
	TotalRequests   int64   `json:"total_requests"`
	KVCacheHits     int64   `json:"kv_cache_hits"`
	KVCacheHitRatio float64 `json:"kv_cache_hit_ratio"`
	AverageLatencyMs float64 `json:"average_latency_ms"`
	CircuitTripped  bool    `json:"circuit_tripped"`
}

// MetricsCollector tracks real-time performance and load metrics across OmniServe-Go.
type MetricsCollector struct {
	// Gauges
	queueDepth     int64
	activeRequests int64

	// Counters
	totalRequests      int64
	successfulRequests int64
	droppedRequests    int64
	totalTokens        int64
	totalBatches       int64
	totalBatchedItems  int64
	kvCacheHits        int64
	kvCacheMisses      int64

	// Batch distribution counters
	batchDistMu sync.RWMutex
	batchSizes  map[string]int64

	// TTFT sliding window reservoir
	ttftMu      sync.RWMutex
	ttftSamples []float64
	maxSamples  int

	// Rate tracking (QPS and Token throughput)
	rateMu        sync.Mutex
	lastTick      time.Time
	lastReqCount  int64
	lastTokCount  int64
	currentQPS    float64
	currentTokSec float64

	// Memory baseline
	startMemStats runtime.MemStats
}

// NewMetricsCollector initializes a new high-performance collector.
func NewMetricsCollector() *MetricsCollector {
	mc := &MetricsCollector{
		maxSamples: 1000,
		batchSizes: map[string]int64{
			"1":     0,
			"2-7":   0,
			"8-15":  0,
			"16-31": 0,
			"32+":   0,
		},
		lastTick:    time.Now(),
		ttftSamples: make([]float64, 0, 1000),
	}
	runtime.ReadMemStats(&mc.startMemStats)
	return mc
}

// IncQueueDepth increments queue depth gauge.
func (m *MetricsCollector) IncQueueDepth() {
	atomic.AddInt64(&m.queueDepth, 1)
}

// DecQueueDepth decrements queue depth gauge.
func (m *MetricsCollector) DecQueueDepth() {
	atomic.AddInt64(&m.queueDepth, -1)
}

// SetQueueDepth sets queue depth gauge explicitly.
func (m *MetricsCollector) SetQueueDepth(val int64) {
	atomic.StoreInt64(&m.queueDepth, val)
}

// IncActiveRequests increments active requests gauge.
func (m *MetricsCollector) IncActiveRequests() {
	atomic.AddInt64(&m.activeRequests, 1)
	atomic.AddInt64(&m.totalRequests, 1)
}

// DecActiveRequests decrements active requests gauge and marks success.
func (m *MetricsCollector) DecActiveRequests(success bool) {
	atomic.AddInt64(&m.activeRequests, -1)
	if success {
		atomic.AddInt64(&m.successfulRequests, 1)
	}
}

// IncDroppedRequests increments dropped requests counter (e.g. 429 queue full).
func (m *MetricsCollector) IncDroppedRequests() {
	atomic.AddInt64(&m.droppedRequests, 1)
	atomic.AddInt64(&m.totalRequests, 1)
}

// AddTokens increments total tokens generated.
func (m *MetricsCollector) AddTokens(n int64) {
	atomic.AddInt64(&m.totalTokens, n)
}

// RecordTTFT records a Time-To-First-Token measurement in milliseconds.
func (m *MetricsCollector) RecordTTFT(duration time.Duration) {
	ms := float64(duration.Microseconds()) / 1000.0
	m.ttftMu.Lock()
	if len(m.ttftSamples) >= m.maxSamples {
		// overwrite earliest
		m.ttftSamples = m.ttftSamples[1:]
	}
	m.ttftSamples = append(m.ttftSamples, ms)
	m.ttftMu.Unlock()
}

// RecordBatch records a batch execution of size n.
func (m *MetricsCollector) RecordBatch(size int) {
	atomic.AddInt64(&m.totalBatches, 1)
	atomic.AddInt64(&m.totalBatchedItems, int64(size))

	bucket := "1"
	switch {
	case size == 1:
		bucket = "1"
	case size >= 2 && size <= 7:
		bucket = "2-7"
	case size >= 8 && size <= 15:
		bucket = "8-15"
	case size >= 16 && size <= 31:
		bucket = "16-31"
	default:
		bucket = "32+"
	}

	m.batchDistMu.Lock()
	m.batchSizes[bucket]++
	m.batchDistMu.Unlock()
}

// RecordKVCacheHit records a hit or miss for KV-cache affinity.
func (m *MetricsCollector) RecordKVCacheHit(hit bool) {
	if hit {
		atomic.AddInt64(&m.kvCacheHits, 1)
	} else {
		atomic.AddInt64(&m.kvCacheMisses, 1)
	}
}

// UpdateRates calculates moving QPS and Token/sec rates.
func (m *MetricsCollector) UpdateRates() {
	m.rateMu.Lock()
	defer m.rateMu.Unlock()

	now := time.Now()
	elapsed := now.Sub(m.lastTick).Seconds()
	if elapsed <= 0.05 {
		return
	}

	currReq := atomic.LoadInt64(&m.totalRequests)
	currTok := atomic.LoadInt64(&m.totalTokens)

	reqDelta := currReq - m.lastReqCount
	tokDelta := currTok - m.lastTokCount

	if reqDelta < 0 {
		reqDelta = 0
	}
	if tokDelta < 0 {
		tokDelta = 0
	}

	// Exponential moving average smoothing: 70% current, 30% new
	instantQPS := float64(reqDelta) / elapsed
	instantTok := float64(tokDelta) / elapsed

	m.currentQPS = 0.6*m.currentQPS + 0.4*instantQPS
	m.currentTokSec = 0.6*m.currentTokSec + 0.4*instantTok

	m.lastTick = now
	m.lastReqCount = currReq
	m.lastTokCount = currTok
}

// Snapshot returns a current view of all collected metrics.
func (m *MetricsCollector) Snapshot() Snapshot {
	m.UpdateRates()

	// Memory metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// TTFT percentiles
	m.ttftMu.RLock()
	samplesCount := len(m.ttftSamples)
	var median, p90, p99, avg float64
	if samplesCount > 0 {
		sorted := make([]float64, samplesCount)
		copy(sorted, m.ttftSamples)
		m.ttftMu.RUnlock()

		sort.Float64s(sorted)
		var sum float64
		for _, v := range sorted {
			sum += v
		}
		avg = sum / float64(samplesCount)
		median = percentile(sorted, 50)
		p90 = percentile(sorted, 90)
		p99 = percentile(sorted, 99)
	} else {
		m.ttftMu.RUnlock()
	}

	// Batch distribution copy
	m.batchDistMu.RLock()
	distCopy := make(map[string]int64, len(m.batchSizes))
	for k, v := range m.batchSizes {
		distCopy[k] = v
	}
	m.batchDistMu.RUnlock()

	totalBatches := atomic.LoadInt64(&m.totalBatches)
	totalBatchedItems := atomic.LoadInt64(&m.totalBatchedItems)
	var avgBatchSize float64
	if totalBatches > 0 {
		avgBatchSize = float64(totalBatchedItems) / float64(totalBatches)
	}

	hits := atomic.LoadInt64(&m.kvCacheHits)
	misses := atomic.LoadInt64(&m.kvCacheMisses)
	totalKV := hits + misses
	var kvRatio float64
	if totalKV > 0 {
		kvRatio = float64(hits) / float64(totalKV)
	}

	totalReq := atomic.LoadInt64(&m.totalRequests)
	var allocsPerReq uint64
	if totalReq > 0 && memStats.Mallocs > m.startMemStats.Mallocs {
		allocsPerReq = (memStats.Mallocs - m.startMemStats.Mallocs) / uint64(totalReq)
	}

	qDepth := atomic.LoadInt64(&m.queueDepth)
	if qDepth < 0 {
		qDepth = 0
	}

	return Snapshot{
		Timestamp:          time.Now(),
		QueueDepth:         qDepth,
		ActiveRequests:     atomic.LoadInt64(&m.activeRequests),
		TotalRequests:      totalReq,
		SuccessfulRequests: atomic.LoadInt64(&m.successfulRequests),
		DroppedRequests:    atomic.LoadInt64(&m.droppedRequests),
		CurrentQPS:         math.Round(m.currentQPS*100) / 100,
		TokensPerSecond:    math.Round(m.currentTokSec*100) / 100,
		TotalTokens:        atomic.LoadInt64(&m.totalTokens),
		MedianTTFTMs:       math.Round(median*100) / 100,
		P90TTFTMs:          math.Round(p90*100) / 100,
		P99TTFTMs:          math.Round(p99*100) / 100,
		AverageTTFTMs:      math.Round(avg*100) / 100,
		TotalBatches:       totalBatches,
		AverageBatchSize:   math.Round(avgBatchSize*100) / 100,
		BatchSizeDist:      distCopy,
		KVCacheHits:        hits,
		KVCacheMisses:      misses,
		KVCacheHitRatio:    math.Round(kvRatio*1000) / 1000,
		MemoryAllocMB:      math.Round(float64(memStats.Alloc)/(1024*1024)*100) / 100,
		TotalAllocMB:       math.Round(float64(memStats.TotalAlloc)/(1024*1024)*100) / 100,
		NumGC:              memStats.NumGC,
		HeapInuseMB:        math.Round(float64(memStats.HeapInuse)/(1024*1024)*100) / 100,
		AllocationsPerReq:  allocsPerReq,
	}
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := (pct / 100.0) * float64(len(sorted)-1)
	lower := int(index)
	upper := lower + 1
	weight := index - float64(lower)

	if upper >= len(sorted) {
		return sorted[lower]
	}
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}
