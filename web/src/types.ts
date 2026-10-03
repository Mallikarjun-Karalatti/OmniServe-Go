export interface WorkerSnapshot {
  id: string;
  url: string;
  state: 'Healthy' | 'Degraded' | 'Dead';
  active_requests: number;
  total_requests: number;
  kv_cache_hits: number;
  kv_cache_hit_ratio: number;
  average_latency_ms: number;
  circuit_tripped: boolean;
}

export interface TelemetrySnapshot {
  timestamp: string;
  queue_depth: number;
  active_requests: number;
  total_requests: number;
  successful_requests: number;
  dropped_requests: number;
  current_qps: number;
  tokens_per_second: number;
  total_tokens: number;
  median_ttft_ms: number;
  p90_ttft_ms: number;
  p99_ttft_ms: number;
  average_ttft_ms: number;
  total_batches: number;
  average_batch_size: number;
  batch_size_distribution: Record<string, number>;
  kv_cache_hits: number;
  kv_cache_misses: number;
  kv_cache_hit_ratio: number;
  memory_alloc_mb: number;
  total_alloc_mb: number;
  num_gc: number;
  heap_inuse_mb: number;
  allocs_per_request: number;
  workers?: WorkerSnapshot[];
}

export interface ChatMessage {
  role: 'system' | 'user' | 'assistant';
  content: string;
}
