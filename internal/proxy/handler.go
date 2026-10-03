package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"omniserve/internal/cluster"
	"omniserve/internal/router"
	"omniserve/internal/scheduler"
	"omniserve/internal/telemetry"
)

// ChatCompletionRequest represents an incoming OpenAI-compatible JSON payload.
type ChatCompletionRequest struct {
	Model           string                  `json:"model"`
	Messages        []scheduler.ChatMessage `json:"messages"`
	Prompt          string                  `json:"prompt,omitempty"`
	Stream          bool                    `json:"stream"`
	MaxTokens       int                     `json:"max_tokens,omitempty"`
	Temperature     float64                 `json:"temperature,omitempty"`
	Priority        int                     `json:"priority,omitempty"`
	RoutingStrategy string                  `json:"routing_strategy,omitempty"`
}

// ChatCompletionResponse is the non-streaming OpenAI chat completion format.
type ChatCompletionResponse struct {
	ID                string         `json:"id"`
	Object            string         `json:"object"`
	Created           int64          `json:"created"`
	Model             string         `json:"model"`
	SystemFingerprint string         `json:"system_fingerprint,omitempty"`
	Choices           []ChatChoice   `json:"choices"`
	Usage             UsageTelemetry `json:"usage"`
}

// ChatChoice is a single completion alternative.
type ChatChoice struct {
	Index        int                   `json:"index"`
	Message      scheduler.ChatMessage `json:"message"`
	FinishReason string                `json:"finish_reason"`
}

// UsageTelemetry reports token usage.
type UsageTelemetry struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ProxyHandler encapsulates HTTP endpoints for the OmniServe-Go gateway.
type ProxyHandler struct {
	queue    *scheduler.BoundedQueue
	registry *cluster.ClusterRegistry
	metrics  *telemetry.MetricsCollector
	batcher  *scheduler.DynamicBatcher
}

// NewProxyHandler instantiates the HTTP gateway handler.
func NewProxyHandler(
	queue *scheduler.BoundedQueue,
	registry *cluster.ClusterRegistry,
	metrics *telemetry.MetricsCollector,
	batcher *scheduler.DynamicBatcher,
) *ProxyHandler {
	return &ProxyHandler{
		queue:    queue,
		registry: registry,
		metrics:  metrics,
		batcher:  batcher,
	}
}

// RegisterRoutes registers all gateway API and inference endpoints.
func (p *ProxyHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/chat/completions", p.HandleChatCompletions)
	mux.HandleFunc("/api/v1/metrics", p.HandleMetrics)
	mux.HandleFunc("/api/v1/stream/metrics", p.HandleStreamMetrics)
	mux.HandleFunc("/api/v1/workers", p.HandleWorkers)
	mux.HandleFunc("/api/v1/workers/toggle-failure", p.HandleToggleWorkerFailure)
	mux.HandleFunc("/api/v1/config/batcher", p.HandleBatcherConfig)
	mux.HandleFunc("/health", p.HandleHealth)
}

// HandleChatCompletions processes OpenAI-compatible chat completion requests.
func (p *ProxyHandler) HandleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var reqPayload ChatCompletionRequest
	if err := json.Unmarshal(body, &reqPayload); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Strategy header or body override
	routingStrategy := reqPayload.RoutingStrategy
	if routingStrategy == "" {
		routingStrategy = r.Header.Get("X-Routing-Strategy")
	}
	if routingStrategy == "" {
		routingStrategy = "prefix-hash"
	}

	// Priority header or body override
	priority := reqPayload.Priority
	if headerPriority := r.Header.Get("X-Priority"); headerPriority != "" {
		if headerPriority == "high" {
			priority = 10
		}
	}

	if reqPayload.Model == "" {
		reqPayload.Model = "omniserve-llama-3"
	}

	// Extract system prefix and compute 64-bit FNV hash for KV-cache affinity
	prefix, prefixHash := router.ExtractAndHash(reqPayload.Messages, reqPayload.Prompt)

	reqID := generateRequestID()
	reqCtx, cancel := context.WithCancel(r.Context())

	req := &scheduler.Request{
		ID:              reqID,
		Prompt:          prefix,
		Messages:        reqPayload.Messages,
		PrefixHash:      prefixHash,
		ResponseChan:    make(chan scheduler.StreamChunk, 64),
		Context:         reqCtx,
		CancelFunc:      cancel,
		Priority:        priority,
		ArrivalTime:     time.Now(),
		MaxTokens:       reqPayload.MaxTokens,
		Temperature:     reqPayload.Temperature,
		Stream:          reqPayload.Stream,
		RoutingStrategy: routingStrategy,
	}

	// Track active request count
	p.metrics.IncActiveRequests()

	// Push to bounded queue with backpressure (429 on overflow)
	if err := p.queue.Push(req); err != nil {
		p.metrics.DecActiveRequests(false)
		cancel()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": "Gateway queue capacity reached (429 Too Many Requests). Backpressure active.",
				"type":    "rate_limit_error",
				"code":    "queue_capacity_exceeded",
			},
		})
		return
	}

	// SSE Streaming response
	if reqPayload.Stream {
		if err := StreamSSEResponse(w, r, req, reqPayload.Model); err != nil {
			// Streaming ended or client disconnected
			return
		}
		return
	}

	// Non-streaming response: accumulate all tokens
	var sb strings.Builder
	var lastChunk scheduler.StreamChunk
	for chunk := range req.ResponseChan {
		if chunk.Err != nil {
			http.Error(w, chunk.Err.Error(), http.StatusInternalServerError)
			return
		}
		sb.WriteString(chunk.Content)
		lastChunk = chunk
	}

	fullResponse := ChatCompletionResponse{
		ID:                req.ID,
		Object:            "chat.completion",
		Created:           time.Now().Unix(),
		Model:             reqPayload.Model,
		SystemFingerprint: lastChunk.WorkerID,
		Choices: []ChatChoice{
			{
				Index: 0,
				Message: scheduler.ChatMessage{
					Role:    "assistant",
					Content: sb.String(),
				},
				FinishReason: "stop",
			},
		},
		Usage: UsageTelemetry{
			PromptTokens:     len(prefix) / 4,
			CompletionTokens: lastChunk.TotalTokens,
			TotalTokens:      (len(prefix) / 4) + lastChunk.TotalTokens,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fullResponse)
}

// HandleMetrics returns the latest telemetry snapshot as JSON.
func (p *ProxyHandler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := p.metrics.Snapshot()
	snap.Workers = p.registry.Snapshots()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

// HandleStreamMetrics streams telemetry ticks every 500ms via SSE.
func (p *ProxyHandler) HandleStreamMetrics(w http.ResponseWriter, r *http.Request) {
	StreamMetricsSSE(r.Context(), w, r, 500*time.Millisecond, func() any {
		snap := p.metrics.Snapshot()
		snap.Workers = p.registry.Snapshots()
		return snap
	})
}

// HandleWorkers lists all worker node statuses.
func (p *ProxyHandler) HandleWorkers(w http.ResponseWriter, r *http.Request) {
	workers := p.registry.Snapshots()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(workers)
}

// HandleToggleWorkerFailure allows UI/benchmarks to simulate node dropouts or recovery.
func (p *ProxyHandler) HandleToggleWorkerFailure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		WorkerID string `json:"worker_id"`
		Fail     bool   `json:"fail"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := p.registry.ToggleWorkerFailure(payload.WorkerID, payload.Fail)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":   true,
		"worker_id": payload.WorkerID,
		"failed":    payload.Fail,
	})
}

// HandleBatcherConfig allows adjusting batcher parameters at runtime.
func (p *ProxyHandler) HandleBatcherConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var cfg struct {
		MaxBatchSize   int `json:"max_batch_size"`
		BatchTimeoutMs int `json:"batch_timeout_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	p.batcher.UpdateConfig(scheduler.BatchConfig{
		MaxBatchSize: cfg.MaxBatchSize,
		BatchTimeout: time.Duration(cfg.BatchTimeoutMs) * time.Millisecond,
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":          true,
		"max_batch_size":   cfg.MaxBatchSize,
		"batch_timeout_ms": cfg.BatchTimeoutMs,
	})
}

// HandleHealth provides a quick liveness check.
func (p *ProxyHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": "omniserve-go",
	})
}

// WithCORS wraps an http.Handler with cross-origin headers for Vite UI integration.
func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Routing-Strategy, X-Priority")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func generateRequestID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "chatcmpl-" + hex.EncodeToString(b)
}
