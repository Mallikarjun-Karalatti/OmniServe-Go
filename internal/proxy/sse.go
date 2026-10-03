package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"omniserve/internal/scheduler"
)

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

// GetBuffer fetches a clean byte buffer from sync.Pool.
func GetBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutBuffer returns a buffer to sync.Pool.
func PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// OpenAIChatChunkDelta represents content changes in a stream chunk.
type OpenAIChatChunkDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// OpenAIChatChunkChoice represents a choice in an OpenAI chunk.
type OpenAIChatChunkChoice struct {
	Index        int                  `json:"index"`
	Delta        OpenAIChatChunkDelta `json:"delta"`
	FinishReason *string              `json:"finish_reason"`
}

// OpenAIChatChunk is the OpenAI-compatible streaming response payload.
type OpenAIChatChunk struct {
	ID                string                  `json:"id"`
	Object            string                  `json:"object"`
	Created           int64                   `json:"created"`
	Model             string                  `json:"model"`
	SystemFingerprint string                  `json:"system_fingerprint,omitempty"`
	Choices           []OpenAIChatChunkChoice `json:"choices"`
}

// StreamSSEResponse streams token chunks from req.ResponseChan to the client using net/http.Flusher.
func StreamSSEResponse(
	w http.ResponseWriter,
	r *http.Request,
	req *scheduler.Request,
	modelName string,
) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported by client connection", http.StatusInternalServerError)
		return fmt.Errorf("flusher unsupported")
	}

	// Set required SSE streaming headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	createdTimestamp := time.Now().Unix()
	firstChunk := true

	clientCtx := r.Context()

	for {
		select {
		case <-clientCtx.Done():
			// Client disconnected prematurely — cancel upstream inference!
			if req.CancelFunc != nil {
				req.CancelFunc()
			}
			return clientCtx.Err()

		case chunk, ok := <-req.ResponseChan:
			if !ok {
				// Channel closed, write final SSE [DONE]
				buf := GetBuffer()
				buf.WriteString("data: [DONE]\n\n")
				_, _ = w.Write(buf.Bytes())
				flusher.Flush()
				PutBuffer(buf)
				return nil
			}

			if chunk.Err != nil {
				// Write error payload
				buf := GetBuffer()
				errPayload := map[string]any{
					"error": map[string]string{
						"message": chunk.Err.Error(),
						"type":    "server_error",
					},
				}
				data, _ := json.Marshal(errPayload)
				buf.WriteString("data: ")
				buf.Write(data)
				buf.WriteString("\n\n")
				_, _ = w.Write(buf.Bytes())
				flusher.Flush()
				PutBuffer(buf)
				return chunk.Err
			}

			// Format chunk into OpenAI chat completion chunk
			buf := GetBuffer()

			var delta OpenAIChatChunkDelta
			if firstChunk {
				delta.Role = "assistant"
				delta.Content = chunk.Content
				firstChunk = false
			} else {
				delta.Content = chunk.Content
			}

			var finishReason *string
			if chunk.IsLast {
				stop := "stop"
				finishReason = &stop
			}

			respChunk := OpenAIChatChunk{
				ID:                req.ID,
				Object:            "chat.completion.chunk",
				Created:           createdTimestamp,
				Model:             modelName,
				SystemFingerprint: chunk.WorkerID,
				Choices: []OpenAIChatChunkChoice{
					{
						Index:        0,
						Delta:        delta,
						FinishReason: finishReason,
					},
				},
			}

			encoded, err := json.Marshal(respChunk)
			if err == nil {
				buf.WriteString("data: ")
				buf.Write(encoded)
				buf.WriteString("\n\n")
				_, writeErr := w.Write(buf.Bytes())
				if writeErr != nil {
					PutBuffer(buf)
					if req.CancelFunc != nil {
						req.CancelFunc()
					}
					return writeErr
				}
				flusher.Flush()
			}
			PutBuffer(buf)

			if chunk.IsLast {
				// Write standard termination
				doneBuf := GetBuffer()
				doneBuf.WriteString("data: [DONE]\n\n")
				_, _ = w.Write(doneBuf.Bytes())
				flusher.Flush()
				PutBuffer(doneBuf)
				return nil
			}
		}
	}
}

// StreamMetricsSSE streams real-time telemetry snapshots every interval (e.g. 500ms) to clients.
func StreamMetricsSSE(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	interval time.Duration,
	getSnapshot func() any,
) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	clientCtx := r.Context()

	for {
		select {
		case <-clientCtx.Done():
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := getSnapshot()
			buf := GetBuffer()
			data, err := json.Marshal(snapshot)
			if err == nil {
				buf.WriteString("data: ")
				buf.Write(data)
				buf.WriteString("\n\n")
				_, writeErr := w.Write(buf.Bytes())
				if writeErr != nil {
					PutBuffer(buf)
					return
				}
				flusher.Flush()
			}
			PutBuffer(buf)
		}
	}
}
