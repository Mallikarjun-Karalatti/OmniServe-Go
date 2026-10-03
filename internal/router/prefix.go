package router

import (
	"strings"

	"omniserve/internal/scheduler"
)

const (
	// FNV-1a 64-bit constants
	fnvOffsetBasis uint64 = 14695981039346656037
	fnvPrime       uint64 = 1099511628211
)

// ExtractPrefix extracts the system prompt or common conversation prefix from messages or raw prompt.
func ExtractPrefix(messages []scheduler.ChatMessage, rawPrompt string) string {
	// First check system role in chat messages
	for _, msg := range messages {
		if strings.EqualFold(msg.Role, "system") && strings.TrimSpace(msg.Content) != "" {
			return strings.TrimSpace(msg.Content)
		}
	}

	// Next check first message content if system prompt is absent
	if len(messages) > 0 {
		firstContent := strings.TrimSpace(messages[0].Content)
		if len(firstContent) > 0 {
			// Take up to the first 256 runes/characters as prompt prefix
			if len(firstContent) > 256 {
				return firstContent[:256]
			}
			return firstContent
		}
	}

	// Fallback to raw prompt prefix
	trimmed := strings.TrimSpace(rawPrompt)
	if len(trimmed) > 256 {
		return trimmed[:256]
	}
	return trimmed
}

// HashPrefix computes a 64-bit FNV-1a hash of the prefix string.
func HashPrefix(prefix string) uint64 {
	if len(prefix) == 0 {
		return 0
	}
	h := fnvOffsetBasis
	for i := 0; i < len(prefix); i++ {
		h ^= uint64(prefix[i])
		h *= fnvPrime
	}
	return h
}

// ExtractAndHash extracts the prefix and returns both the prefix string and its 64-bit hash.
func ExtractAndHash(messages []scheduler.ChatMessage, rawPrompt string) (string, uint64) {
	prefix := ExtractPrefix(messages, rawPrompt)
	return prefix, HashPrefix(prefix)
}
