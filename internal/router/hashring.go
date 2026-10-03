package router

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

var (
	// ErrNoHealthyWorkers is returned when no workers in the ring are available.
	ErrNoHealthyWorkers = errors.New("no healthy workers available in hash ring")
	// ErrRingEmpty is returned when the hash ring has zero workers.
	ErrRingEmpty = errors.New("hash ring is empty")
)

// HashRing implements consistent hashing with virtual nodes for KV-cache affinity.
type HashRing struct {
	mu           sync.RWMutex
	vnodeCount   int
	ring         []uint64          // Sorted slice of virtual node hashes
	nodeMap      map[uint64]string // Map from virtual node hash to WorkerID
	workers      map[string]bool   // Set of registered worker IDs
}

// NewHashRing creates a consistent hash ring with the specified virtual nodes per physical worker.
func NewHashRing(vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = 64
	}
	return &HashRing{
		vnodeCount: vnodes,
		nodeMap:    make(map[uint64]string),
		workers:    make(map[string]bool),
		ring:       make([]uint64, 0),
	}
}

// AddWorker inserts a physical worker node and its virtual nodes onto the ring.
func (hr *HashRing) AddWorker(workerID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.workers[workerID] {
		return
	}
	hr.workers[workerID] = true

	for i := 0; i < hr.vnodeCount; i++ {
		vnodeKey := workerID + "#vnode" + strconv.Itoa(i)
		hash := HashPrefix(vnodeKey)
		hr.ring = append(hr.ring, hash)
		hr.nodeMap[hash] = workerID
	}

	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

// RemoveWorker removes a worker and all of its virtual nodes from the ring.
func (hr *HashRing) RemoveWorker(workerID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if !hr.workers[workerID] {
		return
	}
	delete(hr.workers, workerID)

	newRing := make([]uint64, 0, len(hr.ring)-hr.vnodeCount)
	for _, hash := range hr.ring {
		if hr.nodeMap[hash] == workerID {
			delete(hr.nodeMap, hash)
		} else {
			newRing = append(newRing, hash)
		}
	}
	hr.ring = newRing
}

// GetWorker maps a PrefixHash to the primary consistent hash worker.
// If isHealthy callback is provided and returns false, it traverses clockwise to find
// the next surviving worker (automatic circuit breaker failover).
func (hr *HashRing) GetWorker(prefixHash uint64, isHealthy func(workerID string) bool) (string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return "", ErrRingEmpty
	}

	// Binary search for the first virtual node >= prefixHash
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= prefixHash
	})

	if idx >= len(hr.ring) {
		idx = 0 // Wrap around to the start of the ring
	}

	// If no health filter is provided, return direct hashed node
	if isHealthy == nil {
		hash := hr.ring[idx]
		return hr.nodeMap[hash], nil
	}

	// Traverse clockwise to find the first healthy worker
	checked := make(map[string]bool)
	n := len(hr.ring)
	for i := 0; i < n; i++ {
		currIdx := (idx + i) % n
		hash := hr.ring[currIdx]
		workerID := hr.nodeMap[hash]

		if checked[workerID] {
			continue
		}
		checked[workerID] = true

		if isHealthy(workerID) {
			return workerID, nil
		}
	}

	return "", ErrNoHealthyWorkers
}

// Workers returns all registered physical worker IDs.
func (hr *HashRing) Workers() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	res := make([]string, 0, len(hr.workers))
	for w := range hr.workers {
		res = append(res, w)
	}
	sort.Strings(res)
	return res
}

// DumpRingState outputs summary information about virtual node distribution.
func (hr *HashRing) DumpRingState() string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return fmt.Sprintf("Ring with %d workers and %d virtual nodes", len(hr.workers), len(hr.ring))
}
