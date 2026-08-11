package main

import (
	"maps"
	"strconv"
	"sync"
)

type State struct {
	mu           sync.RWMutex
	seenMessages map[int]bool
	neighbors    []string
	pendingBatch map[string][]int
	retryQueue   map[BatchKey]BatchEntry
	counter      uint64
}

func (s *State) AddMessageIfNew(msg int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.seenMessages[msg] {
		return false
	}
	s.seenMessages[msg] = true
	return true
}

func (s *State) FilterAndAddUnseen(incoming []int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newMsgs []int
	for _, msg := range incoming {
		if !s.seenMessages[msg] {
			s.seenMessages[msg] = true
			newMsgs = append(newMsgs, msg)
		}
	}
	return newMsgs
}

func (s *State) Messages() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := make([]int, 0, len(s.seenMessages))
	for k := range s.seenMessages {
		messages = append(messages, k)
	}

	return messages
}

func (s *State) SetStarTopology(nodeId string, fullTopology map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if nodeId == "n0" {
		var neighbors []string
		for peer := range fullTopology {
			if peer != "n0" {
				neighbors = append(neighbors, peer)
			}
		}
		s.neighbors = neighbors
	} else {
		s.neighbors = []string{"n0"}
	}
}

func (s *State) Topology() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	neighbors := make([]string, len(s.neighbors))
	copy(neighbors, s.neighbors)

	return neighbors
}

func (s *State) AddMsgToRetryQueue(key BatchKey, batch BatchEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retryQueue[key] = batch
}

func (s *State) RetryQueue() map[BatchKey]BatchEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := map[BatchKey]BatchEntry{}
	maps.Copy(messages, s.retryQueue)

	return messages
}

func (s *State) RemoveFromRetryQueue(msg BatchKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.retryQueue, msg)
}

func (s *State) AddMessagesToPendingQueue(messages []int, dest string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pendingBatch[dest] = append(s.pendingBatch[dest], messages...)
}

func (s *State) DrainPendingBatch() map[string][]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	messages := s.pendingBatch
	s.pendingBatch = map[string][]int{}

	return messages
}

func (s *State) NextBatchId(nodeId string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	batchId := nodeId + "-" + strconv.FormatUint(s.counter, 10)
	s.counter++

	return batchId
}
