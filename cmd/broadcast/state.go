package main

import (
	"maps"
	"sync"
	"time"
)

type State struct {
	mu           sync.RWMutex
	seenMessages map[int]bool
	neighbors    []string
	retryQueue   map[RetryMessage]time.Time
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

func (s *State) Messages() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := make([]int, 0, len(s.seenMessages))
	for k := range s.seenMessages {
		messages = append(messages, k)
	}

	return messages
}

func (s *State) AddNeighbors(neighbors []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.neighbors = neighbors
}

func (s *State) Topology() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	neighbors := make([]string, len(s.neighbors))
	copy(neighbors, s.neighbors)

	return neighbors
}

func (s *State) AddMsgToRetryQueue(msg RetryMessage, timestamp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retryQueue[msg] = timestamp
}

func (s *State) RetryQueue() map[RetryMessage]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := map[RetryMessage]time.Time{}
	maps.Copy(messages, s.retryQueue)

	return messages
}

func (s *State) RemoveFromRetryQueue(msg RetryMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.retryQueue, msg)
}
