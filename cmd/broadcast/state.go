package main

import "sync"

type State struct {
	mu           sync.RWMutex
	seenMessages map[int]bool
	neighbors    []string
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
