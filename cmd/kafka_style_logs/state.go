package main

import (
	"maps"
	"sync"
)

type State struct {
	mu               sync.Mutex
	messages         map[string][]int
	committedOffsets map[string]int
}

// Add Message will add a message to the slice for a specified key and return the offset
// of that message.
func (s *State) AddMessage(key string, msg int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[key] = append(s.messages[key], msg)

	return len(s.messages[key]) - 1
}

func (s *State) Messages() map[string][]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	messages := make(map[string][]int, len(s.messages))
	maps.Copy(messages, s.messages)

	return messages
}

func (s *State) MessagesForKey(key string) []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	messages := make([]int, len(s.messages[key]))
	copy(messages, s.messages[key])

	return messages
}

func (s *State) Message(key string, idx int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.messages[key][idx]
}

func (s *State) CommittedOffsetsForKey(key string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset, exists := s.committedOffsets[key]
	return offset, exists
}

func (s *State) CommitOffset(key string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if current, exists := s.committedOffsets[key]; !exists || offset > current {
		s.committedOffsets[key] = offset
	}
}
