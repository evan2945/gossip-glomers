package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	mu           sync.RWMutex
	seenMessages []int
	neighbors    []string
}

func (s *State) AddMessage(msg int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seenMessages = append(s.seenMessages, msg)
}

func (s *State) Messages() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := make([]int, len(s.seenMessages))
	copy(messages, s.seenMessages)

	return messages
}

func (s *State) AddNeighbors(neighbors []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.neighbors = neighbors
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}
type BroadcastReply struct {
	Type string `json:"type"`
}

type ReadBody struct {
	Type string `json:"type"`
}

type ReadReply struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
	MsgId    int                 `json:"msg_id"`
}

type TopologyReply struct {
	Type string `json:"type"`
}

func main() {
	n := maelstrom.NewNode()
	state := State{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := BroadcastBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.AddMessage(body.Message)

		return n.Reply(msg, BroadcastReply{
			Type: "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := ReadBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := state.Messages()

		return n.Reply(msg, ReadReply{
			Type:     "read_ok",
			Messages: messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		body := TopologyBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.AddNeighbors(body.Topology[n.ID()])

		return n.Reply(msg, TopologyReply{
			Type: "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
