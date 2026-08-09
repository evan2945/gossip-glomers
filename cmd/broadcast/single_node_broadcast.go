package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	state := State{seenMessages: make(map[int]bool)}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := BroadcastBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := handleGossip(&state, n, body.Message, msg.Src); err != nil {
			return err
		}

		return n.Reply(msg, BroadcastReply{
			Type: "broadcast_ok",
		})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		body := BroadcastBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := handleGossip(&state, n, body.Message, msg.Src); err != nil {
			return err
		}

		return nil
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

func handleGossip(state *State, n *maelstrom.Node, msg int, src string) error {
	// If this node hasn't seen the message before, process it.
	if state.AddMessageIfNew(msg) {
		neighbors := state.Topology()

		for _, neighbor := range neighbors {
			// Only send to a neighbor if the neighbor wasn't the one who sent it.
			if neighbor == src {
				continue
			}
			gossip := BroadcastBody{
				Type:    "gossip",
				Message: msg,
			}

			if err := n.Send(neighbor, gossip); err != nil {
				return err
			}
		}
	}

	return nil
}
