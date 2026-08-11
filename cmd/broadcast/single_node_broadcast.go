package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := maelstrom.NewNode()
	state := State{
		seenMessages: make(map[int]bool),
		pendingBatch: make(map[string][]int),
		retryQueue:   make(map[BatchKey]BatchEntry),
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := BroadcastBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := handleGossip(&state, []int{body.Message}, msg.Src); err != nil {
			return err
		}

		return n.Reply(msg, BroadcastReply{
			Type: "broadcast_ok",
		})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		body := GossipBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := handleGossip(&state, body.Messages, msg.Src); err != nil {
			return err
		}

		return n.Reply(msg, GossipReply{
			Type:    "gossip_ok",
			BatchID: body.BatchId,
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

		state.SetStarTopology(n.ID(), body.Topology)

		return n.Reply(msg, TopologyReply{
			Type: "topology_ok",
		})
	})

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// First, drain the retry queue
				retryMessages := state.RetryQueue()
				for batchKey, batchEntry := range retryMessages {
					if time.Since(batchEntry.SentAt) > 500*time.Millisecond {
						sendGossipRpc(&state, n, batchKey, batchEntry)
					}
				}

				// Second, drain the pending queue. We don't do this first because the messages will end up in the
				// retry queue, which means unnecessary work.
				pendingMessages := state.DrainPendingBatch()
				for dest, messages := range pendingMessages {
					if len(messages) == 0 {
						continue
					}

					batchId := state.NextBatchId(n.ID())
					batchKey := BatchKey{
						Dest:    dest,
						BatchID: batchId,
					}
					batchBody := BatchEntry{
						Messages: messages,
						SentAt:   time.Now(),
					}
					sendGossipRpc(&state, n, batchKey, batchBody)
				}
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	cancel()
}

func handleGossip(state *State, messages []int, src string) error {
	// If this node hasn't seen the message before, process it.
	newMessages := state.FilterAndAddUnseen(messages)
	if len(newMessages) > 0 {
		neighbors := state.Topology()

		for _, neighbor := range neighbors {
			// Only send to a neighbor if the neighbor wasn't the one who sent it.
			if neighbor == src {
				continue
			}

			// Instead of immediately sending, we should add it to a batch that is processed by our
			// goroutine
			// sendGossipRpc(state, n, neighbor, msg)
			state.AddMessagesToPendingQueue(newMessages, neighbor)
		}
	}

	return nil
}

func sendGossipRpc(state *State, n *maelstrom.Node, batchKey BatchKey, batchBody BatchEntry) {
	gossip := GossipBody{
		Type:     "gossip",
		BatchId:  batchKey.BatchID,
		Messages: batchBody.Messages,
	}

	n.RPC(batchKey.Dest, gossip, func(msg maelstrom.Message) error {
		body := GossipReply{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// If we get a response back, don't attempt to retry.
		if body.Type == "gossip_ok" {
			state.RemoveFromRetryQueue(BatchKey{
				Dest:    msg.Src,
				BatchID: body.BatchID,
			})
		}
		return nil
	})

	// Add message to retry queue for now so that we can rery. If the message went through
	// and we retry, the receiving node will filter out the repeated gossip packets and will
	// attempt to reply "gossip_ok" again, at which point we can remove it from the retry queue
	newBody := batchBody
	newBody.SentAt = time.Now()
	state.AddMsgToRetryQueue(batchKey, newBody)
}
