package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := State{
		messages:         make(map[string][]int),
		committedOffsets: make(map[string]int),
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		body := SendBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offset := s.AddMessage(body.Key, body.Msg)

		respBody := SendReply{
			Type:   "send_ok",
			Offset: offset,
		}

		return n.Reply(msg, respBody)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		body := PollBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][][]int, len(body.Offsets))

		for key, offset := range body.Offsets {
			// If requesting an offset greater than one that exists in the messages,
			// return an empty slice for that key.
			keyMessages := s.MessagesForKey(key)
			msgs[key] = [][]int{}

			if offset >= len(keyMessages) {
				continue
			}

			// For now, only returning 5 messages from the log. While adding messages to return,
			// ensure that we aren't trying to get an offset that doesn't exist (client requested
			// the most recent 2 logs, so only return those two).
			// maybe we can use something like for _, val := range keyMessagese[offset:min(len(keyMessages-1, offset+5))]
			end := min(offset+5, len(keyMessages))
			for i := offset; i < end; i++ {
				msgs[key] = append(msgs[key], []int{i, keyMessages[i]})
			}
		}

		respBody := PollReply{
			Type: "poll_ok",
			Msgs: msgs,
		}

		return n.Reply(msg, respBody)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := CommitOffsetsBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			s.CommitOffset(key, offset)
		}

		return n.Reply(msg, CommitOffsetsReply{Type: "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body := ListCommittedBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)

		for _, key := range body.Keys {
			if offset, ok := s.CommittedOffsetsForKey(key); ok {
				offsets[key] = offset
			}
		}

		respBody := ListCommittedReply{
			Type:    "list_committed_offsets_ok",
			Offsets: offsets,
		}

		return n.Reply(msg, respBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
