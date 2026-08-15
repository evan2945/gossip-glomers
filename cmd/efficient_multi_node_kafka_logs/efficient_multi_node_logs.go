package main

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	*maelstrom.Node
	kv        maelstrom.KV
	nodesOnce sync.Once
	nodeIds   []string
}

func main() {
	ctx := context.Background()
	n := maelstrom.NewNode()

	s := Server{
		Node: n,
		kv:   *maelstrom.NewLinKV(n),
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		body := SendBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var offset int
		for {
			var logs []int
			// isNew := false

			isNew, err := readFromStore(ctx, s.kv, "logs:"+body.Key, &logs)
			if err != nil {
				return err
			}

			var newLog []int
			newLog = append(logs, body.Msg)

			err = s.kv.CompareAndSwap(ctx, "logs:"+body.Key, logs, newLog, isNew)
			if err == nil {
				offset = len(newLog) - 1
				break
			}

			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
				continue
			}

			return err
		}

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
			var logs []int
			_, err := readFromStore(ctx, s.kv, "logs:"+key, &logs)

			if err != nil {
				return err
			}

			msgs[key] = [][]int{}

			if offset >= len(logs) {
				continue
			}

			end := min(offset+5, len(logs))
			for i := offset; i < end; i++ {
				msgs[key] = append(msgs[key], []int{i, logs[i]})
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
			for {
				var committedOffset int
				isNew, err := readFromStore(ctx, s.kv, "offsets:"+key, &committedOffset)
				if err != nil {
					return err
				}

				if offset <= committedOffset {
					break
				}

				err = s.kv.CompareAndSwap(ctx, "offsets:"+key, committedOffset, offset, isNew)
				if err == nil {
					break
				}

				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
					continue
				}

				return err
			}
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
			var offset int
			_, err := readFromStore(ctx, s.kv, "offsets:"+key, &offset)
			if err != nil {
				return err
			}

			offsets[key] = offset
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

func readFromStore[T any](ctx context.Context, kv maelstrom.KV, key string, storeInto *T) (bool, error) {
	isNew := false

	err := kv.ReadInto(ctx, key, storeInto)
	if err != nil {
		var readRpcErr *maelstrom.RPCError
		if errors.As(err, &readRpcErr) && readRpcErr.Code == maelstrom.KeyDoesNotExist {
			isNew = true
		} else {
			return isNew, err
		}
	}

	return isNew, nil
}

func (s *Server) sortedNodeIds() []string {
	s.nodesOnce.Do(func() {
		ids := s.NodeIDs()
		sort.Strings(ids)
		s.nodeIds = ids
	})
	return s.nodeIds
}

func getOwner(key string, nodes []string) string {
	h := fnv.New32a()
	h.Write([]byte(key))
	// Map hash value to an index in the sorted list of node IDs
	idx := int(h.Sum32()) % len(nodes)
	return nodes[idx]
}
