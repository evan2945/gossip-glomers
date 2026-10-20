package main

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"maps"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	*maelstrom.Node
	mu        sync.RWMutex
	kv        maelstrom.KV
	nodesOnce sync.Once
	nodeIds   []string
	logs      map[string][]int
	offsets   map[string]int
}

func main() {
	ctx := context.Background()
	n := maelstrom.NewNode()

	s := Server{
		Node:    n,
		kv:      *maelstrom.NewLinKV(n),
		logs:    make(map[string][]int),
		offsets: make(map[string]int),
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		body := SendBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body.Key
		owner := s.getOwner(key)

		// Check if this message should be processed by this node. If not, we should send a synchronous
		// RPC request to the owning node so that we can respond to the client once the owning node has
		// processed the message.
		if owner != s.ID() {
			resp, err := s.SyncRPC(ctx, owner, msg.Body)
			if err != nil {
				return err
			}

			var replyBody SendReply
			if err := json.Unmarshal(resp.Body, &replyBody); err != nil {
				return err
			}
			return s.Reply(msg, replyBody)
		}

		// If this node owns the message, process it and store the message in the local log buffer
		s.mu.Lock()
		offset := len(s.logs[key])
		s.logs[key] = append(s.logs[key], body.Msg)
		s.mu.Unlock()

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

		// Map: Owner Node ID -> Sub-map of { key: requested_offset }
		remotePolls := make(map[string]map[string]int)

		finalMsgs := make(map[string][][]int)

		// 1. Partition keys between local vs remote nodes
		for key, offset := range body.Offsets {
			owner := s.getOwner(key)

			if owner == s.ID() {
				// Read directly from local memory for owned keys
				s.mu.RLock()
				logSlice := append([]int(nil), s.logs[key]...)
				s.mu.RUnlock()

				if offset < len(logSlice) {
					end := min(offset+5, len(logSlice))
					keyMsgs := make([][]int, 0, end-offset)
					for i := offset; i < end; i++ {
						keyMsgs = append(keyMsgs, []int{i, logSlice[i]})
					}
					finalMsgs[key] = keyMsgs
				}
			} else {
				// Stage for remote proxying
				if remotePolls[owner] == nil {
					remotePolls[owner] = make(map[string]int)
				}
				remotePolls[owner][key] = offset
			}
		}

		// 2. Synchronously forward targeted sub-polls to owning nodes
		for owner, subOffsets := range remotePolls {
			subReq := PollBody{
				Type:    "poll",
				Offsets: subOffsets,
			}

			resp, err := s.SyncRPC(ctx, owner, subReq)
			if err != nil {
				return err
			}

			var subReply PollReply
			if err := json.Unmarshal(resp.Body, &subReply); err != nil {
				return err
			}

			// Merge remote owner's logs into final response
			maps.Copy(finalMsgs, subReply.Msgs)
		}

		// 3. Return aggregated results back to client
		return n.Reply(msg, PollReply{
			Type: "poll_ok",
			Msgs: finalMsgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := CommitOffsetsBody{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		remoteCommits := make(map[string]map[string]int)

		// 1. Partition commits between local owner vs remote owners
		for key, offset := range body.Offsets {
			owner := s.getOwner(key)

			if owner == s.ID() {
				s.mu.Lock()
				if offset > s.offsets[key] {
					s.offsets[key] = offset
				}
				s.mu.Unlock()
			} else {
				if remoteCommits[owner] == nil {
					remoteCommits[owner] = make(map[string]int)
				}
				remoteCommits[owner][key] = offset
			}
		}

		// 2. Forward remote commits to their respective owning nodes
		for owner, subOffsets := range remoteCommits {
			subReq := CommitOffsetsBody{
				Type:    "commit_offsets",
				Offsets: subOffsets,
			}
			_, err := s.SyncRPC(ctx, owner, subReq)
			if err != nil {
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

		remoteLists := make(map[string][]string)
		committedOffsets := make(map[string]int)

		// 1. Group keys by owner
		for _, key := range body.Keys {
			owner := s.getOwner(key)

			if owner == s.ID() {
				s.mu.RLock()
				if offset, exists := s.offsets[key]; exists {
					committedOffsets[key] = offset
				}
				s.mu.RUnlock()
			} else {
				remoteLists[owner] = append(remoteLists[owner], key)
			}
		}

		// 2. Forward sub-requests for remote keys to their owner nodes
		for owner, keys := range remoteLists {
			subReq := ListCommittedBody{
				Type: "list_committed_offsets",
				Keys: keys,
			}
			resp, err := s.SyncRPC(ctx, owner, subReq)
			if err != nil {
				return err
			}

			var subReply ListCommittedReply
			if err := json.Unmarshal(resp.Body, &subReply); err != nil {
				return err
			}

			maps.Copy(committedOffsets, subReply.Offsets)
		}

		return n.Reply(msg, ListCommittedReply{
			Type:    "list_committed_offsets_ok",
			Offsets: committedOffsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) sortedNodeIds() []string {
	s.nodesOnce.Do(func() {
		ids := s.NodeIDs()

		sorted := make([]string, len(ids))
		copy(sorted, ids)

		sort.Strings(sorted)
		s.nodeIds = sorted
	})
	return s.nodeIds
}

func (s *Server) getOwner(key string) string {
	nodes := s.sortedNodeIds()

	h := fnv.New32a()
	h.Write([]byte(key))
	// Map hash value to an index in the sorted list of node IDs
	idx := int(h.Sum32()) % len(nodes)

	return nodes[idx]
}
