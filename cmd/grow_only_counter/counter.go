package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddBody struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddReply struct {
	Type string `json:"type"`
}

type ReadBody struct {
	Type string `json:"type"`
}

type ReadReply struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

type Server struct {
	*maelstrom.Node
	kv         *maelstrom.KV
	totalCount atomic.Int64
}

func main() {
	node := maelstrom.NewNode()
	server := Server{
		Node: node,
		kv:   maelstrom.NewSeqKV(node),
	}

	server.Handle("add", func(msg maelstrom.Message) error {
		body := AddBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Simply add the delta in the message to the total count stored in the server.
		// This total is the sum of all values seen.
		server.totalCount.Add(int64(body.Delta))

		// Immediately return an ok response. No need to write as a background process
		// will be handling the write process.
		return server.Reply(msg, AddReply{
			Type: "add_ok",
		})
	})

	server.Handle("read", func(msg maelstrom.Message) error {
		body := ReadBody{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Force a flush of OUR own local memory to seq-kv right now
		// so any peer reading our key gets our absolute latest count.
		// This is mostly because maelstrom will send a "read" request to
		// all nodes at the end of testing to get the values from each node.
		// It could be that this reequest happens in an interval where the most
		// recent flush hasn't happened, so instead if we get a read request,
		// go ahead and flush out latest value to kv so all nodes who got a read
		// will have each others latest values.
		currTotal := server.totalCount.Load()
		attemptCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_ = server.kv.Write(attemptCtx, server.ID(), currTotal)
		cancel()

		var globalSum int
		for _, nodeID := range server.NodeIDs() {
			// Instead of making a network call to see what the accumulated sum is for this node, just
			// use the value in totalCount
			if nodeID == server.ID() {
				globalSum += int(server.totalCount.Load())
			} else {
				val := readIntWithRetry(context.Background(), server.kv, nodeID)
				globalSum += val
			}
		}

		respBody := ReadReply{
			Type:  "read_ok",
			Value: globalSum,
		}

		return server.Reply(msg, respBody)
	})

	go server.backgroundFlusher(context.Background())

	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}

func readIntWithRetry(parentCtx context.Context, kv *maelstrom.KV, nodeKey string) int {
	backoff := 50 * time.Millisecond
	maxBackoff := 500 * time.Millisecond

	for {
		attemptCtx, cancel := context.WithTimeout(parentCtx, 150*time.Millisecond)
		// Get the current value of this node's key. IF the key hasn't been created yet,
		// set currVal to 0.
		val, err := kv.ReadInt(attemptCtx, nodeKey)
		// When this call returns, cancel the attempt context immediately so we don't waste
		// time waiting for the context to finish. Also, don't use defer here since that only calls
		// at the function end, not the loop end.
		cancel()

		if err == nil {
			return val
		}

		// If the error is because the key doesn't exists, just return 0 with no error.
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return 0
		}

		select {
		case <-time.After(backoff):
		case <-parentCtx.Done():
			return 0
		}
		backoff = min(maxBackoff, backoff*2)
	}
}

func (s *Server) backgroundFlusher(parentCtx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	lastFlushedValue := -1

	for {
		select {
		case <-ticker.C:
			// If the node id hasn't been set yet, just continue
			if s.ID() == "" {
				continue
			}

			// Atomically read the total count so far
			currTotal := s.totalCount.Load()
			if currTotal == int64(lastFlushedValue) {
				continue
			}

			// Attempt to write the total count. If it fails, no need to retry, it will attempt again on the next tick.
			attemptCtx, cancel := context.WithTimeout(parentCtx, 100*time.Millisecond)
			err := s.kv.Write(attemptCtx, s.ID(), currTotal)
			cancel()

			if err == nil {
				lastFlushedValue = int(currTotal)
			}

		case <-parentCtx.Done():
			return
		}
	}
}

// ========================================================================
// NOTE: The commented code below is from a previous attempt in which retry logic was used when a write didn't
// happen. This used backoff to continue to retry the write until it succeeded. This worked fine, but I prefer
// the solution above in which each node kept a running sum of all the numbers it saw and then periodically flushed
// that total to the kv store. This reduced the reliance on retries in the write path.

// n.Handle("add", func(msg maelstrom.Message) error {
// 	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
// 	defer cancel()

// 	body := AddBody{}

// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
// 		return err
// 	}

// 	updateCounter(ctx, n.ID(), kv, body.Delta)

// 	return n.Reply(msg, AddReply{
// 		Type: "add_ok",
// 	})
// })

// n.Handle("read", func(msg maelstrom.Message) error {
// 	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
// 	defer cancel()

// 	body := ReadyBody{}

// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
// 		return err
// 	}

// 	var currVal int
// 	// For each node, we need to fetch the counters for that key and add it to a running total.
// 	// The global count will be the total after all node keys have been read.
// 	for _, node := range n.NodeIDs() {
// 		val := readIntWithRetry(ctx, kv, node)
// 		currVal += val
// 	}

// 	resp := ReadReply{
// 		Type:  "read_ok",
// 		Value: currVal,
// 	}

// 	return n.Reply(msg, resp)
// })

// func updateCounter(parentCtx context.Context, nodeKey string, kv *maelstrom.KV, delta int) {
// 	backoff := 50 * time.Millisecond
// 	maxBackoff := 500 * time.Millisecond

// 	currVal := readIntWithRetry(parentCtx, kv, nodeKey)
// 	for {
// 		attemptCtx, cancel := context.WithTimeout(parentCtx, 150*time.Millisecond)
// 		// Since each node has its own key in the global store, we don't need
// 		// to worry about key contention, so just do a write.
// 		err := kv.Write(attemptCtx, nodeKey, currVal+delta)
// 		cancel()

// 		if err == nil {
// 			return
// 		}

// 		// If the write didn't succeed, sleep for the backoff time period, update backoff,
// 		// and then continue loop to retry the write.
// 		select {
// 		case <-time.After(backoff):
// 		case <-parentCtx.Done():
// 			return // Abort if parent context was canceled
// 		}
// 		backoff = min(maxBackoff, backoff*2)
// 		continue
// 	}
// }
