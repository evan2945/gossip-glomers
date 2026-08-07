package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var counter uint64

	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = n.ID() + "-" + strconv.FormatUint(counter, 10)

		atomic.AddUint64(&counter, 1)

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
