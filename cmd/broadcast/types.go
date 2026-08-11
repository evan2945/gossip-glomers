package main

import "time"

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

type GossipReply struct {
	Type    string `json:"type"`
	BatchID string `json:"batch_id"`
}

type GossipBody struct {
	Type     string `json:"type"`
	BatchId  string `json:"batch_id"`
	Messages []int  `json:"messages"`
}

type BatchKey struct {
	Dest    string `json:"dest"`
	BatchID string `json:"batch_id"`
}

type BatchEntry struct {
	Messages []int     `json:"messages"`
	SentAt   time.Time `json:"sent_at"`
}
