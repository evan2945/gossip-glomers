package main

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
