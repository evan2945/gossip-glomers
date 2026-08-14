package main

type SendBody struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type SendReply struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type PollBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollReply struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

type CommitOffsetsBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsReply struct {
	Type string `json:"type"`
}

type ListCommittedBody struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedReply struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}
