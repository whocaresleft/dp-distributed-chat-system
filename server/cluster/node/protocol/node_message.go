package protocol

type MessageType uint8

const (
	ElectionJoin MessageType = iota
	ElectionStart
	ElectionProposal
	ElectionVote
	ElectionLeader
)

type Message struct {
	Sender      string      `json:"sender"`
	Destination string      `json:"destination"`
	Type        MessageType `json:"message-type"`
	Body        []string    `json:"value"`
	Round       uint        `json:"round"`
}
