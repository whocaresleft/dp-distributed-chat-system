package protocol

import "fmt"

// TreeFlags are used to specify an action accompanying the message
type TreeFlags uint8

const (
	TFlags_NONE         TreeFlags = 0b00000000 // No action
	TFlags_EPOCH        TreeFlags = 0b00000001 // (From root) Parent forwarding an incrementing counter to children
	TFlags_NOPARENTREQ  TreeFlags = 0b00000010 // Help request, This node has no parent (shut off or other problem)
	TFlags_NOPARENTREP  TreeFlags = 0b00000100 // Help respose, some node asked for help, I am responding with YES/NO
	TFlags_Q            TreeFlags = 0b00001000 // Q used in the construction of SPT, asking a node to be a child
	TFlags_A            TreeFlags = 0b00010000 // A user in the construction of SPT, responding to Q
	TFLags_INPUTNEXTHOP TreeFlags = 0b00100000 // Tells the parent that a certain input node is reachable through itself
	TFLags_PARENTPORT   TreeFlags = 0b01000000 // Tells the children what is the port to use on the data plane
)

// TreeMessage is a type of message used after an election, to build the SPT.
// They also carry an epoch (to understand how this node is synchronized with parent and children), flags, and a payload
type TreeMessage struct {
	Header MessageHeader `json:"header"`
	Epoch  uint64        `json:"epoch"`
	Flags  TreeFlags     `json:"flags"`
	Body   []string      `json:"body"`
}

func NewTreeMessage(h *MessageHeader, epoch uint64, flags TreeFlags, body []string) *TreeMessage {
	return &TreeMessage{
		Header: *h,
		Epoch:  epoch,
		Flags:  flags,
		Body:   body,
	}
}

func (t *TreeMessage) GetHeader() *MessageHeader {
	return &t.Header
}
func (t *TreeMessage) SetHeader(h *MessageHeader) {
	t.Header = *h
}
func (t *TreeMessage) String() string {
	return fmt.Sprintf("Header{%s}, Epoch{%d}, Flags{%d}, Body{%s}", t.Header.String(), t.Epoch, t.Flags, t.Body)
}
func (t *TreeMessage) Clone() Message {
	newBody := make([]string, len(t.Body))
	copy(newBody, t.Body)
	return &TreeMessage{
		*t.Header.Clone(),
		t.Epoch,
		t.Flags,
		newBody,
	}
}
func (t *TreeMessage) MarkTimestamp(timestamp uint64) {
	h := t.GetHeader()
	h.MarkTimestamp(timestamp)
}
