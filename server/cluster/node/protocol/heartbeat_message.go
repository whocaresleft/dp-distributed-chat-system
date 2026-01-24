package protocol

import (
	"fmt"
)

// HeartbeatMessage is a type of message used as heartbeats once nodes become neighbors in the control plane.
// Since their only reason is to get received, they don't carry any information, they are just a wrapper for an header
type HeartbeatMessage struct {
	Header MessageHeader `json:"header"`
}

func NewHeartbeatMessage(h *MessageHeader) *HeartbeatMessage {
	return &HeartbeatMessage{
		Header: *h,
	}
}

func (h *HeartbeatMessage) GetHeader() *MessageHeader {
	return &h.Header
}

func (h *HeartbeatMessage) SetHeader(head *MessageHeader) {
	h.Header = *head
}

func (h *HeartbeatMessage) String() string {
	return fmt.Sprintf("{Header{%s}}", h.Header.String())
}

func (h *HeartbeatMessage) Clone() Message {
	return NewHeartbeatMessage(h.Header.Clone())
}
func (h *HeartbeatMessage) MarkTimestamp(timestamp uint64) {
	head := h.GetHeader()
	head.MarkTimestamp(timestamp)
}
