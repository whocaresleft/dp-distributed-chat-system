package topology

import (
	"fmt"
	"server/cluster/node/protocol"
)

type HeartbeatMessage struct {
	Header protocol.MessageHeader `json:"header"`
}

func NewHeartbeatMessage(h *protocol.MessageHeader) *HeartbeatMessage {
	return &HeartbeatMessage{
		Header: *h,
	}
}

func (h *HeartbeatMessage) GetHeader() *protocol.MessageHeader {
	return &h.Header
}

func (h *HeartbeatMessage) SetHeader(head *protocol.MessageHeader) {
	h.Header = *head
}

func (h *HeartbeatMessage) String() string {
	return fmt.Sprintf("{Header{%s}}", h.Header.String())
}

func (h *HeartbeatMessage) Clone() protocol.Message {
	return NewHeartbeatMessage(h.Header.Clone())
}
func (h *HeartbeatMessage) MarkTimestamp(timestamp uint64) {
	head := h.GetHeader()
	head.MarkTimestamp(timestamp)
}
