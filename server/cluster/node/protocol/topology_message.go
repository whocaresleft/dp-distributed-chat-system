package protocol

import (
	"fmt"
	"server/cluster/node"
)

// Jflags are used to determine the step in the connection process
type Jflags uint8

const (
	Jflags_JOIN   Jflags = 0b00000001
	Jflags_ACK    Jflags = 0b00000010
	Jflags_REJOIN Jflags = 0b00000100
)

// TopologyMessage is a type of message used to send a neighbor a logical connection request.
// They also carry the IP address, for the network layer, and some flags
type TopologyMessage struct {
	Header  MessageHeader `json:"header"`
	Address node.Address  `json:"address"` // Ip address of the node (so the other knows how to reach it)
	Flags   Jflags        `json:"flags"`   // Flags telling the step in the connection process (Join -> Join-Ack -> Ack)
}

func NewTopologyMessage(h *MessageHeader, address node.Address, flags Jflags) *TopologyMessage {
	return &TopologyMessage{
		Header:  *h,
		Address: address,
		Flags:   flags,
	}
}

func (j *TopologyMessage) GetHeader() *MessageHeader {
	return &j.Header
}
func (j *TopologyMessage) SetHeader(h *MessageHeader) {
	j.Header = *h
}
func (j *TopologyMessage) String() string {
	return fmt.Sprintf("Header{%s}, Address{%v}, Flags{%v}", j.Header.String(), j.Address, j.Flags)
}
func (j *TopologyMessage) Clone() Message {
	return &TopologyMessage{
		*j.Header.Clone(),
		j.Address.Clone(),
		j.Flags,
	}
}
func (j *TopologyMessage) MarkTimestamp(timestamp uint64) {
	h := j.GetHeader()
	h.MarkTimestamp(timestamp)
}
