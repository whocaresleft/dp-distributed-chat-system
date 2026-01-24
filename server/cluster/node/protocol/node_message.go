package protocol

import (
	"fmt"
	"server/cluster/node"
)

// MessageScope is the topic of the message
type MessageScope uint8

const (
	Topology MessageScope = iota
	Election
	Heartbeat
	Tree
	Data
)

var readableType = []string{
	"Topology",
	"Election",
	"Heartbeat",
	"Tree",
	"Data",
}

// String returns the readable version of the message scope s
func (s MessageScope) String() string {
	return readableType[int(s)]
}

// OutMessage encapsulates a message with a destination id.
// Mainly used on outbout buffers to determine the destination without peeking into the packet.
type OutMessage struct {
	DestId  node.NodeId
	Message Message
}

// MessageHeader is the header that is present in every  Message
// Containing vital information for the nodes to correctly communicatem with and synchronize each other
type MessageHeader struct {
	Sender      string       `json:"sender"`        // Identifier of the sender (node-<id>)
	Destination string       `json:"destination"`   // Identifier of the destination (node-<id>)
	Scope       MessageScope `json:"message-scope"` // Scope of the message
	TimeStamp   uint64       `json:"timestamp"`     // Logical clock timestamp of the message
}

// NewMessageHeader creates and returns a message header with the given information, setting timestamp to 0 by default
func NewMessageHeader(sender, destination string, scope MessageScope) *MessageHeader {
	return &MessageHeader{
		sender, destination, scope, 0,
	}
}

// MarkTimestamp marks the timestamp field in a message header
func (h *MessageHeader) MarkTimestamp(timestamp uint64) {
	h.TimeStamp = timestamp
}

// String returns a readable version of the header
func (h *MessageHeader) String() string {
	return fmt.Sprintf("Sender{%s}, Destination{%s}, Type{%s}, Timestamp{%d}", h.Sender, h.Destination, h.Scope.String(), h.TimeStamp)
}

// Clone creates a deep clone of the header
func (h *MessageHeader) Clone() *MessageHeader {
	return &MessageHeader{
		h.Sender,
		h.Destination,
		h.Scope,
		h.TimeStamp,
	}
}

// Message is any message that can be used to communicate by nodes (thanks to the header)
type Message interface {
	GetHeader() *MessageHeader // Returns header of the message
	SetHeader(*MessageHeader)  // Sets the header
	String() string            // Returns a readable version
	Clone() Message            // Deep clones the message
	MarkTimestamp(uint64)      // Timestamps the (header of the) message
}
