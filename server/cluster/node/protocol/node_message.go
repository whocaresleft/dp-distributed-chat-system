package protocol

import (
	"fmt"
)

type MessageType uint8

const (
	Topology MessageType = iota
	Election
	Heartbeat
)

var readableType = []string{
	"Topology",
	"Election",
	"Heartbeat",
}

func (t MessageType) String() string {
	return readableType[int(t)]
}

type MessageHeader struct {
	Sender      string      `json:"sender"`
	Destination string      `json:"destination"`
	Type        MessageType `json:"message-type"`
	TimeStamp   uint64      `json:"timestamp"`
}

func NewMessageHeader(sender, destination string, mType MessageType) *MessageHeader {
	return &MessageHeader{
		sender, destination, mType, 0,
	}
}

func (h *MessageHeader) MarkTimestamp(timestamp uint64) {
	h.TimeStamp = timestamp
}

func (h *MessageHeader) String() string {
	return fmt.Sprintf("Sender{%s}, Destination{%s}, Type{%s}, Timestamp{%d}", h.Sender, h.Destination, h.Type.String(), h.TimeStamp)
}

func (h *MessageHeader) Clone() *MessageHeader {
	return &MessageHeader{
		h.Sender,
		h.Destination,
		h.Type,
		h.TimeStamp,
	}
}

type Message interface {
	GetHeader() *MessageHeader
	SetHeader(*MessageHeader)
	String() string
	Clone() Message
}

const (
	Jflags_JOIN   uint8 = 0b00000001
	Jfags_ACK     uint8 = 0b00000010
	Jflags_REJOIN uint8 = 0b00000100
)

type TopologyMessage struct {
	Header  MessageHeader `json:"header"`
	Address string        `json:"address"`
	Flags   uint8         `json:"flags"`
}

func NewTopologyMessage(h *MessageHeader, ip string, flags uint8) *TopologyMessage {
	return &TopologyMessage{
		Header:  *h,
		Address: ip,
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
	return fmt.Sprintf("Header{%s}, Address{%s}, Flags{%v}", j.Header.String(), j.Address, j.Flags)
}
func (j *TopologyMessage) Clone() Message {
	return &TopologyMessage{
		*j.Header.Clone(),
		j.Address,
		j.Flags,
	}
}

type TreeMessage struct {
	Header MessageHeader `json:"header"`
}

func NewTreeMessage(h *MessageHeader) *TreeMessage {
	return &TreeMessage{
		Header: *h,
	}
}

func (j *TreeMessage) GetHeader() *MessageHeader {
	return &j.Header
}
func (j *TreeMessage) SetHeader(h *MessageHeader) {
	j.Header = *h
}
func (j *TreeMessage) String() string {
	return fmt.Sprintf("Header{%s}", j.Header.String())
}
func (j *TreeMessage) Clone() Message {
	return &TreeMessage{
		*j.Header.Clone(),
	}
}
