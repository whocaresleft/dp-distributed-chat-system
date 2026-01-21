package protocol

import (
	"fmt"
	"server/cluster/node"
)

type MessageType uint8

const (
	Topology MessageType = iota
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

func (t MessageType) String() string {
	return readableType[int(t)]
}

type OutMessage struct {
	DestId  node.NodeId
	Message Message
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
	MarkTimestamp(uint64)
}

type Jflags uint8

const (
	Jflags_JOIN   Jflags = 0b00000001
	Jflags_ACK    Jflags = 0b00000010
	Jflags_REJOIN Jflags = 0b00000100
)

type TopologyMessage struct {
	Header  MessageHeader `json:"header"`
	Address node.Address  `json:"address"`
	Flags   Jflags        `json:"flags"`
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

type TreeFlags uint8

const (
	TFlags_NONE         TreeFlags = 0b00000000
	TFlags_EPOCH        TreeFlags = 0b00000001
	TFlags_NOPARENTREQ  TreeFlags = 0b00000010
	TFlags_NOPARENTREP  TreeFlags = 0b00000100
	TFlags_Q            TreeFlags = 0b00001000
	TFlags_A            TreeFlags = 0b00010000
	TFLags_INPUTNEXTHOP TreeFlags = 0b00100000
	TFLags_PARENTPORT   TreeFlags = 0b01000000
)

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
