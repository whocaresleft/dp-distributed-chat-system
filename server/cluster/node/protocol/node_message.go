package protocol

import "fmt"

type MessageType uint8

const (
	Join MessageType = iota
	Election
)

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
	return fmt.Sprintf("Sender{%s}, Destination{%s}, Type{%d}, Timestamp{%d}", h.Sender, h.Destination, h.Type, h.TimeStamp)
}

type Message interface {
	GetHeader() *MessageHeader
	SetHeader(*MessageHeader)
	String() string
}

type JoinMessage struct {
	Header  MessageHeader `json:"header"`
	Address string        `json:"address"`
}

func NewJoinMessage(h *MessageHeader, ip string) *JoinMessage {
	return &JoinMessage{
		Header:  *h,
		Address: ip,
	}
}

func (j *JoinMessage) GetHeader() *MessageHeader {
	return &j.Header
}
func (j *JoinMessage) SetHeader(h *MessageHeader) {
	j.Header = *h
}
func (j *JoinMessage) String() string {
	return fmt.Sprintf("Header{%s}, Address{%s}", j.Header, j.Address)
}
