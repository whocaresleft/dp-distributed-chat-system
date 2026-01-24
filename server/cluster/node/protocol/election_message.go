package protocol

import (
	"fmt"
	"server/cluster/election"
)

// ElectionMessageType is a type of message that is sent during YO-YO
type ElectionMessageType uint8

const (
	Start ElectionMessageType = iota
	Proposal
	Vote
	Leader
)

// ElectionMessage is a type of message used to start, during, and to end an election.
// They also carry the current election id, round, type and a payload
type ElectionMessage struct {
	Header      MessageHeader       `json:"header"`
	MessageType ElectionMessageType `json:"election-type"`
	ElectionId  election.ElectionId `json:"election-id"`
	Body        []string            `json:"body"`
	Round       uint                `json:"round"`
}

func NewElectionMessage(h *MessageHeader, mType ElectionMessageType, electionId election.ElectionId, body []string, roundEpoch uint) *ElectionMessage {
	return &ElectionMessage{
		Header:      *h,
		MessageType: mType,
		ElectionId:  electionId,
		Body:        body,
		Round:       roundEpoch,
	}
}

func (e *ElectionMessage) GetHeader() *MessageHeader {
	return &e.Header
}

func (e *ElectionMessage) SetHeader(h *MessageHeader) {
	e.Header = *h
}

func (e *ElectionMessage) String() string {
	return fmt.Sprintf("Header{%s}, MessageType{%d}, ElectionID{%s}, Round{%d}, Body{%s}", e.Header.String(), uint8(e.MessageType), e.ElectionId, uint64(e.Round), e.Body)
}

func (e *ElectionMessage) Clone() Message {
	newBody := make([]string, len(e.Body))
	copy(newBody, e.Body)
	return NewElectionMessage(e.Header.Clone(), e.MessageType, e.ElectionId, newBody, e.Round)
}

func (e *ElectionMessage) MarkTimestamp(timestamp uint64) {
	h := e.GetHeader()
	h.MarkTimestamp(timestamp)
}
