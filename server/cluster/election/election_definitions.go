package election

import (
	"fmt"
	"server/cluster/node/protocol"
	"time"
)

const Timeout = time.Second * 30

type LinkDirection bool
type ElectionId string

const InvalidId ElectionId = ""

const ( // Suppose i is this node and j is the other
	Incoming LinkDirection = false // (j, i)
	Outgoing LinkDirection = true  // (i, j)
)

func (l *LinkDirection) inverse() LinkDirection {
	return !(*l)
}

////////////////////////////////////////////////

type ElectionStatus uint8

const (
	Source ElectionStatus = iota
	InternalNode
	Sink
	Loser
	Winner
)

////////////////////////////////////////////////

type ElectionState uint8

const (
	Idle ElectionState = iota
	WaitingYoDown
	WaitingYoUp
)

////////////////////////////////////////////////

type ElectionMessageType uint8

const (
	Start ElectionMessageType = iota
	Proposal
	Vote
	Leader
)

type ElectionMessage struct {
	Header      protocol.MessageHeader `json:"header"`
	MessageType ElectionMessageType    `json:"election-type"`
	ElectionId  ElectionId             `json:"election-id"`
	Body        []string               `json:"body"`
	Round       uint                   `json:"round"`
}

func NewElectionMessage(h *protocol.MessageHeader, mType ElectionMessageType, electionId ElectionId, body []string, roundEpoch uint) *ElectionMessage {
	return &ElectionMessage{
		Header:      *h,
		MessageType: mType,
		ElectionId:  electionId,
		Body:        body,
		Round:       roundEpoch,
	}
}

func (e *ElectionMessage) GetHeader() *protocol.MessageHeader {
	return &e.Header
}

func (e *ElectionMessage) SetHeader(h *protocol.MessageHeader) {
	e.Header = *h
}

func (e *ElectionMessage) String() string {
	return fmt.Sprintf("Header{%s}, MessageType{%d}, ElectionID{%s}, Round{%d}", e.Header.String(), e.MessageType, e.ElectionId, e.Round)
}
