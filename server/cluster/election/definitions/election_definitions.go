package election_definitions

import (
	"fmt"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"strconv"
	"strings"
	"time"
)

const Timeout = time.Second * 30

type ElectionVerdict struct {
	LeaderId   node.NodeId
	ElectionId ElectionId
}

type LinkDirection bool
type ElectionId string

const InvalidId ElectionId = ""

type Comparator int8

func (e ElectionId) Compare(other ElectionId) int64 {
	if e == other {
		return 0
	}
	if e == InvalidId {
		return -1
	}
	if other == InvalidId {
		return 1
	}

	clock1, discriminant1, _ := e.Parse()
	clock2, discriminant2, _ := other.Parse()

	if clock1 == clock2 {
		return int64(discriminant2) - int64(discriminant1)
	}
	return int64(clock1 - clock2)
}

func (e ElectionId) Parse() (uint64, node.NodeId, error) {

	clockString, discriminantString, ok := strings.Cut(string(e), "-")
	if !ok {
		return 0, 0, fmt.Errorf("Id was mal-formatted")
	}

	clock, err := strconv.ParseInt(clockString, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	discriminant, err := strconv.ParseInt(discriminantString, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return uint64(clock), node.NodeId(discriminant), nil
}

const ( // Suppose i is this node and j is the other
	Incoming LinkDirection = false // (j, i)
	Outgoing LinkDirection = true  // (i, j)
)

func (l *LinkDirection) Inverse() LinkDirection {
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

var readableStatuses = []string{
	"Source",
	"InternalNode",
	"Sink",
	"Loser",
	"Winner",
}

func (s ElectionStatus) String() string {
	return readableStatuses[uint(s)]
}

////////////////////////////////////////////////

type ElectionState uint8

const (
	Idle ElectionState = iota
	WaitingYoDown
	WaitingYoUp
)

var readableStates = []string{
	"Idle",
	"Waiting Yo Down",
	"Waiting Yo Up",
}

func (s ElectionState) String() string {
	return readableStates[uint(s)]
}

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
	return fmt.Sprintf("Header{%s}, MessageType{%d}, ElectionID{%s}, Round{%d}, Body{%s}", e.Header.String(), uint8(e.MessageType), e.ElectionId, uint64(e.Round), e.Body)
}

func (e *ElectionMessage) Clone() protocol.Message {
	newBody := make([]string, len(e.Body))
	copy(newBody, e.Body)
	return NewElectionMessage(e.Header.Clone(), e.MessageType, e.ElectionId, newBody, e.Round)
}

func (e *ElectionMessage) MarkTimestamp(timestamp uint64) {
	h := e.GetHeader()
	h.MarkTimestamp(timestamp)
}
