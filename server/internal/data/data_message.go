package data

import (
	"fmt"
	"server/cluster/node"
	"server/cluster/node/protocol"
)

type MessageType uint8

const (
	REQUEST  MessageType = 0
	RESPONSE MessageType = 1
	SYNC     MessageType = 2
)

type Status uint8

const (
	SUCCESS Status = iota
	FAILURE
	UNDEFINED
)

type Action uint8

const (
	ActionUserRegister Action = iota
	ActionUserLogin
	ActionUserDelete
	ActionUserGetNameTag
	ActionUsersGetName

	ActionGroupCreate
	ActionGroupDelete
	ActionGroupJoin
	ActionGroupRemove

	ActionFriendSend
	ActionFriendRemove

	ActionMessageSend
	ActionMessageRecv
)

type RWFlag bool

const (
	WRITE RWFlag = false
	READ  RWFlag = true
)

var ActionRWFlags = map[Action]RWFlag{
	ActionUserRegister:   WRITE,
	ActionUserLogin:      READ,
	ActionUserDelete:     WRITE,
	ActionUserGetNameTag: READ,
	ActionUsersGetName:   READ,

	ActionGroupCreate: WRITE,
	ActionGroupDelete: WRITE,
	ActionGroupJoin:   WRITE,
	ActionGroupRemove: WRITE,

	ActionMessageSend: WRITE,
	ActionMessageRecv: READ,
}

type DataMessage struct {
	Header          protocol.MessageHeader `json:"header"`
	MessageID       string                 `json:"message-id"`
	OriginNode      node.NodeId            `json:"origin-node"`
	DestinationNode node.NodeId            `json:"destination-node"`
	Type            MessageType            `json:"type"`
	Action          Action                 `json:"action"`
	Status          Status                 `json:"status"`
	Payload         []byte                 `json:"payload"`
	Epoch           uint64                 `json:"epoch"`
	ErrorMessage    string                 `json:"error-message,omitempty"`
}

func newDataMessage(
	header *protocol.MessageHeader,
	messageId string,
	originNode, destinationNode node.NodeId,
	typ MessageType,
	status Status,
	action Action,
	payload []byte,
	epoch uint64,
	errorMsg string,
) *DataMessage {
	return &DataMessage{*header, messageId, originNode, destinationNode, typ, action, status, payload, epoch, errorMsg}
}

func (d *DataMessage) GetHeader() *protocol.MessageHeader {
	return &d.Header
}

func (d *DataMessage) SetHeader(h *protocol.MessageHeader) {
	d.Header = *h
}

func (d *DataMessage) String() string {
	return fmt.Sprintf("Header{%s}, messageId{%s}, Origin Node{%d}, Destination Node{%d}, Type{%v}, Status{%v}, Error Message{%s}, Epoch{%d}, Payload{%s}", d.Header.String(), d.MessageID, d.OriginNode, d.DestinationNode, d.Type, d.Status, d.ErrorMessage, d.Epoch, string(d.Payload))
}

func (d *DataMessage) Clone() protocol.Message {
	newPayload := make([]byte, len(d.Payload))
	copy(newPayload, d.Payload)
	return newDataMessage(
		d.Header.Clone(),
		d.MessageID,
		d.OriginNode,
		d.DestinationNode,
		d.Type,
		d.Status,
		d.Action,
		newPayload,
		d.Epoch,
		d.ErrorMessage,
	)
}
func (d *DataMessage) MarkTimestamp(timestamp uint64) {
	h := d.GetHeader()
	h.MarkTimestamp(timestamp)
}

func NewRequestMessage(h *protocol.MessageHeader, messageId string, origin, destination node.NodeId, action Action, payload []byte, epoch uint64) *DataMessage {
	return newDataMessage(h, messageId, origin, destination, REQUEST, UNDEFINED, action /*scope,*/, payload, epoch, "")
}

func NewResponseMessage(h *protocol.MessageHeader, messageId string, origin, destination node.NodeId, status Status, action Action /*scope Scope,*/, errorMsg string, payload []byte, epoch uint64) *DataMessage {
	return newDataMessage(h, messageId, origin, destination, RESPONSE, status, action /*scope,*/, payload, epoch, errorMsg)
}
