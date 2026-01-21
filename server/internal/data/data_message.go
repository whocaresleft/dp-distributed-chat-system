package data

import (
	"fmt"
	"server/cluster/node"
	"server/cluster/node/protocol"
)

type MessageType bool

const (
	REQUEST  MessageType = false
	RESPONSE MessageType = true
)

type Action bool

const (
	WRITE Action = false
	READ  Action = true
)

type Status uint8

const (
	SUCCESS Status = iota
	FAILURE
	UNDEFINED
)

type DataMessage struct {
	Header          protocol.MessageHeader `json:"header"`
	MessageID       string                 `json:"message-id"`
	OriginNode      node.NodeId            `json:"origin-node"`
	DestinationNode node.NodeId            `json:"destination-node"`
	Type            MessageType            `json:"type"`
	Action          Action                 `json:"action"`
	Status          Status                 `json:"status"`
	Payload         []byte                 `json:"payload"`
	ErrorMessage    string                 `json:"error-message,omitempty"`
}

func newDataMessage(header *protocol.MessageHeader, messageId string, originNode, destinationNode node.NodeId, typ MessageType, status Status, action Action, payload []byte, errorMsg string) *DataMessage {
	return &DataMessage{*header, messageId, originNode, destinationNode, typ, action, status, payload, errorMsg}
}

func (d *DataMessage) GetHeader() *protocol.MessageHeader {
	return &d.Header
}

func (d *DataMessage) SetHeader(h *protocol.MessageHeader) {
	d.Header = *h
}

func (d *DataMessage) String() string {
	return fmt.Sprintf("Header{%s}, messageId{%s}, Origin Node{%d}, Destination Node{%d}, Type{%v}, Status{%v}, Error Message{%s}, Payload{%s}", d.Header.String(), d.MessageID, d.OriginNode, d.DestinationNode, d.Type, d.Status, d.ErrorMessage, string(d.Payload))
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
		d.ErrorMessage,
	)
}
func (d *DataMessage) MarkTimestamp(timestamp uint64) {
	h := d.GetHeader()
	h.MarkTimestamp(timestamp)
}

func NewRequestMessage(h *protocol.MessageHeader, messageId string, origin, destination node.NodeId, action Action, payload []byte) *DataMessage {
	return newDataMessage(h, messageId, origin, destination, REQUEST, UNDEFINED, action, payload, "")
}

func NewResponseMessage(h *protocol.MessageHeader, messageId string, origin, destination node.NodeId, status Status, action Action, errorMsg string, payload []byte) *DataMessage {
	return newDataMessage(h, messageId, origin, destination, RESPONSE, status, action, payload, errorMsg)
}
