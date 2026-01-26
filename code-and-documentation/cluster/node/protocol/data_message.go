/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package protocol

import (
	"fmt"
	"server/cluster/node"
)

// DataMessageType represents what a node is asking/receiving from another
type DataMessageType uint8

const (
	REQUEST  DataMessageType = iota // Requesting an action
	RESPONSE                        // Response for a previous request
	SYNC_ONE                        // Writer gives an update to its children (Readers)
	SYNC_ALL                        // Writer gives the whole DB snapshot to its children (Readers)
	SNAPSHOT                        // Reader asks Writer for a DB snapshot
)

// DataStatus is the outcome of the operation
type DataStatus uint8

const (
	SUCCESS DataStatus = iota
	FAILURE
	UNDEFINED // Unimportant (e.g. requests)
)

// DataAction is the operations that needs to be carried out by the destination (writer or reader)
type DataAction uint8

const (
	ActionNone           DataAction = iota // No action
	ActionUserRegister                     // Create user
	ActionUserLogin                        // Authenticate user
	ActionUserDelete                       // Delete user
	ActionUserGetNameTag                   // Get user by its name+tag
	ActionUsersGetName                     // Get users with same name

	ActionGroupCreate       // Create group
	ActionGroupDelete       // Delete group
	ActionGroupGetUUID      // Get group by UUID
	ActionGroupMembersGet   // Get group members
	ActionGroupMemberJoin   // Add member to group
	ActionGroupMemberRemove // Remove member from group

	ActionMessageSend      // Send DM message
	ActionMessageRecv      // Recv all DM messages
	ActionGroupMessageSend // Send groupchat message
	ActionGroupMessageRecv // Recv all groupchat messages
)

// RWFlag tells whether the operation is WRITE or READ
type RWFlag bool

const (
	WRITE RWFlag = false
	READ  RWFlag = true
)

// ActionRWFlags maps each action with a WRITE or READ.
// Better for consistency (not forgetting on each message + it's O(1) lookup)
var ActionRWFlags = map[DataAction]RWFlag{
	ActionNone:           READ,
	ActionUserRegister:   WRITE,
	ActionUserLogin:      READ,
	ActionUserDelete:     WRITE,
	ActionUserGetNameTag: READ,
	ActionUsersGetName:   READ,

	ActionGroupCreate:       WRITE,
	ActionGroupDelete:       WRITE,
	ActionGroupMemberJoin:   WRITE,
	ActionGroupMemberRemove: WRITE,
	ActionGroupMembersGet:   READ,
	ActionGroupGetUUID:      READ,

	ActionMessageSend:      WRITE,
	ActionMessageRecv:      READ,
	ActionGroupMessageSend: WRITE,
	ActionGroupMessageRecv: READ,
}

// DataMessage is the only type of message that is used on the data plane. It
// is used to forward DB operations requests to the appropriate persistence nodes.
type DataMessage struct {
	Header          MessageHeader   `json:"header"`
	MessageID       string          `json:"message-id"`
	OriginNode      node.NodeId     `json:"origin-node"`
	DestinationNode node.NodeId     `json:"destination-node"`
	Type            DataMessageType `json:"type"`
	Action          DataAction      `json:"action"`
	Status          DataStatus      `json:"status"`
	Payload         []byte          `json:"payload"`
	Epoch           uint64          `json:"epoch"`
	ErrorMessage    string          `json:"error-message,omitempty"`
}

func newDataMessage(
	header *MessageHeader,
	messageId string,
	originNode, destinationNode node.NodeId,
	typ DataMessageType,
	status DataStatus,
	action DataAction,
	payload []byte,
	epoch uint64,
	errorMsg string,
) *DataMessage {
	return &DataMessage{*header, messageId, originNode, destinationNode, typ, action, status, payload, epoch, errorMsg}
}

func (d *DataMessage) GetHeader() *MessageHeader {
	return &d.Header
}

func (d *DataMessage) SetHeader(h *MessageHeader) {
	d.Header = *h
}

func (d *DataMessage) String() string {
	return fmt.Sprintf("Header{%s}, messageId{%s}, Origin Node{%d}, Destination Node{%d}, Type{%v}, Status{%v}, Error Message{%s}, Epoch{%d}, Payload{%s}", d.Header.String(), d.MessageID, d.OriginNode, d.DestinationNode, d.Type, d.Status, d.ErrorMessage, d.Epoch, string(d.Payload))
}

func (d *DataMessage) Clone() Message {
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

func NewRequestMessage(h *MessageHeader, messageId string, origin, destination node.NodeId, action DataAction, payload []byte, epoch uint64) *DataMessage {
	return newDataMessage(h, messageId, origin, destination, REQUEST, UNDEFINED, action /*scope,*/, payload, epoch, "")
}

func NewResponseMessage(h *MessageHeader, messageId string, origin, destination node.NodeId, status DataStatus, action DataAction /*scope Scope,*/, errorMsg string, payload []byte, epoch uint64) *DataMessage {
	return newDataMessage(h, messageId, origin, destination, RESPONSE, status, action /*scope,*/, payload, epoch, errorMsg)
}
