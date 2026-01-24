package entity

import "time"

// Represents a message sent between two users or in a group chat.
type Message struct {
	UUID      string    `gorm:"primaryKey" json:"uuid"`          // Unique identifier
	ChatID    string    `gorm:"not null" json:"chat-id"`         // Identifier of the Chat. It's <user1-uuid>:<user2-uuid> for DMs and <group-uuid> for group messages.
	Content   string    `gorm:"not null" json:"content"`         // Actual content of the message
	Epoch     uint64    `gorm:"not null;default=0" json:"epoch"` // Epoch of the creation of the message
	CreatedAt time.Time `gorm:"not null" json:"created-at"`      // Time of creation. This is relative to the single Writer in the system.

	IsForGroup bool `gorm:"default:false" json:"is_for_group"` // Flag used to check if the message was sent in a group or in DM.

	SenderUUID   string `gorm:"index" json:"sender"`   // UUID of the user that sent the message
	ReceiverUUID string `gorm:"index" json:"receiver"` // UUID of the user, or group, that received it (hence the previous flag)
}
