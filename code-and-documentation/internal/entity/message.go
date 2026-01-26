/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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
