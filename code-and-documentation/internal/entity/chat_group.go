/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package entity

import (
	"time"

	"gorm.io/gorm"
)

// Group entity for the chat system
type ChatGroup struct {
	UUID      string         `gorm:"primaryKey" json:"uuid"`           // Unique identifier
	Name      string         `gorm:"not null;index" json:"name"`       // Name of the group chat
	CreatedAt time.Time      `gorm:"not null;index" json:"created-at"` // Time of creation. This is relative to the single Writer in the system
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted-at"`          // Time of soft deletion. This is relative to the single Writer in the system
	Epoch     uint64         `gorm:"index;default=0" json:"epoch"`     // Epoch of last modification of the group

	Members []*User `gorm:"many2many:group_members;" json:"members"` // List of users inside the group
}
