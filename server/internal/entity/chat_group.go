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
