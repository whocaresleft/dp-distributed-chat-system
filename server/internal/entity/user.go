package entity

import (
	"time"

	"gorm.io/gorm"
)

// User entity for the chat system
// The combination Username, Tag is unique to give a pre 2024 Discord vibe (allowing multiple people to have the same username, different tag).
type User struct {
	UUID      string         `gorm:"primaryKey" json:"uuid"`                     // Unique identifier
	Username  string         `gorm:"uniqueIndex:user_tag_index" json:"username"` // Username, text string
	Tag       string         `gorm:"uniqueIndex:user_tag_index" json:"tag"`      // Tag, it has the following pattern: [0,9]{3,6} (sequence of 3 to 6 decimal numbers)
	CreatedAt time.Time      `gorm:"not null;index" json:"created-at"`           // Time of creation. This is relative to the single Writer in the system
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted-at"`                    // Time of soft deletion. This is relative to the single Writer in the system
	Epoch     uint64         `gorm:"index;default=0" json:"epoch"`               // Epoch of last modification of the user

	Secret UserSecret   `gorm:"foreignKey:UserUUID;references:UUID" json:"secrets,omitempty"` // Hash of the user, not preloaded by default in queries
	Groups []*ChatGroup `gorm:"many2many:group_members;" json:"groups"`                       // List of groups the user is in
}
