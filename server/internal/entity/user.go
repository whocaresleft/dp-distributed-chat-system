package entity

import (
	"time"

	"gorm.io/gorm"
)

type User struct {
	UUID      string         `gorm:"primaryKey" json:"uuid"`
	Username  string         `gorm:"uniqueIndex:user_tag_index" json:"username"`
	Tag       string         `gorm:"uniqueIndex:user_tag_index" json:"tag"`
	CreatedAt time.Time      `gorm:"not null;index" json:"created-at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted-at"`
	Epoch     uint64         `gorm:"index;default=0" json:"epoch"`

	Secret UserSecret  `gorm:"foreignKey:UserUUID;references:UUID" json:"secrets,omitempty"`
	Groups []ChatGroup `gorm:"many2many:group_members;" json:"groups"`
}
