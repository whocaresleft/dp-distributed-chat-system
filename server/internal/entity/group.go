package entity

import (
	"time"

	"gorm.io/gorm"
)

type Group struct {
	UUID      string         `gorm:"primaryKey"`
	Name      string         `gorm:"uniqueIndex:user_tag_index"`
	CreatedAt time.Time      `gorm:"not null;index"`
	DeletedAt gorm.DeletedAt `gorm:"index"`
	Epoch     uint           `gorm:"index;default=0"`

	Members []UserGroupTable `gorm:"foreignKey:GroupUUID;references:UUID"`
}
