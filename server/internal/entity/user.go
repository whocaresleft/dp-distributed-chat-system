package entity

import (
	"time"

	"gorm.io/gorm"
)

type UUID string

type User struct {
	UUID      UUID           `gorm:"primaryKey"`
	Username  string         `gorm:"uniqueIndex:user_tag_index"`
	Tag       string         `gorm:"uniqueIndex:user_tag_index"`
	CreatedAt time.Time      `gorm:"not null;index"`
	DeletedAt gorm.DeletedAt `gorm:"index"`
	Epoch     uint           `gorm:"index;default=0"`

	Secret   UserSecret    `gorm:"foreignKey:UserUUID;references:UUID"`
	Sessions []UserSession `gorm:"foreignKey:UserUUID;references:UUID"`
}
