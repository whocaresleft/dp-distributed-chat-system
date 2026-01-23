package entity

import (
	"time"

	"gorm.io/gorm"
)

type ChatGroup struct {
	UUID      string         `gorm:"primaryKey" json:"uuid"`
	Name      string         `gorm:"not null;index" json:"name"`
	CreatedAt time.Time      `gorm:"not null;index" json:"created-at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted-at"`
	Epoch     uint           `gorm:"index;default=0" json:"epoch"`

	Members []User `gorm:"many2many:group_members;" json:"members"`
}
