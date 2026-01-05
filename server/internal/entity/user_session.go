package entity

import (
	"time"

	"gorm.io/gorm"
)

type UserSession struct {
	Token     string         `gorm:"primaryKey"`
	UserEpoch uint           `gorm:"index"`
	CreatedAt time.Time      `gorm:"not null; index"`
	ExpiresAt time.Time      `gorm:"not null; index"`
	RevokedAt gorm.DeletedAt `gorm:"index"`
	UserUUID  UUID           `gorm:"not null; index"`
}
