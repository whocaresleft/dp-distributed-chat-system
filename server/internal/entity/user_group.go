package entity

import "time"

type UserGroupTable struct {
	UserUUID  string `gorm:"primaryKey"`
	GroupUUID string `gorm:"primaryKey"`
	Admin     bool   `gorm:"defaut:'False'"`
	JoinedAt  time.Time
}
