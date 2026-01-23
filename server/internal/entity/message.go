package entity

import "time"

type Message struct {
	ChatID    string    `gorm:"primaryKey" json:"chat-id"`
	Content   string    `gorm:"not null" json:"content"`
	Epoch     uint64    `gorm:"not null;default=0" json:"epoch"`
	CreatedAt time.Time `gorm:"not null" json:"created-at"`

	SenderUUID   string `gorm:"index" json:"sender"`
	ReceiverUUID string `gorm:"index" json:"destination"`
}
