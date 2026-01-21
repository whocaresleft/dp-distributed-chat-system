package entity

type UserSecret struct {
	UserUUID string `gorm:"primaryKey"`
	Hash     string `gorm:"not null; index"`
}
