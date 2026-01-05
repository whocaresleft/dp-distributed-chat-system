package entity

type UserSecret struct {
	UserUUID UUID   `gorm:"primaryKey"`
	Salt     string `gorm:"not null; index"`
	Hash     string `gorm:"not null; index"`
}
