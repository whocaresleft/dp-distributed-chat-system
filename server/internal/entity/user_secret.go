package entity

type UserSecret struct {
	UserUUID string `gorm:"primaryKey" json:"-"`
	Hash     string `gorm:"not null; index" json:"hash"`
}
