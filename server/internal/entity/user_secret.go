package entity

// Just the UUID of a user with its hashed password.
// It's stored in a different table so that even when doing SELECT * on a user, the hash is untouched.
type UserSecret struct {
	UserUUID string `gorm:"primaryKey" json:"-"`         // UUID of the user
	Hash     string `gorm:"not null; index" json:"hash"` // Hash of the user's password. (Currently calculated with BCrypt, already salted, default cost used)
}
