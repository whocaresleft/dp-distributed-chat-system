package implementation

import (
	"server/internal/entity"
	"time"

	"gorm.io/gorm"
)

type SQLiteUserRepository struct {
	db *gorm.DB
}

//func NewSQLiteUserRepository(db *gorm.DB) repository.UserRepository {
//return SQLiteUserRepository{db}
//}

func (repo *SQLiteUserRepository) Create(user *entity.User, password string) error {
	// Begin transaction
	// Try to insert the user into the db
	// If no error is being produces, generate salt, hash the password and insert those too
	return nil
}

func (repo *SQLiteUserRepository) SoftDelete(uuid entity.UUID, deletedAt time.Time) error {
	// Try to set deleted_at date
	return nil
}
