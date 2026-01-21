package repository

import (
	"fmt"
	"server/internal/entity"

	"gorm.io/gorm"
)

type UserRepository interface {
	Create(user *entity.User) error

	SoftDelete(uuid string) error

	UpdateUsername(uuid string, username string, newEpoch uint) error
	UpdateTag(uuid string, tag string, newEpoch uint) error
	UpdatePassword(uuid string, hash string, newEpoch uint) error

	GetByUUID(uuid string) (*entity.User, error)
	GetByNameTag(name, tag string) (*entity.User, error)
}

type SQLiteUserRepository struct {
	db *gorm.DB
}

func NewSQLiteUserRepository(db *gorm.DB) UserRepository {
	return &SQLiteUserRepository{db}
}

func (repo *SQLiteUserRepository) Create(user *entity.User) error {
	return repo.db.Create(user).Error
}

func (repo *SQLiteUserRepository) SoftDelete(uuid string) error {
	return repo.db.Where("UUID = ?", uuid).Delete(&entity.User{}).Error
}

func (repo *SQLiteUserRepository) UpdateUsername(uuid string, username string, newEpoch uint) error {
	result := repo.db.Model(&entity.User{}).
		Where("UUID = ? AND Epoch < ?", uuid, newEpoch).
		Updates(map[string]interface{}{
			"Username": username,
			"Epoch":    newEpoch,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("Sorry, username could not be updated. Retry later.")
	}
	return nil
}

func (repo *SQLiteUserRepository) UpdateTag(uuid string, tag string, newEpoch uint) error {
	result := repo.db.Model(&entity.User{}).
		Where("UUID = ? AND Epoch < ?", uuid, newEpoch).
		Updates(map[string]interface{}{
			"Tag":   tag,
			"Epoch": newEpoch,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("Sorry, tag could not be updated. Retry later.")
	}
	return nil
}

func (repo *SQLiteUserRepository) UpdatePassword(uuid string, hash string, newEpoch uint) error {

	return repo.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&entity.User{}).
			Where("UUID = ? AND Epoch < ?", uuid, newEpoch).
			Update("Epoch", newEpoch)

		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 0 {
			return fmt.Errorf("Sorry, password could not be updated. Retry later.")
		}

		result = tx.Model(&entity.UserSecret{}).
			Where("UserUUID = ?", uuid).
			Update("Hash", hash)

		if result.Error != nil {
			return result.Error
		}
		return nil
	})
}

func (repo *SQLiteUserRepository) GetByUUID(uuid string) (*entity.User, error) {
	var user entity.User
	err := repo.db.Where("UUID = ?", uuid).First(&user).Error
	return &user, err
}
func (repo *SQLiteUserRepository) GetByNameTag(username, tag string) (*entity.User, error) {
	var user entity.User
	err := repo.db.Where("Username = ? AND Tag = ?", username, tag).First(&user).Error
	return &user, err
}
