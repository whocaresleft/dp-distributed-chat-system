package repository

import (
	"server/internal/entity"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UserRepository interface {
	Create(user *entity.User) (uint64, error)
	SynCreate(user *entity.User, incomingEpoch uint64) (uint64, error)

	SoftDelete(uuid string) (uint64, error)
	SynSoftDelete(uuid string, incomingEpoch uint64) (uint64, error)

	GetForLogin(name, tag string) (*entity.User, error)

	GetByUUID(uuid string) (*entity.User, error)
	GetByNameTag(name, tag string) (*entity.User, error)
	GetByName(name string) ([]*entity.User, error)
}

type SQLiteUserRepository struct {
	db *gorm.DB
}

func NewSQLiteUserRepository(db *gorm.DB) UserRepository {
	return &SQLiteUserRepository{db}
}

func (repo *SQLiteUserRepository) Create(user *entity.User) (uint64, error) {

	var epoch uint64 = 0
	err := repo.db.Transaction(func(tx *gorm.DB) error {
		var state entity.SystemState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		state.CurrentEpoch++
		if err := tx.Save(&state).Error; err != nil {
			return err
		}

		user.Epoch = state.CurrentEpoch
		epoch = state.CurrentEpoch

		if err := tx.Create(user).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}

func (repo *SQLiteUserRepository) SynCreate(user *entity.User, incomingEpoch uint64) (uint64, error) {

	var epoch uint64 = 0
	err := repo.db.Transaction(func(tx *gorm.DB) error {
		var state entity.SystemState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}

		if incomingEpoch > state.CurrentEpoch {
			state.CurrentEpoch = incomingEpoch
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
		}
		epoch = state.CurrentEpoch
		if err := tx.Create(user).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}

func (repo *SQLiteUserRepository) SoftDelete(uuid string) (uint64, error) {
	var epoch uint64 = 0
	err := repo.db.Transaction(func(tx *gorm.DB) error {
		var state entity.SystemState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}

		state.CurrentEpoch++
		if err := tx.Save(&state).Error; err != nil {
			return err
		}

		epoch = state.CurrentEpoch

		if err := tx.Where("UUID = ?", uuid).Delete(&entity.User{}).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}
func (repo *SQLiteUserRepository) SynSoftDelete(uuid string, incomingEpoch uint64) (uint64, error) {
	var epoch uint64 = 0
	err := repo.db.Transaction(func(tx *gorm.DB) error {
		var state entity.SystemState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if incomingEpoch > state.CurrentEpoch {
			state.CurrentEpoch = incomingEpoch
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
		}
		epoch = state.CurrentEpoch

		if err := tx.Where("UUID = ?", uuid).Delete(&entity.User{}).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}

func (repo *SQLiteUserRepository) GetForLogin(username, tag string) (*entity.User, error) {
	var user entity.User

	if err := repo.db.Preload("Secret").Where("Username = ? AND Tag = ?", username, tag).First(&user).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

func (repo *SQLiteUserRepository) GetByUUID(uuid string) (*entity.User, error) {
	var user entity.User
	err := repo.db.Where("UUID = ?", uuid).First(&user).Error
	return &user, err
}

func (repo *SQLiteUserRepository) GetByNameTag(username, tag string) (*entity.User, error) {
	var user entity.User
	err := repo.db.Where("username = ? AND tag = ?", username, tag).First(&user).Error
	return &user, err
}

func (repo *SQLiteUserRepository) GetByName(username string) ([]*entity.User, error) {
	var users []*entity.User
	err := repo.db.Where("username = ?", username).Find(&users).Error
	return users, err
}
