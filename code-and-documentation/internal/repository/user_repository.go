/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package repository

import (
	"server/internal/entity"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// This repository is used to manipulate the users in the system. It allows CR_D (Create, Read and Delete operations) on the users
// uint64's are used since the write opearations change the system's epoch, while syncing operations apply the received one.
type UserRepository interface {
	Create(user *entity.User) (uint64, error)                          // Inserts a user in the repository
	SynCreate(user *entity.User, incomingEpoch uint64) (uint64, error) // Synchronizes the creation of a user (performed by replicas)

	SoftDelete(uuid string) (uint64, error)                          // Soft deletes the user (the record remains, it's just marked deleted)
	SynSoftDelete(uuid string, incomingEpoch uint64) (uint64, error) // Synchronizes the deletion of the user (performed by replicas)

	GetForLogin(name, tag string) (*entity.User, error) // Retrieves the user with given name and tag, it also returns it's hashed password, hence, used for login.

	GetByUUID(uuid string) (*entity.User, error)             // Retrieves the user with the given uuid.
	GetByNameTag(name, tag string) (*entity.User, error)     // Retreives the user with the given name and tag.
	GetByName(name string) ([]*entity.User, error)           // Retrieves the users that share the given name
	GetGroups(name, tag string) ([]*entity.ChatGroup, error) // Retrieves all the groups the user with name and tag is in

	GetAll() ([]*entity.User, error) // Retrieves all the users, WITH their secret
}

// Implementation of the repository using a SQLite DB
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
	err := repo.db.Preload("Groups").Where("username = ? AND tag = ?", username, tag).First(&user).Error
	return &user, err
}

func (repo *SQLiteUserRepository) GetByName(username string) ([]*entity.User, error) {
	var users []*entity.User
	err := repo.db.Where("username = ?", username).Find(&users).Error
	return users, err
}

func (repo *SQLiteUserRepository) GetAll() ([]*entity.User, error) {
	var users []*entity.User
	err := repo.db.Preload("Secret").Find(&users).Error
	return users, err
}

func (repo *SQLiteUserRepository) GetGroups(name, tag string) ([]*entity.ChatGroup, error) {
	var user entity.User
	err := repo.db.Preload("Groups").Where("Username = ? AND Tag = ?", name, tag).First(&user).Error
	return user.Groups, err
}
