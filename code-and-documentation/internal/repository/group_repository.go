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

// This repository is used to manipulate the groups and user-groups relations in the system. It allows CR_D (Create, Read and Delete operations) on the groups
// uint64's are used since the write opearations change the system's epoch, while syncing operations apply the received one.
type GroupRepository interface {
	Create(group *entity.ChatGroup) (uint64, error)                          // Inserts a group in the repository
	SynCreate(group *entity.ChatGroup, incomingEpoch uint64) (uint64, error) // Synchronizes Create (for replicas)

	SoftDelete(uuid string) (uint64, error)                          // Deletes the group with the given uuid.
	SynSoftDelete(uuid string, incomingEpoch uint64) (uint64, error) // Synchronizes SoftDelete (for replicas)

	GetByUUID(uuid string) (*entity.ChatGroup, error)    // Retrieves the group with the given uuid.
	GetByName(name string) ([]*entity.ChatGroup, error)  // Retrieves the groups that share the given name.
	GetPartecipants(uuid string) ([]*entity.User, error) // Retrieves the members of the group with given uuid.
	GetAll() ([]*entity.ChatGroup, error)                // Retrieves all the groups, each WITH the list of members (users)

	AddUser(uuid string, user *entity.User) (uint64, error)                          // Adds a user to the group, the one with given uuid.
	SynAddUser(uuid string, user *entity.User, incomingEpoch uint64) (uint64, error) // Synchronizes AddUser (for replicas)
	RemoveUser(uuid, userUuid string) (uint64, error)                                // Removes the user from the group, the one with the given uuid.
	SynRemoveUser(uuid, userUuid string, incomingEpoch uint64) (uint64, error)       // Synchronizes RemoveUser (for replicas)
}

// Implementation of the repository using a SQLite DB
type SQLiteGroupRepository struct {
	db *gorm.DB
}

func NewSQLiteGroupRepository(db *gorm.DB) GroupRepository {
	return &SQLiteGroupRepository{db}
}

func (repo *SQLiteGroupRepository) Create(group *entity.ChatGroup) (uint64, error) {

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

		group.Epoch = state.CurrentEpoch
		epoch = state.CurrentEpoch

		if err := tx.Omit("Members.*").Create(group).Error; err != nil {
			return err
		}
		return nil
	})
	return epoch, err
}

func (repo *SQLiteGroupRepository) SynCreate(group *entity.ChatGroup, incomingEpoch uint64) (uint64, error) {

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
		if err := tx.Omit("Members.*").Create(group).Error; err != nil {
			return err
		}
		return nil
	})
	return epoch, err
}

func (repo *SQLiteGroupRepository) SoftDelete(uuid string) (uint64, error) {
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

		if err := tx.Where("UUID = ?", uuid).Delete(&entity.ChatGroup{}).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}
func (repo *SQLiteGroupRepository) SynSoftDelete(uuid string, incomingEpoch uint64) (uint64, error) {
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

		if err := tx.Where("UUID = ?", uuid).Delete(&entity.ChatGroup{}).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}

func (repo *SQLiteGroupRepository) GetByUUID(uuid string) (*entity.ChatGroup, error) {
	var group entity.ChatGroup
	err := repo.db.Where("UUID = ?", uuid).First(&group).Error
	return &group, err
}

func (repo *SQLiteGroupRepository) GetByName(name string) ([]*entity.ChatGroup, error) {
	var groups []*entity.ChatGroup
	err := repo.db.Where("name = ?", name).Find(&groups).Error
	return groups, err
}

func (repo *SQLiteGroupRepository) GetPartecipants(uuid string) ([]*entity.User, error) {
	var group entity.ChatGroup
	err := repo.db.Preload("Members").Where("UUID = ?", uuid).First(&group).Error
	return group.Members, err
}
func (repo *SQLiteGroupRepository) GetAll() ([]*entity.ChatGroup, error) {
	var groups []*entity.ChatGroup
	err := repo.db.Preload("Members").Find(&groups).Error
	return groups, err
}

func (repo *SQLiteGroupRepository) AddUser(uuid string, user *entity.User) (uint64, error) {
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

		group := entity.ChatGroup{UUID: uuid}
		if err := tx.Model(&group).Update("Epoch", epoch).Error; err != nil {
			return err
		}

		if err := tx.Model(&group).Association("Members").Append(user); err != nil {
			return err
		}

		return nil
	})
	return epoch, err
}

func (repo *SQLiteGroupRepository) SynAddUser(uuid string, user *entity.User, incomingEpoch uint64) (uint64, error) {
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

		group := entity.ChatGroup{UUID: uuid}
		if err := tx.Model(&group).Update("Epoch", epoch).Error; err != nil {
			return err
		}

		if err := tx.Model(&group).Association("Members").Append(user); err != nil {
			return err
		}
		return nil
	})
	return epoch, err
}
func (repo *SQLiteGroupRepository) RemoveUser(uuid, userUuid string) (uint64, error) {
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

		group := entity.ChatGroup{UUID: uuid}
		user := entity.User{UUID: userUuid}

		if err := tx.Model(&group).Update("Epoch", epoch).Error; err != nil {
			return err
		}

		if err := tx.Model(&group).Association("Members").Delete(&user); err != nil {
			return err
		}
		var count int64
		count = tx.Model(&group).Association("Members").Count()
		if count == 0 {
			if err := tx.Delete(&group).Error; err != nil {
				return err
			}
		}

		return nil
	})
	return epoch, err
}
func (repo *SQLiteGroupRepository) SynRemoveUser(uuid, userUuid string, incomingEpoch uint64) (uint64, error) {
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

		group := entity.ChatGroup{UUID: uuid}
		user := entity.User{UUID: userUuid}

		if err := tx.Model(&group).Update("Epoch", epoch).Error; err != nil {
			return err
		}

		if err := tx.Model(&group).Association("Members").Delete(&user); err != nil {
			return err
		}
		var count int64
		count = tx.Model(&group).Association("Members").Count()
		if count == 0 {
			if err := tx.Delete(&group).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return epoch, err
}
