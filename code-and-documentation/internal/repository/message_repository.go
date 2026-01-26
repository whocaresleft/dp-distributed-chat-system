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

// This repository is used to manipulate the messages in the system. It allows CR_D (Create, Read and Delete operations) on the messages.
// uint64's are used since the write opearations change the system's epoch, while syncing operations apply the received one.
type MessageRepository interface {
	Create(message *entity.Message) (uint64, error)                          // Inserts a message in the group
	SynCreate(message *entity.Message, incomingEpoch uint64) (uint64, error) // Synchronizes Create (for replicas)

	Get(chatID string) ([]*entity.Message, error) // Retrives the messages from the chat with the given ID
	GetAll() ([]*entity.Message, error)           // Retrives all the messages
}

// Implementation of the repository using a SQLite DB
type SQLiteMessageRepository struct {
	db *gorm.DB
}

func NewSQLiteMessageRepository(db *gorm.DB) MessageRepository {
	return &SQLiteMessageRepository{db}
}

func (repo *SQLiteMessageRepository) Create(message *entity.Message) (uint64, error) {

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

		message.Epoch = state.CurrentEpoch
		epoch = state.CurrentEpoch

		if err := tx.Create(message).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}

func (repo *SQLiteMessageRepository) SynCreate(message *entity.Message, incomingEpoch uint64) (uint64, error) {

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
		if err := tx.Create(message).Error; err != nil {
			return err
		}
		return nil
	})

	return epoch, err
}

func (repo *SQLiteMessageRepository) Get(chatID string) ([]*entity.Message, error) {
	var messages []*entity.Message
	err := repo.db.Where("chat_id = ?", chatID).Order("created_at ASC").Find(&messages).Error
	return messages, err
}

func (repo *SQLiteMessageRepository) GetAll() ([]*entity.Message, error) {
	var messages []*entity.Message
	err := repo.db.Find(&messages).Error
	return messages, err
}
