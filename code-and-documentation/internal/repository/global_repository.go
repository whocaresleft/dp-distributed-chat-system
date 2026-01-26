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
)

// This repository holds the system Epoch, that is, a counter that is used to trace where we are in time.
// Each write means incrementing the epoch, so it traces how many changes the system has endured.
type GlobalRepository interface {
	Create(*entity.SystemState) error                                                  // Creates a system state
	GetSystemState() (*entity.SystemState, error)                                      // Retrieves the system state
	GetCurrentEpoch() (uint64, error)                                                  // Retrieves the epoch from the system state
	SyncSnapshot(uint64, []*entity.User, []*entity.Message, []*entity.ChatGroup) error // Syncs with the given snapshot, updating its state with this one.
}

// Implementation of the repository using a SQLite DB
type SQLiteGlobalRepository struct {
	db *gorm.DB
}

func NewSQLiteGlobalRepository(db *gorm.DB) GlobalRepository {
	return &SQLiteGlobalRepository{db}
}

func (g *SQLiteGlobalRepository) Create(e *entity.SystemState) error {
	return g.db.Create(e).Error
}

func (g *SQLiteGlobalRepository) GetSystemState() (*entity.SystemState, error) {
	var state *entity.SystemState
	err := g.db.First(&state, 1).Error
	return state, err
}

func (g *SQLiteGlobalRepository) GetCurrentEpoch() (uint64, error) {
	state, err := g.GetSystemState()
	if err != nil {
		return 0, err
	}
	return state.CurrentEpoch, nil
}

func (g *SQLiteGlobalRepository) SyncSnapshot(newEpoch uint64, users []*entity.User, messages []*entity.Message, groups []*entity.ChatGroup) error {

	// Usually a sync is performed by a reader node, after the writer gives it its DB snapshot to make sure its up to date as a replica.
	// Since the writer is considered a 'truthful source', from the election, its DB is the 'right' one.
	// Each record is deleted and all of the snapshot ones are created, actually having a full state replication of the writer's node.
	// Then, after this one, reader receive small snapshots each write operation the writer performs.

	return g.db.Transaction(func(tx *gorm.DB) error {
		tx.Exec("DELETE FROM group_members")
		tx.Exec("DELETE FROM user_secrets")
		tx.Exec("DELETE FROM messages")
		tx.Exec("DELETE FROM users")
		tx.Exec("DELETE FROM chat_groups")

		var state entity.SystemState
		if err := tx.First(&state, 1).Error; err != nil {
			state = entity.SystemState{ID: 1, CurrentEpoch: newEpoch}
			tx.Create(&state)
		} else {
			state.CurrentEpoch = newEpoch
			tx.Save(&state)
		}

		if len(users) > 0 {
			if err := tx.Create(&users).Error; err != nil {
				return err
			}
		}

		if len(groups) > 0 {
			if err := tx.Omit("Members.*").Create(&groups).Error; err != nil {
				return err
			}
		}

		if len(messages) > 0 {
			if err := tx.Create(&messages).Error; err != nil {
				return err
			}
		}

		return nil
	})
}
