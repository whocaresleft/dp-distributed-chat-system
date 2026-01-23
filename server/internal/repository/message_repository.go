package repository

import (
	"server/internal/entity"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MessageRepository interface {
	Create(message *entity.Message) (uint64, error)
	SynCreate(message *entity.Message, incomingEpoch uint64) (uint64, error)

	Get(chatID string) ([]*entity.Message, error)
}

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
	err := repo.db.Where("ChatID = ?", chatID).Order("CreatedAt ASC").Find(&messages).Error
	return messages, err
}
