package repository

import (
	"server/internal/entity"

	"gorm.io/gorm"
)

type GlobalRepository interface {
	Create(*entity.SystemState) error
	GetSystemState() (*entity.SystemState, error)
	GetCurrentEpoch() (uint64, error)
}

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
