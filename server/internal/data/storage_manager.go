package data

import (
	"fmt"
	"server/internal/entity"
	"server/internal/repository"
	"sync/atomic"

	"gorm.io/gorm"
)

type StorageManager struct {
	db *gorm.DB

	cacheEpoch atomic.Uint64

	systemRepo  repository.GlobalRepository
	userRepo    repository.UserRepository
	groupRepo   repository.GroupRepository
	messageRepo repository.MessageRepository
}

func NewStorageManager(db *gorm.DB) *StorageManager {
	s := &StorageManager{
		db:         db,
		cacheEpoch: atomic.Uint64{},
	}

	s.systemRepo = repository.NewSQLiteGlobalRepository(db)
	s.userRepo = repository.NewSQLiteUserRepository(db)
	s.messageRepo = repository.NewSQLiteMessageRepository(db)

	state, err := s.systemRepo.GetSystemState()
	if err != nil {
		newState := entity.SystemState{ID: 1, CurrentEpoch: 0}
		s.systemRepo.Create(&newState)
		s.cacheEpoch.Store(0)
	} else {
		s.cacheEpoch.Store(state.CurrentEpoch)
	}

	fmt.Print("I HAVE ", s.cacheEpoch.Load())

	return s
}

func (s *StorageManager) UpdateEpochCache(newEpoch uint64) {
	s.cacheEpoch.Store(newEpoch)
}

func (s *StorageManager) GetCachedEpoch() uint64 {
	return s.cacheEpoch.Load()
}

func (s *StorageManager) GetGlobalRepository() repository.GlobalRepository {
	return s.systemRepo
}

func (s *StorageManager) GetUserRepository() repository.UserRepository {
	return s.userRepo
}

func (s *StorageManager) GetGroupRepository() repository.GroupRepository {
	return s.groupRepo
}

func (s *StorageManager) GetMessageRepository() repository.MessageRepository {
	return s.messageRepo
}
