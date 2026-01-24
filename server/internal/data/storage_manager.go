package data

import (
	"server/internal/entity"
	"server/internal/repository"
	"sync/atomic"

	"gorm.io/gorm"
)

// Storage manager gathers all the repositories needed for the chat system in a single container.
type StorageManager struct {
	db *gorm.DB // Under the hood we use the SQLite implementation

	cacheEpoch atomic.Uint64 // Epoch of the system, stored in cache, to speed up reads without accessing the DB each time

	// Repositories
	systemRepo  repository.GlobalRepository
	userRepo    repository.UserRepository
	messageRepo repository.MessageRepository
	groupRepo   repository.GroupRepository
}

func NewStorageManager(db *gorm.DB) *StorageManager {
	s := &StorageManager{
		db:         db,
		cacheEpoch: atomic.Uint64{},
	}

	s.systemRepo = repository.NewSQLiteGlobalRepository(db)
	s.userRepo = repository.NewSQLiteUserRepository(db)
	s.messageRepo = repository.NewSQLiteMessageRepository(db)
	s.groupRepo = repository.NewSQLiteGroupRepository(db)

	state, err := s.systemRepo.GetSystemState()
	if err != nil {
		newState := entity.SystemState{ID: 1, CurrentEpoch: 0}
		s.systemRepo.Create(&newState)
		s.cacheEpoch.Store(0)
	} else {
		s.cacheEpoch.Store(state.CurrentEpoch)
	}

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
