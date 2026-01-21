package data

import "server/internal/repository"

type StorageManager struct {
	userRepo repository.UserRepository
}
