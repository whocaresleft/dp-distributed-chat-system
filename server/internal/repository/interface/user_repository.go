package repository

import (
	"server/internal/entity"
	"time"
)

type UserRepository interface {
	Create(user *entity.User, password string) error

	SoftDelete(uuid entity.UUID, deletedAt time.Time) error

	UpdateUsername(uuid entity.UUID, username string) error
	UpdateTag(uuid entity.UUID, tag string) error
	UpdatePassword(uuid entity.UUID, salt, hash string, newEpoch uint) error

	GetByUUID(uuid entity.UUID) (*entity.User, error)
	GetByNameTag(name, tag string) (*entity.User, error)
}
