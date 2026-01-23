package repository

import "server/internal/entity"

type GroupRepository interface {
	Create(group *entity.ChatGroup) error

	SoftDelete(uuid string) error

	GetByUUID(uuid string) (*entity.ChatGroup, error)
}
