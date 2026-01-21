package repository

import "server/internal/entity"

type GroupRepository interface {
	Create(group *entity.Group) error

	SoftDelete(uuid string) error

	UpdateName(uuid string, newEpoch uint) error

	GetByUUID(uuid string) (*entity.Group, error)
}
