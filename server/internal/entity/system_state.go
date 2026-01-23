package entity

type SystemState struct {
	ID           uint64 `gorm:"primaryKey"`
	CurrentEpoch uint64 `gorm:"not null;default=0"`
}
