package entity

// System state, represented by the couple (ID, CurrentEpoch)
// ID is used only to have a unique record, as continuosly changing epoch, if it were the the primary key, is not ideal.
type SystemState struct {
	ID           uint64 `gorm:"primaryKey"`
	CurrentEpoch uint64 `gorm:"not null;default=0"` // Number of 'WRITING' operations done (either C, D, U as only R does not change the state of the system)
}
