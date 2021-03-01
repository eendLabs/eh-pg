package mocks

import (
	"github.com/google/uuid"
	"time"
)

// Model is a mocked read model, useful in testing.
type Model struct {
	ID        uuid.UUID `db:"id"`
	Version   int       `db:"version"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
}

func (m Model) EntityID() uuid.UUID {
	return m.ID
}
