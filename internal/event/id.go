package event

import "github.com/google/uuid"

// NewID returns a random UUID v4 string.
func NewID() string {
	return uuid.New().String()
}
