package gocelery

import "github.com/gofrs/uuid"

// generateUUID generates a v4 uuid and returns it as a string
func generateUUID() string {
	return uuid.Must(uuid.NewV4()).String()
}
