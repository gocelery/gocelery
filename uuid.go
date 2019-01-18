package gocelery

import uuid "github.com/satori/go.uuid"

// generateUUID generates a v4 uuid and returns it as a string
func generateUUID() string {
	return uuid.Must(uuid.NewV4(), nil).String()
}
