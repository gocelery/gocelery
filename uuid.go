package gocelery

import uuid "github.com/satori/go.uuid"

// generateUUID generates a v4 uuid and returns it as a string
func generateUUID() string {

	uuid := uuid.NewV4()

	return uuid.String()
}
