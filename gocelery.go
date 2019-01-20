package gocelery

import (
	"github.com/gocelery/gocelery/protocol"
)

// Broker is interface for all brokers
type Broker interface {
	Set([]byte) error
	Get() ([]byte, error)
}

// Backend is interface for all backends
type Backend interface {
	Set(string, []byte) error
	Get(string) ([]byte, error) // non-blocking only
}

// Parser is interface for message parsers
type Parser interface {
	Decode([]byte) (*protocol.TaskMessage, error)
	ContentType() string
}

// Client is client for running celery
type Client struct {
	Broker  Broker
	Backend Backend
}
