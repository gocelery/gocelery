package gocelery

// CeleryBroker is interface for all brokers
type CeleryBroker interface {
	Set([]byte) error
	Get() error
}

// CeleryBackend is interface for all backends
type CeleryBackend interface {
	Set(id string, result []byte) error
	Get(id string) ([]byte, error) // non-blocking only
}

// CeleryClient is client for running celery
type CeleryClient struct {
	Broker  CeleryBroker
	Backend CeleryBackend
}
