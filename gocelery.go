package gocelery

type CeleryBroker interface {
	Consume() // get task non-blocking

	SendCeleryMessage(*CeleryMessage) error
	GetTaskMessage() (*TaskMessage, error) // must be non-blocking
}

type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
}
type CeleryClient struct {
	Broker  CeleryBroker
	Backend CeleryBackend
}
