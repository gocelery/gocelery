package gocelery

// Error represents an error returned from in memory backend.
type Error string

func (err Error) Error() string {
	return string(err)
}

type InMemoryBackend struct{
	ResultStore map[string]*ResultMessage
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{make(map[string]*ResultMessage)}
}

func (b *InMemoryBackend) GetResult(taskID string) (*ResultMessage, error) {
	if res, ok := b.ResultStore[taskID]; ok {
		return res, nil
	}
	var err Error = "task does not exist"
	return nil, err
}

func (b *InMemoryBackend) SetResult(taskID string, result *ResultMessage) error {
	b.ResultStore[taskID] = result
	return nil
}


