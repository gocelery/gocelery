package gocelery

import "errors"

type InMemoryBackend struct {
	ResultStore map[string]*ResultMessage
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{make(map[string]*ResultMessage)}
}

func (b *InMemoryBackend) GetResult(taskID string) (*ResultMessage, error) {
	if res, ok := b.ResultStore[taskID]; ok {
		return res, nil
	}
	return nil, errors.New("task does not exist")
}

func (b *InMemoryBackend) SetResult(taskID string, result *ResultMessage) error {
	b.ResultStore[taskID] = result
	return nil
}

func (b *InMemoryBackend) Clear() {
	b.ResultStore = make(map[string]*ResultMessage)
}
