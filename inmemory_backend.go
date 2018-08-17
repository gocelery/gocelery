package gocelery

import (
	"errors"
	"sync"
)

type InMemoryBackend struct {
	ResultStore map[string]*ResultMessage
	lock sync.RWMutex
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{make(map[string]*ResultMessage), sync.RWMutex{}}
}

func (b *InMemoryBackend) GetResult(taskID string) (*ResultMessage, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if res, ok := b.ResultStore[taskID]; ok {
		return res, nil
	}
	return nil, errors.New("task does not exist")
}

func (b *InMemoryBackend) SetResult(taskID string, result *ResultMessage) error {
	b.lock.Lock()
	b.ResultStore[taskID] = result
	b.lock.Unlock()
	return nil
}

func (b *InMemoryBackend) Clear() {
	b.ResultStore = make(map[string]*ResultMessage)
}
