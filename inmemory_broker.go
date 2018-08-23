package gocelery

import "sync"

type InMemoryBroker struct {
	taskQueue []*TaskMessage
	lock sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{make([]*TaskMessage, 0), sync.RWMutex{}}
}

func (b *InMemoryBroker) SendCeleryMessage(m *CeleryMessage) error {
	// this could be a bit too inefficient, for we can do sharding or use sync.Map later
	b.lock.Lock()
	b.taskQueue = append(b.taskQueue, m.GetTaskMessage())
	b.lock.Unlock()
	return nil
}

func (b *InMemoryBroker) GetTaskMessage() (t *TaskMessage, e error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(b.taskQueue) == 0 {
		return nil, nil
	}
	t, b.taskQueue = b.taskQueue[0], b.taskQueue[1:]
	return t, nil
}

func (b *InMemoryBroker) Clear() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.taskQueue = make([]*TaskMessage, 0)
	return nil
}

func (b *InMemoryBroker) isEmpty() (bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return len(b.taskQueue) == 0
}
