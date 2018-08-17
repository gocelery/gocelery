package gocelery

type InMemoryBroker struct {
	taskQueue []*TaskMessage
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{make([]*TaskMessage, 0)}
}

func (b *InMemoryBroker) SendCeleryMessage(m *CeleryMessage) error {
	b.taskQueue = append(b.taskQueue, m.GetTaskMessage())
	return nil
}

func (b *InMemoryBroker) GetTaskMessage() (t *TaskMessage, e error) {
	if len(b.taskQueue) == 0 {
		return nil, nil
	}
	t, b.taskQueue = b.taskQueue[0], b.taskQueue[1:]
	return t, nil
}

func (b *InMemoryBroker) Clear(m *CeleryMessage) error {
	b.taskQueue = make([]*TaskMessage, 0)
	return nil
}
