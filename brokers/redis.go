package brokers

import "github.com/gomodule/redigo/redis"

/*
Design Doc

* offload redis pool configuration to user


*/

const (
	defaultQueueName = "celery"
)

// RedisBroker implements CeleryBroker interface for Redis
type RedisBroker struct {
	Pool      *redis.Pool
	QueueName string
}

func (b *RedisBroker) Send() error {
	queueName := b.ensureQueueName()
	// TODO: get payload
	conn := b.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("LPUSH", b.QueueName, payload)
	return err
}

func (b *RedisBroker) Get() error {
	queueName := b.ensureQueueName()
	if queueName == "" {
		queueName = defaultQueueName
	}
	conn := b.Pool.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BLPOP", queueName, "1")
	if err != nil {
		return err
	}
	return nil
}

func (b *RedisBroker) ensureQueueName() string {
	queueName := b.QueueName
	if queueName == "" {
		queueName = defaultQueueName
	}
	return queueName
}
