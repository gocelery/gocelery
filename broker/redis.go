package broker

import (
	"time"

	"github.com/go-redis/redis"
)

const (
	defaultQueueName = "celery"
)

// RedisBroker implements CeleryBroker interface for Redis
type RedisBroker struct {
	Client    *redis.Client
	QueueName string
}

// Set -
// "LPUSH" "celery" data
func (b *RedisBroker) Set(payload []byte) error {
	return b.Client.LPush(b.getQueueName(), payload).Err()
}

// Get -
// "BRPOP" "celery" "celery\x06\x163" "celery\x06\x166" "celery\x06\x169" "1"
func (b *RedisBroker) Get() error {
	return b.Client.BRPop(5*time.Second, b.getQueueName(), "1").Err()
}

func (b *RedisBroker) getQueueName() string {
	queueName := b.QueueName
	if queueName == "" {
		queueName = defaultQueueName
	}
	return queueName
}
