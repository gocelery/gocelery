package brokers

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

const (
	defaultQueueName = "celery"
)

// RedisBroker implements CeleryBroker interface for Redis
// connection should be handled by caller
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
func (b *RedisBroker) Get() ([]byte, error) {
	//log.Printf("REDIS getting broker message")
	data, err := b.Client.BRPop(5*time.Second, b.getQueueName(), "1").Result()
	if err != nil {
		return nil, err
	}
	if data[0] != "celery" {
		return nil, fmt.Errorf("not celery data")
	}
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid celery data")
	}
	//log.Printf("REDIS broker got: %v", data[1])
	return []byte(data[1]), nil
}

func (b *RedisBroker) getQueueName() string {
	queueName := b.QueueName
	if queueName == "" {
		queueName = defaultQueueName
	}
	return queueName
}
