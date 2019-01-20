package backends

import (
	"fmt"

	"github.com/go-redis/redis"
)

// RedisBackend implements CeleryBackend interface for Redis
type RedisBackend struct {
	Client    *redis.Client
	QueueName string
}

// Set pushes result back into backend
func (b *RedisBackend) Set(id string, result []byte) error {
	return nil
}

// Get calls API to get asynchronous result
// "GET" "celery-task-meta-bbe9bb43-6e6b-4d7e-90f4-b3af5e3eef2f"
func (b *RedisBackend) Get(id string) ([]byte, error) {
	cmd := b.Client.Get(fmt.Sprintf("celery-task-meta-%s", id))
	res, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	// parse results
	_ = res
	return nil, nil
}
