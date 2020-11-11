package gocelery

import (
	"encoding/json"
	"fmt"

	// "golang.org/x/net/context"

	"github.com/go-redis/redis/v7"
)

// RedisClusterCeleryBackend is celery backend for rediscluster
type RedisClusterCeleryBackend struct {
	*redis.ClusterClient
}

// NewRedisClusterBackend creates new RedisClusterCeleryBackend with given redis cluster pool.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisClusterBackend(conn *redis.ClusterClient) *RedisClusterCeleryBackend {
	return &RedisClusterCeleryBackend{
		ClusterClient: conn,
	}
}

// NewRedisClusterCeleryBackend creates new RedisClusterCeleryBackend
//
// Deprecated: NewRedisCeleryBackend exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
func NewRedisClusterCeleryBackend(uri string) *RedisClusterCeleryBackend {
	return &RedisClusterCeleryBackend{}
}

// GetResult queries redis backend to get asynchronous result
func (cb RedisClusterCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	key := "celery-task-meta-" + taskID
	strcmd := cb.ClusterClient.Get(key)
	val, err := strcmd.Bytes()

	if err != nil {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage ResultMessage
	err = json.Unmarshal(val, &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb RedisClusterCeleryBackend) SetResult(taskID string, result *ResultMessage) error {
	key := "celery-task-meta-" + taskID
	statuscmd := cb.ClusterClient.Set(key, result, 86400)
	_, err := statuscmd.Result()
	return err
}
