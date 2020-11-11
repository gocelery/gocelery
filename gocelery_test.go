// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	uuid "github.com/satori/go.uuid"
)

const TIMEOUT = 2 * time.Second

var (
	redisPool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://")
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}
	redisBroker          = NewRedisCeleryBroker("redis://")
	redisBrokerWithConn  = NewRedisBroker(redisPool)
	redisBackend         = NewRedisCeleryBackend("redis://")
	redisBackendWithConn = NewRedisBackend(redisPool)
	amqpBroker           = NewAMQPCeleryBroker("amqp://")
	amqpBackend          = NewAMQPCeleryBackend("amqp://")
)

// TestNoArg tests successful function execution
// with no argument and valid return value
func TestNoArg(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		expected int
	}{
		{
			name:     "no argument that returns integer value with redis broker/backend ",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: func() int { return 5545 },
			expected: 5545,
		},
		{
			name:     "no argument that returns integer value with redis broker/backend ",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &noArgTask{},
			expected: 1,
		},
		{
			name:     "no argument that returns integer value with amqp broker/backend ",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: func() int { return 6930 },
			expected: 6930,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		// json always return float64 intead of int
		if tc.expected != int(res.(float64)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestInteger tests successful function execution
// with integer arguments and return value
func TestInteger(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      int
		inB      int
		expected int
	}{
		{
			name:     "integer addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addInt,
			inA:      2485,
			inB:      6468,
			expected: 8953,
		},
		{
			name:     "integer addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addInt,
			inA:      2485,
			inB:      6468,
			expected: 8953,
		},
		{
			name:     "integer addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addInt,
			inA:      2485,
			inB:      6468,
			expected: 8953,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		// json always return float64 intead of int
		if tc.expected != int(res.(float64)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestIntegerNamedArguments tests successful function execution
// with integer arguments and return value
func TestIntegerNamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      int
		inB      int
		expected int
	}{
		{
			name:     "integer addition (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addIntTask{},
			inA:      2485,
			inB:      6468,
			expected: 8953,
		},
		{
			name:     "integer addition (named arguments) with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addIntTask{},
			inA:      2485,
			inB:      6468,
			expected: 8953,
		},
		{
			name:     "integer addition (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addIntTask{},
			inA:      2485,
			inB:      6468,
			expected: 8953,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		// json always return float64 intead of int
		if tc.expected != int(res.(float64)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestString tests successful function execution
// with string arguments and return value
func TestString(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      string
		inB      string
		expected string
	}{
		{
			name:     "string addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStr,
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
		{
			name:     "string addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStr,
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
		{
			name:     "string addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStr,
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(string) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestStringNamedArguments tests successful function execution
// with string arguments and return value
func TestStringNamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      string
		inB      string
		expected string
	}{
		{
			name:     "string addition (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrTask{},
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
		{
			name:     "string addition (named arguments) with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrTask{},
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
		{
			name:     "string addition (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrTask{},
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(string) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestStringInteger tests successful function execution
// with string and integer arguments and string return value
func TestStringInteger(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      string
		inB      int
		expected string
	}{
		{
			name:     "integer and string concatenation with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStrInt,
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
		{
			name:     "integer and string concatenation with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStrInt,
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
		{
			name:     "integer and string concatenation with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStrInt,
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(string) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestStringIntegerNamedArguments tests successful function execution
// with string and integer arguments and string return value
func TestStringIntegerNamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      string
		inB      int
		expected string
	}{
		{
			name:     "integer and string concatenation (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrIntTask{},
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
		{
			name:     "integer and string concatenation (named arguments) with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrIntTask{},
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
		{
			name:     "integer and string concatenation (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrIntTask{},
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(string) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestFloat tests successful function execution
// with float64 arguments and return value
func TestFloat(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      float64
		inB      float64
		expected float64
	}{
		{
			name:     "float addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat,
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		{
			name:     "float addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat,
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		{
			name:     "float addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat,
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(float64) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestFloatNamedArguments tests successful function execution
// with float64 arguments and return value
func TestFloatNamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      float64
		inB      float64
		expected float64
	}{
		{
			name:     "float addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloatTask{},
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		{
			name:     "float addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloatTask{},
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		{
			name:     "float addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloatTask{},
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(float64) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestFloat32 tests successful function execution
// with float32 arguments and return value
// Bug(sickyoon): float32 as an argument throws a panic
// https://github.com/gocelery/gocelery/issues/75
func TestFloat32(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      float32
		inB      float32
		expected float32
	}{
		{
			name:     "float32 addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat32,
			inA:      3.4580,
			inB:      5.3688,
			expected: float32(8.8268),
		},
		{
			name:     "float32 addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat32,
			inA:      3.4580,
			inB:      5.3688,
			expected: float32(8.8268),
		},
		{
			name:     "float32 addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat32,
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != float32(res.(float64)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestFloat32NamedArguments tests successful function execution
// with float32 arguments and return value
// Bug(sickyoon): float32 as an argument throws a panic
// https://github.com/gocelery/gocelery/issues/75
func TestFloat32NamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      float32
		inB      float32
		expected float32
	}{
		{
			name:     "float32 addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloat32Task{},
			inA:      3.4580,
			inB:      5.3688,
			expected: float32(8.8268),
		},
		{
			name:     "float32 addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloat32Task{},
			inA:      3.4580,
			inB:      5.3688,
			expected: float32(8.8268),
		},
		{
			name:     "float32 addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloat32Task{},
			inA:      3.4580,
			inB:      5.3688,
			expected: float32(8.8268),
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != float32(res.(float64)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestBool tests successful function execution
// with boolean arguments and return value
func TestBool(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      bool
		inB      bool
		expected bool
	}{
		{
			name:     "boolean and operation with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: andBool,
			inA:      true,
			inB:      false,
			expected: false,
		},
		{
			name:     "boolean and operation with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: andBool,
			inA:      true,
			inB:      false,
			expected: false,
		},
		{
			name:     "boolean and operation with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: andBool,
			inA:      true,
			inB:      true,
			expected: true,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(bool) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestBoolNamedArguments tests successful function execution
// with boolean arguments and return value
func TestBoolNamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      bool
		inB      bool
		expected bool
	}{
		{
			name:     "boolean and operation (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &andBoolTask{},
			inA:      true,
			inB:      false,
			expected: false,
		},
		{
			name:     "boolean and operation (named arguments) with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &andBoolTask{},
			inA:      true,
			inB:      false,
			expected: false,
		},
		{
			name:     "boolean and operation (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &andBoolTask{},
			inA:      true,
			inB:      true,
			expected: true,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != res.(bool) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestArrayIntNamedArguments tests successful function execution
// with array arguments and integer return value
func TestArrayIntNamedArguments(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      []string
		inB      []string
		expected int
	}{
		{
			name:     "maximum array length (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &maxArrLenTask{},
			inA:      []string{"a", "b", "c", "d"},
			inB:      []string{"e", "f", "g", "h"},
			expected: 4,
		},
		{
			name:     "maximum array length (named arguments) with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &maxArrLenTask{},
			inA:      []string{"a", "b", "c", "d"},
			inB:      []string{"e", "f", "g", "h"},
			expected: 4,
		},
		{
			name:     "maximum array length (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &maxArrLenTask{},
			inA:      []string{"a", "b", "c", "d"},
			inB:      []string{"e", "f", "g", "h"},
			expected: 4,
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.DelayKwargs(
			tc.taskName,
			map[string]interface{}{
				"a": tc.inA,
				"b": tc.inB,
			},
		)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if tc.expected != int(res.(float64)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestArray tests successful function execution
// with array arguments and return value
func TestArray(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      []string
		inB      []string
		expected []string
	}{
		{
			name:     "array addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addArr,
			inA:      []string{"a", "b", "c", "d"},
			inB:      []string{"e", "f", "g", "h"},
			expected: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		},
		{
			name:     "array addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addArr,
			inA:      []string{"a", "b", "c", "d"},
			inB:      []string{"e", "f", "g", "h"},
			expected: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		},
		{
			name:     "array addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addArr,
			inA:      []string{"a", "b", "c", "d"},
			inB:      []string{"e", "f", "g", "h"},
			expected: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if !reflect.DeepEqual(tc.expected, convertInterfaceToStringSlice(res)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// TestMap tests successful function execution
// with map arguments and return value
func TestMap(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      map[string]string
		inB      map[string]string
		expected map[string]string
	}{
		{
			name:     "integer addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addMap,
			inA:      map[string]string{"a": "a"},
			inB:      map[string]string{"b": "b"},
			expected: map[string]string{"a": "a", "b": "b"},
		},
		{
			name:     "integer addition with redis broker/backend with connection",
			broker:   redisBrokerWithConn,
			backend:  redisBackendWithConn,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addMap,
			inA:      map[string]string{"a": "a"},
			inB:      map[string]string{"b": "b"},
			expected: map[string]string{"a": "a", "b": "b"},
		},
		{
			name:     "integer addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addMap,
			inA:      map[string]string{"a": "a"},
			inB:      map[string]string{"b": "b"},
			expected: map[string]string{"a": "a", "b": "b"},
		},
	}
	for _, tc := range testCases {
		cli, _ := NewCeleryClient(tc.broker, tc.backend, 1)
		cli.Register(tc.taskName, tc.taskFunc)
		cli.StartWorker()
		asyncResult, err := cli.Delay(tc.taskName, tc.inA, tc.inB)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
			cli.StopWorker()
			continue
		}
		if !reflect.DeepEqual(tc.expected, convertInterfaceToStringMap(res)) {
			t.Errorf("test '%s': returned result %+v is different from expected result %+v", tc.name, res, tc.expected)
		}
		cli.StopWorker()
	}
}

// noArgTask accepts no function arguments
type noArgTask struct{}

// ParseKwargs does nothing for noArgTask since there are no arguments to process
func (a *noArgTask) ParseKwargs(kwargs map[string]interface{}) error {
	return nil
}

// RunTask executes noArgTask example by returning hard-coded integer value 1
func (a *noArgTask) RunTask() (interface{}, error) {
	return 1, nil
}

// addInt returns sum of two integers
func addInt(a, b int) int {
	return a + b
}

// addIntTask returns sum of two integers
type addIntTask struct {
	a int
	b int
}

// ParseKwargs parses named arguments for addIntTask example
func (a *addIntTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	a.a = int(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	a.b = int(kwargBFloat)
	return nil
}

// RunTask executes addIntTask example
func (a *addIntTask) RunTask() (interface{}, error) {
	result := a.a + a.b
	return result, nil
}

// addStr concatenates two given strings
func addStr(a, b string) string {
	return a + b
}

// addStrTask concatenates two given strings
type addStrTask struct {
	a string
	b string
}

func (a *addStrTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	a.a, ok = kwargA.(string)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	a.b, ok = kwargB.(string)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	return nil
}

func (a *addStrTask) RunTask() (interface{}, error) {
	return a.a + a.b, nil
}

// addStrInt concatenates string and integer
func addStrInt(a string, b int) string {
	return a + strconv.Itoa(b)
}

// addStrIntTask concatenates string and integer
type addStrIntTask struct {
	a string
	b int
}

func (a *addStrIntTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	a.a, ok = kwargA.(string)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	a.b = int(kwargBFloat)
	return nil
}

func (a *addStrIntTask) RunTask() (interface{}, error) {
	return a.a + strconv.Itoa(a.b), nil
}

// addFloat returns sum of two float64 values
func addFloat(a, b float64) float64 {
	return a + b
}

// addFloatTask returns sum of two float64 values
type addFloatTask struct {
	a float64
	b float64
}

func (a *addFloatTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	a.a, ok = kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	a.b, ok = kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	return nil
}

func (a *addFloatTask) RunTask() (interface{}, error) {
	return a.a + a.b, nil
}

// Bug(sickyoon): float32 as am argument throws a panic
// https://github.com/gocelery/gocelery/issues/75

// addFloat32 returns sum of two float32 values
func addFloat32(a, b float32) float32 {
	return a + b
}

// addFloat32Task returns sum of two float32 values
type addFloat32Task struct {
	a float32
	b float32
}

func (a *addFloat32Task) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	a.a = float32(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	a.b = float32(kwargBFloat)
	return nil
}

func (a *addFloat32Task) RunTask() (interface{}, error) {
	return a.a + a.b, nil
}

// andBool returns result of and operation of two given boolean values
func andBool(a, b bool) bool {
	return a && b
}

// andBoolTask returns result of and operation of two given boolean values
type andBoolTask struct {
	a bool
	b bool
}

func (a *andBoolTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	a.a, ok = kwargA.(bool)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	a.b, ok = kwargB.(bool)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	return nil

}

func (a *andBoolTask) RunTask() (interface{}, error) {
	return a.a && a.b, nil
}

// maxArrLenTask returns maximum length of two given arrays
type maxArrLenTask struct {
	a []string
	b []string
}

// ParseKwargs parses named arguments for maxArrLenTask
func (m *maxArrLenTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	m.a = convertInterfaceToStringSlice(kwargA)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	m.b = convertInterfaceToStringSlice(kwargB)
	return nil
}

// RunTask returns maximum length of two given arrays from maxArrLenTask struct
func (m *maxArrLenTask) RunTask() (interface{}, error) {
	if len(m.a) > len(m.b) {
		return len(m.a), nil
	}
	return len(m.b), nil
}

// addArr concatenates two slices together
func addArr(a, b []interface{}) []interface{} {
	return append(a, b...)
}

// addMap concatenates two maps together
func addMap(a, b map[string]interface{}) map[string]interface{} {
	c := map[string]interface{}{}
	for k, v := range a {
		c[k] = v
	}
	for k, v := range b {
		c[k] = v
	}
	return c
}

func convertInterfaceToStringSlice(i interface{}) []string {
	is := i.([]interface{})
	stringSlice := make([]string, len(is))
	for i, v := range is {
		stringSlice[i] = fmt.Sprint(v)
	}
	return stringSlice
}

func convertInterfaceToStringMap(i interface{}) map[string]string {
	im := i.(map[string]interface{})
	stringMap := make(map[string]string, len(im))
	for i, v := range im {
		stringMap[i] = fmt.Sprint(v)
	}
	return stringMap
}