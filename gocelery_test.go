package gocelery

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

const TIMEOUT = 2 * time.Second

var (
	redisBroker  = NewRedisCeleryBroker("redis://localhost:6379")
	redisBackend = NewRedisCeleryBackend("redis://localhost:6379")
	amqpBroker   = NewAMQPCeleryBroker("amqp://")
	amqpBackend  = NewAMQPCeleryBackend("amqp://")
)

// TestExecutionSuccess covers test cases
// with successful function execution
func TestExecutionSuccess(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      interface{}
		inB      interface{}
		expected interface{}
	}{
		{
			name:     "integer addition with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addInt,
			inA:      2485,
			inB:      6468,
			expected: 8953.0, // json always return float64
		},
		{
			name:     "integer addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addInt,
			inA:      2485,
			inB:      6468,
			expected: 8953.0, // json always return float64
		},
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
			name:     "string addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStr,
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
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
			name:     "integer and string concatenation with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addStrInt,
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
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
			name:     "float addition with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: addFloat,
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		// Bug(sickyoon): float32 as am argument throws a panic
		// https://github.com/gocelery/gocelery/issues/75
		// {
		// 	name:     "float32 addition with redis broker/backend",
		// 	broker:   redisBroker,
		// 	backend:  redisBackend,
		// 	taskName: uuid.Must(uuid.NewV4()).String(),
		// 	taskFunc: addFloat32,
		// 	inA:      3.4580,
		// 	inB:      5.3688,
		// 	expected: float32(8.8268),
		// },
		// {
		// 	name:     "float32 addition with amqp broker/backend",
		// 	broker:   amqpBroker,
		// 	backend:  amqpBackend,
		// 	taskName: uuid.Must(uuid.NewV4()).String(),
		// 	taskFunc: addFloat32,
		// 	inA:      3.4580,
		// 	inB:      5.3688,
		// 	expected: float32(8.8268),
		// },
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
			return
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get result for task %s: %+v", tc.name, tc.taskName, err)
		}
		if !reflect.DeepEqual(tc.expected, res) {
			t.Errorf("returned result %+v is different from expected result %+v", res, tc.expected)
			return
		}
		cli.StopWorker()
	}
}

// TestExecutionNamedArguments covers test cases
// with successful execution of functions with named arguments (kwargs)
func TestExecutionNamedArgumentsSuccess(t *testing.T) {
	testCases := []struct {
		name     string
		broker   CeleryBroker
		backend  CeleryBackend
		taskName string
		taskFunc interface{}
		inA      interface{}
		inB      interface{}
		expected interface{}
	}{
		{
			name:     "integer addition (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addIntTask{},
			inA:      2485,
			inB:      6468,
			expected: 8953.0, // json always return float64
		},
		{
			name:     "integer addition (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addIntTask{},
			inA:      2485,
			inB:      6468,
			expected: 8953.0, // json always return float64
		},
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
			name:     "string addition (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrTask{},
			inA:      "hello",
			inB:      "world",
			expected: "helloworld",
		},
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
			name:     "integer and string concatenation (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addStrIntTask{},
			inA:      "hello",
			inB:      5,
			expected: "hello5",
		},
		{
			name:     "float addition (named arguments) with redis broker/backend",
			broker:   redisBroker,
			backend:  redisBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloatTask{},
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		{
			name:     "float addition (named arguments) with amqp broker/backend",
			broker:   amqpBroker,
			backend:  amqpBackend,
			taskName: uuid.Must(uuid.NewV4()).String(),
			taskFunc: &addFloatTask{},
			inA:      3.4580,
			inB:      5.3688,
			expected: 8.8268,
		},
		// Bug(sickyoon): float32 as am argument throws a panic
		// https://github.com/gocelery/gocelery/issues/75
		// {
		// 	name:     "float32 addition with redis broker/backend",
		// 	broker:   redisBroker,
		// 	backend:  redisBackend,
		// 	taskName: uuid.Must(uuid.NewV4()).String(),
		// 	taskFunc: &addFloat32Task{},
		// 	inA:      3.4580,
		// 	inB:      5.3688,
		// 	expected: float32(8.8268),
		// },
		// {
		// 	name:     "float32 addition with amqp broker/backend",
		// 	broker:   amqpBroker,
		// 	backend:  amqpBackend,
		// 	taskName: uuid.Must(uuid.NewV4()).String(),
		// 	taskFunc: &addFloat32Task{},
		// 	inA:      3.4580,
		// 	inB:      5.3688,
		// 	expected: float32(8.8268),
		// },
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
			t.Errorf("test '%s': failed to submit request for task: %s: %+v", tc.name, tc.taskName, err)
			return
		}
		res, err := asyncResult.Get(TIMEOUT)
		if err != nil {
			t.Errorf("failed to get result for task %s: %+v", tc.taskName, err)
		}
		if !reflect.DeepEqual(tc.expected, res) {
			t.Errorf("test '%s': returned result %v is different from expected result %v", tc.name, res, tc.expected)
			return
		}
		cli.StopWorker()
	}
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
// func addFloat32(a, b float32) float32 {
// 	return a + b
// }

// addFloat32Task returns sum of two float32 values
// type addFloat32Task struct {
// 	a float32
// 	b float32
// }

// func (a *addFloat32Task) ParseKwargs(kwargs map[string]interface{}) error {
// 	kwargA, ok := kwargs["a"]
// 	if !ok {
// 		return fmt.Errorf("undefined kwarg a")
// 	}
// 	a.a, ok = kwargA.(float32)
// 	if !ok {
// 		return fmt.Errorf("malformed kwarg a")
// 	}
// 	kwargB, ok := kwargs["b"]
// 	if !ok {
// 		return fmt.Errorf("undefined kwarg b")
// 	}
// 	a.b, ok = kwargB.(float32)
// 	if !ok {
// 		return fmt.Errorf("malformed kwarg b")
// 	}
// 	return nil
// }

// func (a *addFloat32Task) RunTask() (interface{}, error) {
// 	return a.a + a.b, nil
// }

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
