// goworker is celery worker implementation in Go
// that demonstrates common use cases
// you can start goworker by running this command
//
//    go run example/goworker/main.go
//
// and run python unittest for celery client
// to ensure everything is in working order
//
//    python test_client.py
//
// list of common use cases covered
//
//    1) integer addition
//    2) integer addition with named arguments
//    3) string addition
//    4) integer and string concatenation
//    5) float addition
//    6) maximum array length
//    7) array addition
//    8) dictionary addition
//    9) boolean and operation
//
package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gocelery/gocelery"
)

func main() {

	// initialize celery client
	redisURI := "redis://localhost:6379"
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisCeleryBroker(redisURI),  // broker
		gocelery.NewRedisCeleryBackend(redisURI), // backend
		2,                                        // number of workers
	)

	// register tasks

	// 1) integer addition
	cli.Register("worker.add_int", addInt)

	// 2) integer addition with named arguments
	cli.Register("worker.add_int_kwargs", &addIntTask{})

	// 3) string addition
	cli.Register("worker.add_str", addStr)

	// 4) integer and string concatenation
	cli.Register("worker.add_str_int", addStrInt)

	// 5) float addition
	cli.Register("worker.add_float", addFloat)

	// 6) maximum array length
	cli.Register("worker.max_arr_len", &maxArrLenTask{})

	// 7) array addition
	cli.Register("worker.add_arr", addArr)

	// 8) dictionary addition
	cli.Register("worker.add_dict", addDict)

	// 9) boolean and operation
	cli.Register("worker.and_bool", andBool)

	// start worker pool
	go cli.StartWorker()

	// gracefully terminate after 1 minute
	time.Sleep(60 * time.Second)
	cli.StopWorker()
}

// addInt returns sum of two integers
// 1) integer addition
func addInt(x int, y int) int {
	return x + y
}

// addIntTask returns sum of two integers
// 2) integer addition with named arguments
type addIntTask struct {
	x int
	y int
}

// ParseKwargs parses named arguments for addIntTask example
func (a *addIntTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["x"]
	if !ok {
		return fmt.Errorf("undefined kwarg x")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg x")
	}
	a.x = int(kwargAFloat)
	kwargB, ok := kwargs["y"]
	if !ok {
		return fmt.Errorf("undefined kwarg y")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg y")
	}
	a.y = int(kwargBFloat)
	return nil
}

// RunTask executes addIntTask example
func (a *addIntTask) RunTask() (interface{}, error) {
	result := a.x + a.y
	return result, nil
}

// addStr concatenates two given strings
// 3) string addition
func addStr(a, b string) string {
	return a + b
}

// addStrInt concatenates string and integer
// 4) integer and string concatenation
func addStrInt(a string, b int) string {
	return a + strconv.Itoa(b)
}

// addFloat returns sun of two floating point values
// 5) float addition
func addFloat(a float64, b float64) float64 {
	return a + b
}

// maxArrLenTask returns maximum length of two given arrays
// 6) maximum array length
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
	m.a, ok = kwargA.([]string)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	m.b, ok = kwargB.([]string)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
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
// 7) array addition
func addArr(a, b []interface{}) []interface{} {
	return append(a, b...)
}

// addDict concatenates two maps together
// 8) dictionary addition
func addDict(a, b map[string]interface{}) map[string]interface{} {
	c := map[string]interface{}{}
	for k, v := range a {
		c[k] = v
	}
	for k, v := range b {
		c[k] = v
	}
	return c
}

// andBool returns result of and operation of two given boolean values
// 9) boolean and operation
func andBool(a, b bool) bool {
	return a && b
}
