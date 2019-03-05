package gocelery

func ExampleStartCeleryWorkers() {
	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisCeleryBroker("redis://"),
		NewRedisCeleryBackend("redis://"),
		1,
	)
	add := func(a, b int) int {
		return a + b
	}
	// register task
	cli.Register("add", add)
	// start workers
	cli.StartWorkerWithContext()
}

func ExampleClient() {

}
