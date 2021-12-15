package main

import (
	"tests/config"
	"tests/worker"

	"github.com/sirupsen/logrus"
)

// intended to spin up a worker that runs in a separate process (if desired for testing).

// tweak these as needed
const brokerUrl = "amqp://admin:root@localhost:5672//go-worker"
const backendUrl = "redis://localhost:6379/0"

func main() {
	cli, err := config.GetCeleryClient(
		brokerUrl,
		backendUrl,
	)
	if err != nil {
		logrus.Fatal(err)
	}
	worker.RegisterGoFunctions(cli)
	cli.StartWorker()
	logrus.Println("Remote Go-worker started")
	cli.WaitForStopWorker()
}
