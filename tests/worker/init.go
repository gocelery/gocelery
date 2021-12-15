package worker

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"testing"
	"tests/config"

	"github.com/pnongah/gocelery"

	"github.com/sirupsen/logrus"
)

// PythonBin the name of the python binary on the system
const PythonBin = "python3"

func RunGoWorker(t *testing.T, brokerUrl string, backendUrl string) error {
	cli, err := config.GetCeleryClient(brokerUrl, backendUrl)
	if err != nil {
		return err
	}
	RegisterGoFunctions(cli)
	cli.StartWorker()
	logrus.Println("Go-worker started")
	t.Cleanup(cli.StopWorker)
	return nil
}

func RegisterGoFunctions(cli *gocelery.CeleryClient) {
	cli.Register(GoFunc_Add, Add)
	cli.Register(GoFuncKwargs_Add, &adder{})
}

func RunPythonWorker(t *testing.T, args ...string) error {
	pyargs := []string{"worker/main.py"}
	pyargs = append(pyargs, args...)
	cmd := exec.Command(PythonBin, pyargs...)
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("could not start python worker: %v", err)
	}
	collectPythonLogs(stdout, stderr)
	return nil
}

func collectPythonLogs(stdout io.ReadCloser, stderr io.ReadCloser) {
	logger := func(pipe io.ReadCloser) {
		scanner := bufio.NewScanner(pipe)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			m := scanner.Text()
			fmt.Println("[[python-worker]] " + m)
		}
	}
	go logger(stdout)
	go logger(stderr)
}
