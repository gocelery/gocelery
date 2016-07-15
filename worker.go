package gocelery

// Worker represents distributed task worker
type Worker struct {
	broker CeleryBroker
}

// Run - start workers
func (w *Worker) Run() error {
	return nil
}
