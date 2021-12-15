package config

type (
	WorkerConfig struct {
		UsePyWorker  bool
		UseGoWorker  bool
		GoBrokerURL  string
		GoBackendURL string
		PyBrokerURL  string
		PyBackendURL string
	}
)
