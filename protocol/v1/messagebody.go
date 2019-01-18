package v1

import (
	"encoding/base64"
	"encoding/json"
)

// MessageBody for TaskMessage Protocol v1
type MessageBody struct {
	Task    string                 `json:"task"`
	ID      string                 `json:"id"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     string                 `json:"eta"` // iso8601

	// only supported after 2.0.2
	Expires string `json:"expires,omitempty"` // iso8601

	TaskSet string `json:"taskset,omitempty"`

	// only supported after 2.3
	Chord string `json:"chord,omitempty"` // function signature

	// only supported after 2.5
	UTC bool `json:"utc,omitempty"` // if it is utc timezone

	// only supported after 3.0
	Callbacks []string `json:"callbacks,omitempty"`

	// only supported after 3.0
	Errbacks []string `json:"errbacks,omitempty"`

	// only supported after 3.1
	// TODO: float might be ignored for json
	TimeLimit []float32 `json:"timelimit,omitempty"`
}

// Decode decodes message body
func Decode(encodedBody string) (*MessageBody, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	msg := &MessageBody{}
	if err := json.Unmarshal(body, msg); err != nil {
		return nil, err
	}
	return msg, nil
}
