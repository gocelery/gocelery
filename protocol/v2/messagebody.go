package v2

import (
	"encoding/base64"
	"encoding/json"

	"github.com/gocelery/gocelery/errors"
)

// MessageBody for TaskMessage Protocol v2
type MessageBody struct {
	Args   []interface{}          `json:"args"`
	Kwargs map[string]interface{} `json:"kwargs"`
	Embed  Embed
}

// Embed has method signatures used for callbacks
// TODO: check format
type Embed struct {
	Callbacks []string `json:"callbacks"`
	Errbacks  []string `json:"errbacks"`
	Chain     []string `json:"chain"`
	Chord     string   `json:"chord"`
}

// Decode decodes message body
func Decode(encodedBody string) (*MessageBody, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	msgList := make([]interface{}, 3)
	if err := json.Unmarshal(body, msgList); err != nil {
		return nil, err
	}
	args, ok := msgList[0].([]interface{})
	if !ok {
		return nil, errors.ErrInvalidMessageBody
	}
	kwargs, ok := msgList[1].(map[string]interface{})
	if !ok {
		return nil, errors.ErrInvalidMessageBody
	}
	embed, ok := msgList[2].(Embed)
	if !ok {
		return nil, errors.ErrInvalidMessageBody
	}
	return &MessageBody{
		Args:   args,
		Kwargs: kwargs,
		Embed:  embed,
	}, nil
}
