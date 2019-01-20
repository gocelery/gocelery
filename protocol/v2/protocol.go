package v2

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	Chord     *string  `json:"chord"`
}

// func ParseEmbed(data interface{}) (*Embed, error) {
// 	m, ok := data.(map[string]interface{})
// 	if !ok {
// 		return nil, fmt.Errorf("failed to parse embed")
// 	}

// 	return &Embed{
// 		Callbacks: callbacks,
// 	}, nil
// }

// Decode decodes message body
func Decode(encodedBody string) (*MessageBody, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	msgList := make([]interface{}, 3)
	if err := json.Unmarshal(body, &msgList); err != nil {
		return nil, err
	}
	args, ok := msgList[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid args in message body")
	}
	kwargs, ok := msgList[1].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid kwargs in message body")
	}

	// TODO: parse embed structure
	// msgList[2] is map[string]interface{}
	// may contain nil values
	// use reflect and parse to struct: https://github.com/mitchellh/mapstructure
	// use json to parse to struct
	embed, ok := msgList[2].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid embed in message body: %v", msgList[2])
	}
	_ = embed

	return &MessageBody{
		Args:   args,
		Kwargs: kwargs,
		Embed:  Embed{},
	}, nil
}
