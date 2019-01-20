package parsers

import (
	"encoding/json"

	"github.com/gocelery/gocelery/errors"
	"github.com/gocelery/gocelery/protocol"
)

// JSONParser decodes broker/backend message
type JSONParser struct {
}

// Decode decodes task message
func (p *JSONParser) Decode(msg []byte) (*protocol.TaskMessage, error) {
	if msg == nil {
		return nil, errors.ErrParseEmpty
	}
	tm := &protocol.TaskMessage{}
	err := json.Unmarshal(msg, tm)
	return tm, err
}

// ContentType returns content type string for parser
func (p *JSONParser) ContentType() string {
	return "application/json"
}
