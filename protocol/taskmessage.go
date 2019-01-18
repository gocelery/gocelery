package protocol

import "github.com/gocelery/gocelery/errors"

// TaskMessage format
// TaskMessage Protocol v1 has empty headers
// http://docs.celeryproject.org/en/latest/internals/protocol.html
type TaskMessage struct {
	ContentType     string                 `json:"content-type"`      // default application/json
	ContentEncoding string                 `json:"content-encoding"`  // default utf-8
	Headers         map[string]interface{} `json:"headers,omitempty"` // empty for Message Protocol v1
	Properties      Properties             `json:"properties"`
	Body            string                 `json:"body"` // encoded message body
}

// Properties for TaskMessage
type Properties struct {
	CorrelationID string       `json:"correlation_id"`
	ReplyTo       string       `json:"reply_to,omitempty"` // optional
	BodyEncoding  string       `json:"body_encoding"`      // default base64
	DeliveryTag   string       `json:"delivery_tag"`
	DeliveryMode  int          `json:"delivery_mode"` // default 1
	DeliveryInfo  DeliveryInfo `json:"delivery_info"`

	// priority field used to be in delivery_info for older version
	// for compatibility reason, this field is not parsed
	// Priority      int          `json:"priority"`
}

// DeliveryInfo for TaskMessage Protocol v1 Properties
type DeliveryInfo struct {
	RoutingKey string `json:"routing_key"` // ex) task.succeeded
	Exchange   string `json:"exchange"`    // default celeryev
}

// Validate validates TaskMessage
func (tm *TaskMessage) Validate() error {
	if tm.ContentType != "application/json" {
		return errors.ErrUnsupportedContentType
	}
	if tm.ContentEncoding != "utf-8" {
		return errors.ErrUnsupportedContentEncoding
	}
	if tm.Properties.BodyEncoding != "base64" {
		return errors.ErrUnsupportedBodyEncoding
	}
	return nil
}
