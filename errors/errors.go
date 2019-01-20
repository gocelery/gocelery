package errors

import (
	"errors"
)

var ErrUnsupportedContentType = errors.New("unsupported content type")
var ErrUnsupportedContentEncoding = errors.New("unsupported content encoding")
var ErrUnsupportedBodyEncoding = errors.New("unsupported body encoding")
var ErrInvalidMessageBody = errors.New("invalid message body")
var ErrUninitializedFields = errors.New("uninitialized fields")

var ErrParseEmpty = errors.New("unable to parse empty message")
var ErrParseInvalid = errors.New("invalid message received")
