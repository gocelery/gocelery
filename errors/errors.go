package errors

import "errors"

var ErrUnsupportedContentType = errors.New("unsupported content type")
var ErrUnsupportedContentEncoding = errors.New("unsupported content encoding")
var ErrUnsupportedBodyEncoding = errors.New("unsupported body encoding")
var ErrInvalidMessageBody = errors.New("invalid message body")
