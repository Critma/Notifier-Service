package notifier

import "errors"

var (
	ErrClosed           = errors.New("notifier is closed")
	ErrUnexpectedStatus = errors.New("unexpected status code from external service")
)
