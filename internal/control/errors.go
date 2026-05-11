package control

import "errors"

// Sentinel errors returned by package control. Callers can match with errors.Is.
var (
	ErrFrameTooLarge = errors.New("control: frame exceeds MaxFrameSize")
	ErrUnknownType   = errors.New("control: unknown message type")
	ErrUnexpectedMsg = errors.New("control: unexpected message type")
)
