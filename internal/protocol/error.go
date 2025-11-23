package protocol

type ProtocolError struct {
	Code    int16
	Message string
}

func (pe *ProtocolError) Error() string {
	return pe.Message
}

var ErrUnsupportedVersion = &ProtocolError{
	Code:    35,
	Message: "only api versions 0-4 are accepted",
}
