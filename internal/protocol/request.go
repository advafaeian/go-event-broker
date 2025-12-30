package protocol

type ClientID string

type RequestHeader struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientID          string
	TagBuffer         TagBuffer
}

func (req *RequestHeader) Decode(red *Reader) error {
	req.RequestApiKey = red.Int16()
	req.RequestApiVersion = red.Int16()
	req.CorrelationId = red.Int32()
	ClientIDLength := red.Int16()
	for range ClientIDLength {
		req.ClientID += string(red.Byte())
	}
	req.TagBuffer = red.TagBuffer()
	return nil
}

func (r *RequestHeader) Validate() error {
	if r.RequestApiVersion > 5 {
		return ErrUnsupportedVersion
	}
	return nil
}
