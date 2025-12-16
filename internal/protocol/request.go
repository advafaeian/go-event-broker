package protocol

type ClientID struct {
	Length   int16
	Contents string
}

type RequestHeader struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientID          ClientID
	TagBuffer         TagBuffer
}

func (req *RequestHeader) Decode(red *Reader) error {
	req.RequestApiKey = red.Int16()
	req.RequestApiVersion = red.Int16()
	req.CorrelationId = red.Int32()
	return nil
}

func (r *RequestHeader) Validate() error {
	if r.RequestApiVersion > 5 {
		return ErrUnsupportedVersion
	}
	return nil
}
