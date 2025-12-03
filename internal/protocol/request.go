package protocol

type Request struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
}

func ParseRequest(buf []byte, request *Request) error {
	request.RequestApiKey = BytesToInt16(buf[0:2])
	request.RequestApiVersion = BytesToInt16(buf[2:4])
	request.CorrelationId = BytesToInt32(buf[4:8])
	return nil
}

func (r *Request) Validate() error {
	if r.RequestApiVersion > 5 {
		return ErrUnsupportedVersion
	}
	return nil
}
