package protocol

type Request struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
}

func ParseRequest(buf []byte, request *Request) error {
	request.MessageSize = BytesToInt32(buf[0:4])
	request.RequestApiKey = BytesToInt16(buf[4:6])
	request.RequestApiVersion = BytesToInt16(buf[6:8])
	request.CorrelationId = BytesToInt32(buf[8:12])
	return nil
}
