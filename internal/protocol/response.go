package protocol

type Response struct {
	MessageSize   int32
	CorrelationID int32
	ErrorCode     int16
}

func (r *Response) Encode() []byte {
	result := make([]byte, 0, 10)
	result = append(result, Int32ToBytes(r.MessageSize)[:]...)
	result = append(result, Int32ToBytes(r.CorrelationID)[:]...)
	result = append(result, Int16ToBytes(r.ErrorCode)[:]...)
	return result
}
