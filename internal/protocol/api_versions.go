package protocol

type TaggedField struct{}

type ApiKey struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  []TaggedField
}

type ApiVersionsResponse struct {
	CorrelationID int32
	ErrorCode     int16
	ApiKeys       []ApiKey
	ThrottleMs    int32
	TagBuffer     []TaggedField
}

func (r *ApiVersionsResponse) Encode() []byte {

	result := make([]byte, 0, 128)

	result = append(result, Int32ToBytes(r.CorrelationID)...)
	result = append(result, Int16ToBytes(r.ErrorCode)...)

	if r.ErrorCode != 0 {
		return putSize(result)
	}

	arrayLength := uint32(len(r.ApiKeys) + 1)
	result = append(result, uvarintToBytes(arrayLength)...)
	for _, k := range r.ApiKeys {
		result = append(result, Int16ToBytes(k.ApiKey)...)
		result = append(result, Int16ToBytes(k.MinVersion)...)
		result = append(result, Int16ToBytes(k.MaxVersion)...)

		if len(k.TagBuffer) == 0 {
			result = append(result, byte(0)) // empty tagfield
		}
	}

	result = append(result, Int32ToBytes(r.ThrottleMs)...)
	result = append(result, byte(0))

	out := putSize(result)

	return out
}

func putSize(buf []byte) []byte {
	messageSize := Int32ToBytes(int32(len(buf)))

	out := make([]byte, 4+len(buf))

	copy(out[:4], messageSize)
	copy(out[4:], buf)

	return out
}
