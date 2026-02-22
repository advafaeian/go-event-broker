package protocol

type ApiKey struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  TagBuffer
}

type ApiVersionsResponse struct {
	Header     ResponseHeader
	ErrorCode  int16
	ApiKeys    []ApiKey
	ThrottleMs int32
	TagBuffer  TagBuffer
}

func (r *ApiVersionsResponse) Encode(w *Writer) {
	r.Header.Encode(w, 0)
	w.Int16(r.ErrorCode)
	arrayLength := uint32(len(r.ApiKeys) + 1)
	w.UvarI(arrayLength)

	w.ApiKeys(r.ApiKeys)

	w.Int32(r.ThrottleMs)
	w.Int8(0)
}
