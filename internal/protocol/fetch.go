package protocol

type FetchResponse struct {
	Header     ResponseHeader
	ThrottleMs int32
	ErrorCode  int16
	SessionID  int32
	Responses  []Topic
	TagBuffer  TagBuffer
}

func (r *FetchResponse) Encode(w *Writer) {
	r.Header.Encode(w, 4)
	w.Int32(r.ThrottleMs)
	w.Int16(r.ErrorCode)
	w.Int32(r.SessionID)
	w.CompactArrayTopics(r.Responses)
	w.TagBuffer(r.TagBuffer)
}
