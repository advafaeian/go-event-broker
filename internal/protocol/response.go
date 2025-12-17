package protocol

type ResponseHeader struct {
	CorrelationID int32
	TagBuffer     TagBuffer
}

func (r *ResponseHeader) Encode(w *Writer) {
	w.Int32(r.CorrelationID)
	w.TagBuffer(r.TagBuffer)
}
