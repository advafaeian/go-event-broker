package protocol

type ResponseHeader struct {
	CorrelationID int32
	TagBuffer     TagBuffer
}

func (r *ResponseHeader) Encode(w *Writer, version int) {
	w.Int32(r.CorrelationID)
	if version > 0 {
		w.TagBuffer(r.TagBuffer)
	}
}
