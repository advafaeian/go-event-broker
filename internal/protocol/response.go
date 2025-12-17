package protocol

type ResponseHeader struct {
	CorrelationID int32
}

func (r *ResponseHeader) Encode(w *Writer) {
	w.Int32(r.CorrelationID)
}
