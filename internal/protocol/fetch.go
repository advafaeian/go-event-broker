package protocol

type FetchResponse struct {
	Header    ResponseHeader
	ErrorCode int16
}

func (r *FetchResponse) Encode(w *Writer) {
	r.Header.Encode(w, 4)
	w.Int16(r.ErrorCode)
}
