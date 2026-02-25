package protocol

type Cursor *int8

type DescribeTopicPartitionsRequest struct {
	Topics                 []Topic
	ResponsePartitionLimit int32
	Cursor                 Cursor // pointer to make it nullable
	TagBuffer              TagBuffer
}

func (req *DescribeTopicPartitionsRequest) Decode(red *Reader) error {
	var err error

	if req.Topics, err = red.CompactArrayTopics(); err != nil {
		return err
	}
	if req.ResponsePartitionLimit, err = red.Int32(); err != nil {
		return err
	}

	cursor, err := red.Int8()
	if err != nil {
		return err
	}

	if cursor == -1 {
		req.Cursor = nil
	} else {
		req.Cursor = nil
	}

	if req.TagBuffer, err = red.TagBuffer(); err != nil {
		return err
	}
	return nil
}

type DescribeTopicPartitionsResponse struct {
	Header     ResponseHeader
	ThrottleMs int32
	Topics     []Topic
	NextCursor Cursor
	TagBuffer  TagBuffer
}

func (r *DescribeTopicPartitionsResponse) Encode(w *Writer) {
	r.Header.Encode(w, 1) // version 1

	w.Int32(r.ThrottleMs)
	WriteCompactArray(w, r.Topics)

	w.Cursor(r.NextCursor)

	w.TagBuffer(r.TagBuffer)
}

func (w *Writer) Cursor(c Cursor) {
	if c == nil {
		w.Int8(-1)
	}
}
