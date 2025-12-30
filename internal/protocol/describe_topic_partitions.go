package protocol

type Cursor *int8

type DescribeTopicPartitionsRequest struct {
	Topics                 []Topic
	ResponsePartitionLimit int32
	Cursor                 Cursor // pointer to make it nullable
	TagBuffer              TagBuffer
}

func (req *DescribeTopicPartitionsRequest) Decode(red *Reader) {

	req.Topics = red.Topics()

	req.ResponsePartitionLimit = red.Int32()

	cursor := red.Int8()

	if cursor == -1 {
		req.Cursor = nil
	} else {
		req.Cursor = nil
	}

	req.TagBuffer = red.TagBuffer()
}

type TopicID [16]byte

type Partition struct{}

type Topic struct {
	ErrorCode            int16
	TopicName            string
	TopicID              TopicID
	Partitions           []Partition
	IsInternal           bool
	AuthorizedOperations int32
	TagBuffer            TagBuffer
}

func (t *Topic) encode(w *Writer) {
	w.Int16(t.ErrorCode)
	w.CompactString(t.TopicName)
	w.append(t.TopicID[:])
	w.PartitionsArray(t.Partitions)
	w.Bool(t.IsInternal)
	w.Int32(t.AuthorizedOperations)
	w.TagBuffer(t.TagBuffer)
}
func (t *Topic) decode(r *Reader) {
	t.TopicName = r.CompactString()
	t.TagBuffer = r.TagBuffer()
}

func (r *Reader) Topics() []Topic {
	lengthPlusOne := r.VarInt()
	topics := make([]Topic, lengthPlusOne-1)
	for i := range topics {
		topics[i].decode(r)
	}
	return topics
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
	w.Topics(r.Topics)

	w.Cursor(r.NextCursor)

	w.TagBuffer(r.TagBuffer)
}

func (w *Writer) Topics(topics []Topic) {
	sizePlusOne := uint32(len(topics)) + 1

	w.UvarI(sizePlusOne)

	for _, t := range topics {
		t.encode(w)
	}
}

func (w *Writer) Cursor(c Cursor) {
	if c == nil {
		w.Int8(-1)
	}
}
