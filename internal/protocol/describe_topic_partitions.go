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

	req.TagBuffer = red.TagBuffer()
	return nil
}

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	IsrNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownElr           []int32
	OfflineReplicas        []int32
	TagBuffer              TagBuffer
}

func (w *Writer) CompactArrayInt32(arr []int32) {
	w.buf = append(w.buf, uvarintToBytes(uint32(len(arr)+1))...)
	for i := range arr {
		w.Int32(arr[i])
	}
}

func (p *Partition) encode(w *Writer) {
	w.Int16(p.ErrorCode)
	w.Int32(p.PartitionIndex)
	w.Int32(p.LeaderId)
	w.Int32(p.LeaderEpoch)
	w.CompactArrayInt32(p.ReplicaNodes)
	w.CompactArrayInt32(p.IsrNodes)
	w.CompactArrayInt32(p.EligibleLeaderReplicas)
	w.CompactArrayInt32(p.LastKnownElr)
	w.CompactArrayInt32(p.OfflineReplicas)
	w.TagBuffer(p.TagBuffer)
}

type Topic struct {
	ErrorCode            int16
	TopicName            string
	TopicID              UUID
	IsInternal           bool
	Partitions           []Partition
	AuthorizedOperations int32
	TagBuffer            TagBuffer
}

func (t *Topic) encode(w *Writer) {
	w.Int16(t.ErrorCode)
	w.CompactString(t.TopicName)
	w.append(t.TopicID[:])
	w.Bool(t.IsInternal)
	w.CompactArrayPartitions(t.Partitions)
	w.Int32(t.AuthorizedOperations)
	w.TagBuffer(t.TagBuffer)
}

func (t *Topic) decode(r *Reader) error {
	var err error
	if t.TopicName, err = r.CompactString(); err != nil {
		return err
	}
	t.TagBuffer = r.TagBuffer()

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
	w.CompactArrayTopics(r.Topics)

	w.Cursor(r.NextCursor)

	w.TagBuffer(r.TagBuffer)
}

func (w *Writer) Cursor(c Cursor) {
	if c == nil {
		w.Int8(-1)
	}
}
