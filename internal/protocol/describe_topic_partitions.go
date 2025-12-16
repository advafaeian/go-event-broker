package protocol

type Cursor struct{}

type DescribeTopicPartitionsRequest struct {
	ResponsePartitionLimit int32
	Cursor                 *Cursor // pointer to make it nullable
	TagBuffer              TagBuffer
}

func (req *DescribeTopicPartitionsRequest) Decode(red *Reader) {

	req.ResponsePartitionLimit = red.Int32()

	cursor := red.Int8()

	if cursor == -1 {
		req.Cursor = nil
	} else {
		req.Cursor = &Cursor{}
	}

	req.TagBuffer = red.TagBuffer()
}

// type DescribeTopicPartitionsResponse struct {
// 	Header *Request
// }
