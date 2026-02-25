package protocol

import "fmt"

const (
	FetchKey                   int16 = 1
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75
)

type TaggedField struct{}

type TagBuffer []TaggedField

type UUID [16]byte

type Topic struct {
	ErrorCode            int16
	TopicName            string
	TopicID              UUID
	IsInternal           bool
	Partitions           []Partition
	AuthorizedOperations int32
	TagBuffer            TagBuffer
}

func (t *Topic) decode(r *Reader) error {
	var err error
	if r.Version >= 13 {
		if t.TopicID, err = r.UUID(); err != nil {
			return err
		}
	} else {
		if t.TopicName, err = r.CompactString(); err != nil {
			return err
		}
	}
	if t.TagBuffer, err = r.TagBuffer(); err != nil {
		return fmt.Errorf("Error decoding topic: %w", err)
	}

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
