package protocol

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
