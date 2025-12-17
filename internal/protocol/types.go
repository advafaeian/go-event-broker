package protocol

const (
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75
)

type TaggedField struct{}

type TagBuffer []TaggedField
