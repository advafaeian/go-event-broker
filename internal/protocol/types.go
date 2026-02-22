package protocol

const (
	FetchKey                   int16 = 1
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75
)

var SupportedApiKeys = []ApiKey{
	{ApiKey: ApiVersionsKey, MinVersion: 0, MaxVersion: 4},
	{ApiKey: DescribeTopicPartitionsKey, MinVersion: 0, MaxVersion: 0},
	{ApiKey: FetchKey, MinVersion: 0, MaxVersion: 16},
}

type TaggedField struct{}

type TagBuffer []TaggedField

type UUID [16]byte
