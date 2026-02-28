package handler

import (
	"advafaeian/go-event-broker/internal/metadata"
	"advafaeian/go-event-broker/internal/protocol"
)

func HandleProduce(w *protocol.Writer, r *protocol.Reader, metadataLoader *metadata.MetadataLoader, errCode int16) error {
	request := protocol.ProduceRequest{}
	if err := request.Decode(r); err != nil {
		return err
	}

	reqTopicName := request.Topics[0].TopicName

	var errorCode int16

	topic, err := metadataLoader.Get(reqTopicName)
	// validate topic
	var response protocol.ProduceResponse

	if err != nil {
		errorCode = protocol.UnknownTopicOrPartition
	}

	// validating paritions
	for _, p := range topic.Partitions {
		reqPartitionIndex := request.Topics[0].Partitions[0].PartitionIndex
		reqTopicID := topic.TopicID

		_, err := metadata.LoadPartition(topic.TopicName, reqPartitionIndex)
		if err != nil || reqPartitionIndex != p.PartitionIndex || reqTopicID != p.TopicID {
			errorCode = protocol.UnknownTopicOrPartition
			continue
		}
	}

	if errCode != 0 {
		response = protocol.ProduceResponse{
			Topics: []protocol.ProduceResponseTopic{
				{
					TopicName: reqTopicName,
					Partitions: []protocol.ProduceResponsePartition{
						{
							PartitionID:    request.Topics[0].Partitions[0].PartitionIndex,
							ErrorCode:      errorCode,
							BaseOffset:     -1,
							LogAppendTime:  -1,
							LogStartOffset: -1,
						},
					},
				},
			},
		}
	} else {
		response = protocol.ProduceResponse{
			ThrottleMs: 0,
			Topics: []protocol.ProduceResponseTopic{
				{
					TopicName: topic.TopicName,
					Partitions: []protocol.ProduceResponsePartition{
						{
							ErrorCode:      errorCode,
							PartitionID:    request.Topics[0].Partitions[0].PartitionIndex,
							BaseOffset:     0,
							LogAppendTime:  -1,
							LogStartOffset: 0,
						},
					},
				},
			},
		}
	}

	response.Encode(w)
	return nil
}
