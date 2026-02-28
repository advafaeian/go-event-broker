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

	response := protocol.ProduceResponse{
		Topics: []protocol.ProduceResponseTopic{
			{
				TopicName: request.Topics[0].TopicName,
				Partitions: []protocol.ProduceResponsePartition{
					{
						PartitionID:    request.Topics[0].Partitions[0].PartitionIndex,
						ErrorCode:      protocol.UnknownTopicOrPartition,
						BaseOffset:     -1,
						LogAppendTime:  -1,
						LogStartOffset: -1,
					},
				},
			},
		},
	}

	response.Encode(w)
	return nil
}
