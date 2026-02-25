package handler

import (
	"advafaeian/go-event-broker/internal/metadata"
	"advafaeian/go-event-broker/internal/protocol"
	"fmt"
)

func convertPartitions(p []protocol.Partition) []protocol.FetchResponsePartition {
	fp := make([]protocol.FetchResponsePartition, len(p))

	for i := range p {
		fp[i].ErrorCode = p[i].ErrorCode
		fp[i].PartitionIndex = p[i].PartitionIndex
	}
	return fp
}

func HandleFetch(w *protocol.Writer, r *protocol.Reader, metadata *metadata.MetadataLoader, rh protocol.ResponseHeader, errCode int16) error {

	response := protocol.FetchResponse{
		Header:    rh,
		ErrorCode: errCode,
	}

	req := protocol.FetchRequest{}

	err := req.Decode(r)

	if err != nil {
		return fmt.Errorf("Error decoding fetch request body: %w", err)
	}

	for _, t := range req.Topics {
		topic, ok := metadata.TopicsByID[t.TopicID]

		var resTopic protocol.FetchResponseTopic

		if !ok {
			resTopic = protocol.FetchResponseTopic{
				TopicID: t.TopicID,
				Partitions: []protocol.FetchResponsePartition{
					{
						PartitionIndex: 0,
						ErrorCode:      protocol.UnknownTopicID,
					},
				},
			}
		} else {
			resTopic = protocol.FetchResponseTopic{
				TopicID:    t.TopicID,
				Partitions: convertPartitions(topic.Partitions),
			}
		}

		response.Responses = append(response.Responses, resTopic)
	}
	response.Encode(w)
	return nil
}
