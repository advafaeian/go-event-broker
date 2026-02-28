package handler

import (
	"advafaeian/go-event-broker/internal/metadata"
	"advafaeian/go-event-broker/internal/protocol"
	"fmt"
	"os"
)

func WriteBatchToDisk(logDir string, topic string, partition int32, batch protocol.BatchRecords) error {
	dir := fmt.Sprintf("%s/%s-%d", logDir, topic, partition)

	// Create directory tree if needed
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	path := fmt.Sprintf("%s/00000000000000000000.log", dir)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := &protocol.Writer{}
	if err := batch.Encode(w); err != nil {
		return err
	}

	_, err = f.Write(w.RawBytes())
	return err
}

func HandleProduce(w *protocol.Writer, r *protocol.Reader, metadataLoader *metadata.MetadataLoader, errCode int16) error {
	request := protocol.ProduceRequest{}
	if err := request.Decode(r); err != nil {
		return err
	}

	var respTopics []protocol.ProduceResponseTopic

	for _, reqTopic := range request.Topics {

		var respPartitions []protocol.ProduceResponsePartition

		_, topicErr := metadataLoader.Get(reqTopic.TopicName)

		for _, reqPartition := range reqTopic.Partitions {

			var errorCode int16 = 0

			if topicErr != nil {
				errorCode = protocol.UnknownTopicOrPartition
			} else {
				_, err := metadata.LoadPartition(reqTopic.TopicName, reqPartition.PartitionIndex)
				if err != nil {
					errorCode = protocol.UnknownTopicOrPartition
				}
			}

			if errorCode != 0 {
				respPartitions = append(respPartitions, protocol.ProduceResponsePartition{
					ErrorCode:      errorCode,
					PartitionID:    reqPartition.PartitionIndex,
					BaseOffset:     -1,
					LogAppendTime:  -1,
					LogStartOffset: -1,
				})
				errorCode = 0
				continue
			}

			for _, batch := range reqPartition.RecordBatches {
				WriteBatchToDisk("/tmp/kraft-combined-logs/", reqTopic.TopicName, reqPartition.PartitionIndex, batch)
			}
			respPartitions = append(respPartitions, protocol.ProduceResponsePartition{
				ErrorCode:      errorCode,
				PartitionID:    reqPartition.PartitionIndex,
				BaseOffset:     0,
				LogAppendTime:  -1,
				LogStartOffset: 0,
			})
		}
		respTopics = append(respTopics, protocol.ProduceResponseTopic{
			TopicName:  reqTopic.TopicName,
			Partitions: respPartitions,
		})
	}

	response := protocol.ProduceResponse{
		ThrottleMs: 0,
		Topics:     respTopics,
	}

	response.Encode(w)
	return nil
}
