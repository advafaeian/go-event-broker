package metadata

import (
	"advafaeian/go-event-broker/internal/protocol"
	"fmt"
	"log"
	"os"
)

type MetadataLoader struct {
	Path         string
	Topics       map[string]*protocol.Topic
	TopicsByID   map[protocol.UUID]*protocol.Topic
	BatchRecords []protocol.BatchRecords
}

func NewMetadataLoader(path string) *MetadataLoader {
	return &MetadataLoader{
		Path:         path,
		Topics:       map[string]*protocol.Topic{},
		TopicsByID:   map[protocol.UUID]*protocol.Topic{},
		BatchRecords: []protocol.BatchRecords{},
	}
}

func (l *MetadataLoader) Get(name string) (*protocol.Topic, error) {
	topic, ok := l.Topics[name]
	if !ok {
		return nil, fmt.Errorf("Error getting topic %s from metadata", name)
	}
	return topic, nil
}

func (l *MetadataLoader) GetByUUID(uuid protocol.UUID) (*protocol.Topic, error) {
	topic, ok := l.TopicsByID[uuid]
	if !ok {
		return nil, fmt.Errorf("Error getting topic %s from metadata", uuid)
	}
	return topic, nil
}

func (l *MetadataLoader) Load() error {

	// files, err := filepath.Glob(l.Path)
	// if err != nil {
	// 	return err
	// }

	f, err := os.Open(l.Path)
	if err != nil {
		log.Println("metadata file not found, starting with empty metadata")
	}
	defer f.Close()

	r := protocol.NewReader(f)

	lastTopic := &protocol.Topic{}

	for { // iterating batches
		var batchRecord protocol.BatchRecords

		if err := batchRecord.Decode(r); err != nil {
			break
		}

		for _, record := range batchRecord.Records {
			valueReader := protocol.NewReaderFromBytes(record.Value)

			valueReader.Skip(1)                 // Frame Version
			recordType, _ := valueReader.Int8() // Type
			valueReader.Skip(1)                 // Version

			switch recordType {
			case 12: // Feature Level Record
				continue
			case 2:
				lastTopic = &protocol.Topic{}
				topicName, err := valueReader.CompactString()
				if err != nil {
					return fmt.Errorf("failed to read Topic Record: %w", err)
				}
				uuid, err := valueReader.UUID()
				if err != nil {
					return fmt.Errorf("failed to read Topic Record: %w", err)
				}
				lastTopic.TopicName = topicName
				lastTopic.TopicID = uuid
			case 3:

				partitionID, err := valueReader.Int32()
				if err != nil {
					return fmt.Errorf("failed to read Partition Record: %w", err)
				}
				_, err = valueReader.UUID()
				if err != nil {
					return fmt.Errorf("failed to read Partition Record: %w", err)
				}

				replicaArray, err := valueReader.CompactArrayInt32() // Replica Array
				if err != nil {
					return fmt.Errorf("Error reading metadata replica array %w", err)
				}

				isrArray, err := valueReader.CompactArrayInt32() // In Sync Replica Array
				if err != nil {
					return fmt.Errorf("Error reading in sync replica array %w", err)
				}

				valueReader.UVarInt()                // Length of Removing Replicas array
				valueReader.UVarInt()                // Length of Adding Replicas array
				leaderID, err := valueReader.Int32() // Leader
				if err != nil {
					return fmt.Errorf("Error reading metadata partition leader id %w", err)
				}

				leaderEpoch, err := valueReader.Int32() // Leader Epoch
				if err != nil {
					return fmt.Errorf("Error reading metadata partition leader epoch %w", err)
				}

				valueReader.Int32()            // Partition Epoch
				valueReader.CompactArrayUUID() // Directories Array

				part := protocol.Partition{
					PartitionIndex: partitionID,
					LeaderId:       leaderID,
					LeaderEpoch:    leaderEpoch,
					ReplicaNodes:   replicaArray,
					IsrNodes:       isrArray,
				}

				lastTopic.Partitions = append(lastTopic.Partitions, part)
			}
			if lastTopic.TopicName != "" {
				l.Topics[lastTopic.TopicName] = lastTopic
				l.TopicsByID[lastTopic.TopicID] = lastTopic
			}
		}

		l.BatchRecords = append(l.BatchRecords, batchRecord)
	}
	return nil
}
