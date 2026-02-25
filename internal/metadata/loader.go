package metadata

import (
	"advafaeian/go-event-broker/internal/protocol"
	"fmt"
	"log"
	"os"
)

type MetadataLoader struct {
	Path       string
	Topics     map[string]*protocol.Topic
	TopicsByID map[protocol.UUID]*protocol.Topic
}

func NewMetadataLoader(path string) *MetadataLoader {
	return &MetadataLoader{
		Path:       path,
		Topics:     map[string]*protocol.Topic{},
		TopicsByID: map[protocol.UUID]*protocol.Topic{},
	}
}

func (l *MetadataLoader) Get(name string) (*protocol.Topic, error) {
	topic, ok := l.Topics[name]
	if !ok {
		return nil, fmt.Errorf("Error getting topic %s from metadata", name)
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
		_, err := r.Int64() //  Base Offset
		if err != nil {
			break
		}

		r.Int32()  // batch Length
		r.Skip(4 + // Partition Leader Epoch
			1 + // Magic Byte
			4 + // CRC
			2 + // Attributes
			4 + // Last Offset Delta
			8 + // Base Timestamp
			8 + // Max Timestamp
			8 + // Producer ID
			2 + // Producer Epoch
			4, // Base Sequence
		)

		recordsLength, _ := r.Int32()
		for range recordsLength {
			r.SVarInt()                   // length
			r.Skip(1)                     //Attributes
			r.SVarInt()                   // Timestamp Delta
			r.SVarInt()                   // Offset Delta
			keyLength, err := r.SVarInt() // Key Length
			if err != nil {
				return fmt.Errorf("Error reading metadata key length %w", err)
			}
			for range keyLength {
				r.Skip(1) // Key
			}
			r.SVarInt() // Value Length

			r.Skip(1)                 // Frame Version
			recordType, _ := r.Int8() // Type
			r.Skip(1)                 // Version
			switch recordType {
			case 9: // RemoveTopicRecord
				r.UUID() // TopicId
			case 1: //UnregisterBrokerRecord
				r.Int32() // BrokerId
				r.Int64() // BrokerEpoch
			case 23: // BeginTransactionRecord
				r.CompactString() // Name length and Name
			case 12: // Feature Level Record
				r.CompactString() // Name length and Name
				r.Skip(2)         // Feature Level
			case 2:
				lastTopic = &protocol.Topic{}
				topicName, err := r.CompactString()
				if err != nil {
					return fmt.Errorf("failed to read Topic Record: %w", err)
				}
				uuid, err := r.UUID()
				if err != nil {
					return fmt.Errorf("failed to read Topic Record: %w", err)
				}
				lastTopic.TopicName = topicName
				lastTopic.TopicID = uuid
			case 3:

				partitionID, err := r.Int32()
				if err != nil {
					return fmt.Errorf("failed to read Partition Record: %w", err)
				}
				_, err = r.UUID()
				if err != nil {
					return fmt.Errorf("failed to read Partition Record: %w", err)
				}

				replicaArray, err := r.CompactArrayInt32() // Replica Array
				if err != nil {
					return fmt.Errorf("Error reading metadata replica array %w", err)
				}

				isrArray, err := r.CompactArrayInt32() // In Sync Replica Array
				if err != nil {
					return fmt.Errorf("Error reading in sync replica array %w", err)
				}

				r.UVarInt()                // Length of Removing Replicas array
				r.UVarInt()                // Length of Adding Replicas array
				leaderID, err := r.Int32() // Leader
				if err != nil {
					return fmt.Errorf("Error reading metadata partition leader id %w", err)
				}

				leaderEpoch, err := r.Int32() // Leader Epoch
				if err != nil {
					return fmt.Errorf("Error reading metadata partition leader epoch %w", err)
				}

				r.Int32()            // Partition Epoch
				r.CompactArrayUUID() // Directories Array

				part := protocol.Partition{
					PartitionIndex: partitionID,
					LeaderId:       leaderID,
					LeaderEpoch:    leaderEpoch,
					ReplicaNodes:   replicaArray,
					IsrNodes:       isrArray,
				}

				lastTopic.Partitions = append(lastTopic.Partitions, part)
			}
			r.UVarInt() // Tagged Fields Count
			r.UVarInt() // Headers Array Count
		}
		if lastTopic.TopicName != "" {
			l.Topics[lastTopic.TopicName] = lastTopic
			l.TopicsByID[lastTopic.TopicID] = lastTopic
		}
	}
	return nil
}
