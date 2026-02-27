package metadata

import (
	"advafaeian/go-event-broker/internal/protocol"
	"fmt"
	"os"
)

func LoadPartition(topicName string, partitionIndex int32) ([]protocol.BatchRecords, error) {
	path := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionIndex)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := protocol.NewReader(f)
	var batches []protocol.BatchRecords
	for {
		var batch protocol.BatchRecords
		if err := batch.Decode(r); err != nil {
			break
		}
		batches = append(batches, batch)
	}
	return batches, nil
}
