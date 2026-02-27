package protocol

import (
	"fmt"
)

type FetchResponse struct {
	Header     ResponseHeader
	ThrottleMs int32
	ErrorCode  int16
	SessionID  int32
	Responses  []FetchResponseTopic
	TagBuffer  TagBuffer
}

type FetchResponseTopic struct {
	TopicID    UUID
	Partitions []FetchResponsePartition
	TagBuffer  TagBuffer
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (at *AbortedTransaction) encode(w *Writer) error {
	w.Int64(at.ProducerID)
	w.Int64(at.FirstOffset)
	return nil
}

type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWaterMark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	RecordBatches        []BatchRecords
	TagBuffer            TagBuffer
}

func (p *FetchResponsePartition) encode(w *Writer) error {
	w.Int32(p.PartitionIndex)
	w.Int16(p.ErrorCode)
	w.Int64(p.HighWaterMark)
	w.Int64(p.LastStableOffset)
	w.Int64(p.LogStartOffset)
	WriteCompactArray(w, p.AbortedTransactions)
	w.Int32(p.PreferredReadReplica)

	// records batchs writer
	body := &Writer{}
	for _, BatchRecords := range p.RecordBatches {
		BatchRecords.encode(body)
	}
	w.UvarI(uint32(body.Len()) + 1) //byte size + 1
	w.Write(body.RawBytes())

	w.TagBuffer(p.TagBuffer)
	return nil
}

func (t *FetchResponseTopic) encode(w *Writer) error {
	w.append(t.TopicID[:])
	WriteCompactArray(w, t.Partitions)
	w.TagBuffer(t.TagBuffer)
	return nil
}

func (r *FetchResponse) Encode(w *Writer) {
	r.Header.Encode(w, 4)
	w.Int32(r.ThrottleMs)
	w.Int16(r.ErrorCode)
	w.Int32(r.SessionID)
	WriteCompactArray(w, r.Responses)
	w.TagBuffer(r.TagBuffer)
}

type FetchRequestTopic struct {
	TopicID    UUID
	Partitions []FetchRequestPartition
	TagBuffer  TagBuffer
}

type FetchRequestPartition struct {
	ID                 int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedOffset  int64
	LogStartOffset     int32
	PartitionMaxBytes  int32
	TagBuffer          TagBuffer
}

type FetchRequest struct {
	MaxWaitMS       int32
	MinBytes        int32
	MaxBytes        int32
	IsolationLevel  int8
	SessionID       int32
	SessionEpoch    int32
	Topics          []FetchRequestTopic
	ForgottenTopics []FetchRequestTopic
	RackID          RackID
}

type RackID struct {
	Value     string
	TagBuffer TagBuffer
}

func (r *RackID) Decode(red *Reader) error {
	var err error

	r.Value, err = red.CompactString()
	if err != nil {
		return fmt.Errorf("Error parsing RackID: %w", err)
	}
	r.TagBuffer, err = red.TagBuffer()
	if err != nil {
		return fmt.Errorf("Error parsing RackID %w", err)
	}
	return nil
}

func (r *FetchRequest) Decode(red *Reader) error {
	var err error

	r.MaxWaitMS, err = red.Int32()
	if err != nil {
		return fmt.Errorf("Error parsing fetch request MaxWaitMS: %w", err)
	}
	r.MinBytes, err = red.Int32()
	if err != nil {
		return fmt.Errorf("Error parsing fetch request MinBytes: %w", err)
	}
	r.MaxBytes, err = red.Int32()
	if err != nil {
		return fmt.Errorf("Error parsing fetch request MaxBytes: %w", err)
	}
	r.IsolationLevel, err = red.Int8()
	if err != nil {
		return fmt.Errorf("Error parsing fetch request IsolationLevel: %w", err)
	}
	r.SessionID, err = red.Int32()
	if err != nil {
		return fmt.Errorf("Error parsing fetch request SessionID: %w", err)
	}
	r.SessionEpoch, err = red.Int32()
	if err != nil {
		return fmt.Errorf("Error parsing fetch request SessionEpoch: %w", err)
	}
	r.Topics, err = ReadCompactArray[FetchRequestTopic](red)
	if err != nil {
		return fmt.Errorf("Error parsing fetch request Topics: %w", err)
	}
	r.ForgottenTopics, err = ReadCompactArray[FetchRequestTopic](red)
	if err != nil {
		return fmt.Errorf("Error parsing fetch request ForgottenTopics: %w", err)
	}

	if err = r.RackID.Decode(red); err != nil {
		return fmt.Errorf("Error parsing fetch request: %w", err)
	}
	return nil
}
