package protocol

import (
	"errors"
	"fmt"
	"io"
)

type ProduceRequest struct {
	TransactionalID string // COMPACT_NULLABLE_STRING
	RequiredACKs    int16
	Timeout         int32
	Topics          []ProduceRequestTopic // compact array
	TagBuffer       TagBuffer
}

func (pr *ProduceRequest) Decode(red *Reader) error {
	return pr.decode(red)
}

func (pr *ProduceRequest) decode(red *Reader) error {
	var err error

	if pr.TransactionalID, err = red.CompactString(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequest TransactionalID: %w", err)
	}

	if pr.RequiredACKs, err = red.Int16(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequest RequiredACKs: %w", err)
	}

	if pr.Timeout, err = red.Int32(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequest Timeout: %w", err)
	}

	if pr.Topics, err = ReadCompactArray[ProduceRequestTopic](red); err != nil {
		return fmt.Errorf("Error decoding ProduceRequest Topics: %w", err)
	}

	if pr.TagBuffer, err = red.TagBuffer(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequest TagBuffer: %w", err)
	}

	return err
}

type ProduceRequestTopic struct {
	TopicName  string                    // COMPACT_STRING
	Partitions []ProduceRequestPartition // compact array
	TagBuffer  TagBuffer
}

func (pr *ProduceRequestTopic) decode(red *Reader) error {
	var err error

	if pr.TopicName, err = red.CompactString(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequestTopic TopicName: %w", err)
	}

	if pr.Partitions, err = ReadCompactArray[ProduceRequestPartition](red); err != nil {
		return fmt.Errorf("Error decoding ProduceRequestTopic Partitions: %w", err)
	}

	if pr.TagBuffer, err = red.TagBuffer(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequestTopic TagBuffer: %w", err)
	}

	return err
}

type ProduceRequestPartition struct {
	PartitionIndex int32
	RecordBatches  []BatchRecords // compact array
	TagBuffer      TagBuffer
}

func (pr *ProduceRequestPartition) decode(red *Reader) error {
	var err error

	if pr.PartitionIndex, err = red.Int32(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequestPartition PartitionIndex: %w", err)
	}

	bytesLenPlusOne, err := red.UVarInt()
	if err != nil {
		return fmt.Errorf("Error decoding ProduceRequestPartition recordBatchSize: %w", err)
	}

	batchBytes := make([]byte, bytesLenPlusOne-1)
	red.Read(batchBytes)

	buf := NewReaderFromBytes(batchBytes)
	for {
		var batch BatchRecords
		if err := batch.Decode(buf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		pr.RecordBatches = append(pr.RecordBatches, batch)
	}

	if pr.TagBuffer, err = red.TagBuffer(); err != nil {
		return fmt.Errorf("Error decoding ProduceRequestTopic TagBuffer: %w", err)
	}

	return err
}

type ProduceResponse struct {
	// Message Size int32
	Topics     []ProduceResponseTopic // compact array
	ThrottleMs int32
	TagBuffer  TagBuffer
}

func (pr *ProduceResponse) Encode(w *Writer) error {

	return pr.encode(w)
}

func (pr *ProduceResponse) encode(w *Writer) error {

	WriteCompactArray(w, pr.Topics)
	w.Int32(pr.ThrottleMs)
	w.TagBuffer(pr.TagBuffer)

	return nil
}

type ProduceResponseTopic struct {
	TopicName  string                     // COMPACT_STRING
	Partitions []ProduceResponsePartition // compact array
	TagBuffer  TagBuffer
}

func (pr *ProduceResponseTopic) encode(w *Writer) error {
	w.CompactString(pr.TopicName)
	WriteCompactArray(w, pr.Partitions)
	w.TagBuffer(pr.TagBuffer)
	return nil
}

type ProduceResponsePartition struct {
	PartitionID       int32
	ErrorCode         int16
	BaseOffset        int64
	LogAppendTime     int64
	LogStartOffset    int64
	RecordErrorsArray []RecordError // compact array
	ErrorMessage      string        // COMPACT_NULLABLE_STRING
	TagBuffer         TagBuffer
}

func (pr *ProduceResponsePartition) encode(w *Writer) error {

	w.Int32(pr.PartitionID)
	w.Int16(pr.ErrorCode)
	w.Int64(pr.BaseOffset)
	w.Int64(pr.LogAppendTime)
	w.Int64(pr.LogStartOffset)
	WriteCompactArray(w, pr.RecordErrorsArray)
	w.CompactString(pr.ErrorMessage)
	w.TagBuffer(pr.TagBuffer)

	return nil
}

type RecordError struct {
}

func (re *RecordError) encode(w *Writer) error {

	return nil
}
