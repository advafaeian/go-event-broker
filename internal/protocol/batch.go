package protocol

import (
	"encoding/hex"
	"fmt"
	"hash/crc32"
)

type BatchRecords struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	Crc                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []BatchRecord
}

func (br *BatchRecords) Decode(red *Reader) error {
	var err error

	if br.BaseOffset, err = red.Int64(); err != nil {
		return fmt.Errorf("Error decoding base offset metadata batch record: %w", err)
	}
	if br.BatchLength, err = red.Int32(); err != nil {
		return fmt.Errorf("Error decoding batch length metadata batch record: %w", err)
	}
	if br.PartitionLeaderEpoch, err = red.Int32(); err != nil {
		return fmt.Errorf("Error decoding metadata batch record: %w", err)
	}
	if br.Magic, err = red.Int8(); err != nil {
		return fmt.Errorf("Error decoding PartitionLeaderEpoch metadata batch record: %w", err)
	}
	if br.Crc, err = red.UInt32(); err != nil {
		return fmt.Errorf("Error decoding Crc metadata batch record: %w", err)
	}
	if br.Attributes, err = red.Int16(); err != nil {
		return fmt.Errorf("Error decoding Attributes metadata batch record: %w", err)
	}
	if br.LastOffsetDelta, err = red.Int32(); err != nil {
		return fmt.Errorf("Error decoding LastOffsetDelta metadata batch record: %w", err)
	}
	if br.BaseTimestamp, err = red.Int64(); err != nil {
		return fmt.Errorf("Error decoding BaseTimestamp metadata batch record: %w", err)
	}
	if br.MaxTimestamp, err = red.Int64(); err != nil {
		return fmt.Errorf("Error decoding MaxTimestamp metadata batch record: %w", err)
	}
	if br.ProducerId, err = red.Int64(); err != nil {
		return fmt.Errorf("Error decoding ProducerId metadata batch record: %w", err)
	}
	if br.ProducerEpoch, err = red.Int16(); err != nil {
		return fmt.Errorf("Error decoding ProducerEpoch metadata batch record: %w", err)
	}
	if br.BaseSequence, err = red.Int32(); err != nil {
		return fmt.Errorf("Error decoding BaseSequence metadata batch record: %w", err)
	}

	RecordsCount, err := red.Int32()
	if err != nil {
		return fmt.Errorf("Error decoding RecordsCount metadata batch record: %w", err)
	}
	br.Records = make([]BatchRecord, RecordsCount)
	for i := range br.Records {
		if err = br.Records[i].decode(red); err != nil {
			return fmt.Errorf("Error decoding metadata batch record records: %w", err)
		}
	}

	return nil
}

func (br *BatchRecords) encode(w *Writer) error {
	w.Int64(br.BaseOffset)
	// batch lenght body
	blBody := &Writer{}

	blBody.Int32(br.PartitionLeaderEpoch)
	blBody.Int8(br.Magic)

	// ---- build CRC body ----
	crcBody := &Writer{}

	crcBody.Int16(br.Attributes)
	crcBody.Int32(br.LastOffsetDelta)
	crcBody.Int64(br.BaseTimestamp)
	crcBody.Int64(br.MaxTimestamp)
	crcBody.Int64(br.ProducerId)
	crcBody.Int16(br.ProducerEpoch)
	crcBody.Int32(br.BaseSequence)
	crcBody.Int32(int32(len(br.Records)))

	for i := range br.Records {
		br.Records[i].encode(crcBody)
	}

	crc := crc32.Checksum(crcBody.RawBytes(), crc32.MakeTable(crc32.Castagnoli))

	w.Int32(blBody.Len() + 4 + crcBody.Len()) //BatchLength , 4 for crc
	w.Write(blBody.RawBytes())
	w.UInt32(crc) // write computed CRC
	fmt.Println(hex.Dump(crcBody.RawBytes()))
	w.Write(crcBody.RawBytes())
	fmt.Println(hex.Dump(w.RawBytes()))
	return nil
}

type BatchRecord struct {
	// Length         int32 //varint
	Attributes     int8
	TimestampDelta int64 //varlong
	OffsetDelta    int32 //varint
	// KeyLength      int32 //varint
	Key []byte
	// ValueLength    int32 //varint
	Value []byte
	// HeadersCount   int32 //varint
	Headers []RecordHeader // NULLABLE !
}

func (br *BatchRecord) encode(w *Writer) error {
	body := &Writer{}

	body.Int8(br.Attributes)
	body.SVarL(br.TimestampDelta)
	body.SVarI(br.OffsetDelta)

	if br.Key == nil {
		body.SVarI(-1)
	} else {
		body.SVarI(int32(len(br.Key)))
		body.Write(br.Key)
	}

	if br.Value == nil {
		body.SVarI(-1)
	} else {
		body.SVarI(int32(len(br.Value)))
		body.Write(br.Value)
	}

	if br.Headers == nil {
		body.SVarI(-1)
	} else {
		body.SVarI(int32(len(br.Headers)))
		for _, h := range br.Headers {
			h.encode(body)
		}
	}

	w.SVarI(body.Len()) // the length itself
	w.Write(body.RawBytes())

	return nil
}

func (r *BatchRecord) decode(red *Reader) error {
	var err error
	if _, err = red.SVarInt(); err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	} //varint

	if r.Attributes, err = red.Int8(); err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	}
	if r.TimestampDelta, err = red.SVarLong(); err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	} //varlong
	if r.OffsetDelta, err = red.SVarInt(); err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	} //varint

	KeyLength, err := red.SVarInt()
	if err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	}
	if KeyLength == -1 {
		r.Key = nil
	} else {
		r.Key = make([]byte, KeyLength)
		red.Read(r.Key)
	}

	ValueLength, err := red.SVarInt()
	if err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	}
	if ValueLength == -1 {
		r.Value = nil
	} else {
		r.Value = make([]byte, ValueLength)
		red.Read(r.Value)
	}

	HeadersCount, err := red.SVarInt()
	if err != nil {
		return fmt.Errorf("Error decoding batch record: %w", err)
	} //varint

	if HeadersCount == -1 {
		r.Headers = nil
	} else {
		r.Headers = make([]RecordHeader, HeadersCount)
		for _, h := range r.Headers {
			h.decode(red)
		}
	}

	return err
}

type RecordHeader struct {
	HeaderKey []byte // string
	Value     []byte
}

func (rh *RecordHeader) encode(w *Writer) error {

	w.SVarI(int32(len(rh.HeaderKey)))
	w.Write(rh.HeaderKey)

	w.SVarI(int32(len(rh.Value)))
	w.Write(rh.Value)
	return nil
}

func (rh *RecordHeader) decode(red *Reader) error {
	var err error

	headerKeyLength, err := red.SVarInt()
	if err != nil {
		return fmt.Errorf("Error decoding record header: %w", err)
	}
	rh.HeaderKey = make([]byte, headerKeyLength)
	red.Read(rh.HeaderKey)

	HeaderValueLength, err := red.SVarInt()
	if err != nil {
		return fmt.Errorf("Error decoding record header: %w", err)
	}
	rh.Value = make([]byte, HeaderValueLength)
	red.Read(rh.Value)

	return err
}
