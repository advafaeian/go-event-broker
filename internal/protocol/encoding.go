package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

type Reader struct {
	r *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		r: bufio.NewReader(rd),
	}
}

func NewReaderFromBytes(b []byte) *Reader {
	return NewReader(bytes.NewReader(b))
}

func (r *Reader) Int64() (int64, error) {
	var buf [8]byte
	_, err := io.ReadFull(r.r, buf[:])
	if err != nil {
		return 0, err
	}
	return BytesToInt64(buf[:]), nil
}

func (r *Reader) Int32() (int32, error) {
	var buf [4]byte
	_, err := io.ReadFull(r.r, buf[:])
	if err != nil {
		return 0, err
	}
	return BytesToInt32(buf[:]), nil
}

func (r *Reader) Int16() (int16, error) {
	var buf [2]byte
	_, err := io.ReadFull(r.r, buf[:])
	if err != nil {
		return 0, err
	}
	return BytesToInt16(buf[:]), nil
}

func (r *Reader) Int8() (int8, error) {
	var buf [1]byte
	_, err := io.ReadFull(r.r, buf[:])
	if err != nil {
		return 0, err
	}
	return BytesToInt8(buf[:]), nil
}

func (r *Reader) Byte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(r.r, buf[:])
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *Reader) TagBuffer() TagBuffer {
	r.Byte()
	return TagBuffer{}
}

func (r *Reader) UVarInt() (uint32, error) {
	uvi, err := bytesToUvarint(r.r)
	if err != nil {
		return 0, err
	}
	return uvi, nil
}

func (r *Reader) SVarInt() (int32, error) {
	uvi, err := bytesToSvarint(r.r)
	if err != nil {
		return 0, err
	}
	return uvi, nil
}

func (r *Reader) CompactString() (string, error) {
	uvi, err := bytesToUvarint(r.r)

	if err != nil {
		return "", err
	}

	if uvi == 0 {
		return "", nil // null
	}

	buf := make([]byte, uvi-1)

	_, err = io.ReadFull(r.r, buf[:])
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (r *Reader) CompactArrayInt32() ([]int32, error) {
	lengthPlusOne, err := r.UVarInt()
	if err != nil {
		return []int32{}, fmt.Errorf("Error reading compact array inn32, %w", err)
	}
	buf := make([]int32, lengthPlusOne-1)
	for i := range lengthPlusOne - 1 {
		buf[i], err = r.Int32()
		if err != nil {
			return []int32{}, fmt.Errorf("Error reading compact array int32, %w", err)
		}
	}
	return buf, nil
}

func (r *Reader) CompactArrayUUID() ([]UUID, error) {
	lengthPlusOne, err := r.UVarInt()
	if err != nil {
		return []UUID{}, fmt.Errorf("Error reading compact array uuid, %w", err)
	}
	buf := make([]UUID, lengthPlusOne-1)
	for i := range lengthPlusOne - 1 {
		buf[i], err = r.UUID()
		if err != nil {
			return []UUID{}, fmt.Errorf("Error reading compact array uuid %w", err)
		}
	}
	return buf, nil
}

func (r *Reader) CompactArrayTopics() ([]Topic, error) {
	lengthPlusOne, err := r.UVarInt()
	if lengthPlusOne == 0 {
		return []Topic{}, errors.New("Error reading compact array topics: lengthplusone == 0")
	}
	if err != nil {
		return []Topic{}, fmt.Errorf("Error reading compact array topics %w", err)
	}
	buf := make([]Topic, lengthPlusOne-1)

	for i := range lengthPlusOne - 1 {
		buf[i].decode(r)
	}
	return buf, nil
}

func (r *Reader) Bool() (bool, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return false, err
	}
	if b == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (r *Reader) UUID() ([16]byte, error) {
	var buf [16]byte
	_, err := io.ReadFull(r.r, buf[:])
	if err != nil {
		return [16]byte{}, fmt.Errorf("Error reading UUID: %w", err)
	}
	return buf, nil
}

// func (r *Reader) CompactString() string {
// 	lengthPlusOne := uint8(r.Byte())
// 	s := ""
// 	for range lengthPlusOne - 1 {
// 		s = s + rune(r.Byte())
// 	}
// 	return s
// }

func BytesToInt64(buf []byte) int64 {
	i := int64(buf[0])<<56 | // if we do int64(buf[0])<<24), the first byte will be taken as the sign bit
		int64(buf[1])<<48 |
		int64(buf[2])<<40 |
		int64(buf[3])<<32 |
		int64(buf[4])<<24 |
		int64(buf[5])<<16 |
		int64(buf[6])<<8 |
		int64(buf[7])
	return i
}

func BytesToInt32(buf []byte) int32 {
	i := int32(buf[0])<<24 | // if we do int32(buf[0])<<24), the first byte will be taken as the sign bit
		int32(buf[1])<<16 |
		int32(buf[2])<<8 |
		int32(buf[3])
	return i
}

func BytesToInt16(buf []byte) int16 {
	i := int16(buf[0])<<8 |
		int16(buf[1])
	return i
}

func BytesToInt8(buf []byte) int8 {
	return int8(buf[0])
}

type Writer struct {
	buf []byte
}

func NewWriter() *Writer {
	buf := make([]byte, 4, 128)
	buf[0] = 0
	return &Writer{buf: buf}
}

func (w *Writer) Int32(n int32) {
	w.buf = append(w.buf, Int32ToBytes(n)...)
}

func (w *Writer) Int16(n int16) {
	w.buf = append(w.buf, Int16ToBytes(n)...)
}

func (w *Writer) Int8(n int8) {
	w.buf = append(w.buf, byte(n))
}

func (w *Writer) UvarI(n uint32) {
	w.buf = append(w.buf, uvarintToBytes(n)...)
}

func (w *Writer) ApiKeys(keys []ApiKey) {
	for _, k := range keys {
		w.Int16(k.ApiKey)
		w.Int16(k.MinVersion)
		w.Int16(k.MaxVersion)
		w.TagBuffer(k.TagBuffer)
	}
}

func (w *Writer) TagBuffer(t TagBuffer) {
	if len(t) == 0 {
		w.Int8(0) // empty tagfield
	}
}

func (w *Writer) patchSize() {
	copy(w.buf[0:4], Int32ToBytes(int32(len(w.buf)-4)))
}

func (w *Writer) Bytes() []byte {
	w.patchSize()
	return w.buf
}

func (w *Writer) CompactString(s string) {
	w.buf = append(w.buf, uvarintToBytes(uint32(len(s)+1))...)
	w.buf = append(w.buf, s...)
}

func (w *Writer) Bool(b bool) {
	if b {
		w.Int8(1)
	} else {
		w.Int8(0)
	}
}

func (w *Writer) append(bytes []byte) {
	w.buf = append(w.buf, bytes...)
}

func Int32ToBytes(n int32) []byte {
	return []byte{
		byte(n >> 24),
		byte(n >> 16),
		byte(n >> 8),
		byte(n),
	}
}

func Int16ToBytes(n int16) []byte {
	return []byte{
		byte(n >> 8),
		byte(n),
	}
}

func uvarintToBytes(n uint32) []byte {
	bytes := make([]byte, 0, 1)

	m := n

	var mod uint32

	for m > 0 {
		mod = m % 128
		m /= 128

		bdigit := byte(mod)

		if m > 0 {
			bdigit |= 128
		}

		bytes = append(bytes, bdigit)
	}

	return bytes
}

func bytesToUvarint(r *bufio.Reader) (uint32, error) {

	var i uint32
	var counter int
	cont := byte(1)
	for {
		if cont == 0 {
			break
		}
		b, err := r.ReadByte()

		if err != nil {
			return 0, err
		}
		cont = b >> 7
		b &= 127 // 127 = 0111...
		bi := uint32(b)
		for range counter {
			bi *= 128
		}
		i = i + bi
		counter++
	}
	return i, nil
}

func bytesToSvarint(r *bufio.Reader) (int32, error) {

	uInt, err := bytesToUvarint(r)
	if err != nil {
		return -1, fmt.Errorf("Error bytes to signed var int: %w", err)
	}
	var sInt int32
	if uInt%2 == 0 {
		sInt = int32(uInt) / 2
	} else {
		sInt = -(int32(uInt+1) / 2)
	}
	return sInt, nil
}

func (r *Reader) Skip(n int32) error {
	_, err := io.CopyN(io.Discard, r.r, int64(n))
	return err
}
