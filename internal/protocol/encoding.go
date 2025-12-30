package protocol

import "fmt"

type Reader struct {
	buf    []byte
	offset int
}

func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf, offset: 0}
}

func (r *Reader) Int32() int32 {
	r.offset += 4
	return BytesToInt32(r.buf[r.offset-4 : r.offset])
}

func (r *Reader) Int16() int16 {
	r.offset += 2
	return BytesToInt16(r.buf[r.offset-2 : r.offset])
}

func (r *Reader) Int8() int8 {
	r.offset += 1
	return BytesToInt8(r.buf[r.offset-1 : r.offset])
}

func (r *Reader) Byte() byte {
	r.offset += 1
	return r.buf[r.offset-1]
}

func (r *Reader) TagBuffer() TagBuffer {
	r.offset += 1
	return TagBuffer{}
}

func (r *Reader) VarInt() uint32 {
	offs, uvi := bytesToUvarint(r.buf[r.offset:])
	r.offset += offs
	return uvi
}

func (r *Reader) CompactString() string {
	bytes, lengthPlusOne := bytesToUvarint(r.buf[r.offset:])
	r.offset += bytes
	bts := r.buf[r.offset : r.offset+int(lengthPlusOne)-1]
	r.offset += int(lengthPlusOne) - 1
	return string(bts)
}

func (r *Reader) Bool() bool {
	b := r.Byte()
	if b == 0 {
		return false
	} else {
		return true
	}
}

// func (r *Reader) CompactString() string {
// 	lengthPlusOne := uint8(r.Byte())
// 	s := ""
// 	for range lengthPlusOne - 1 {
// 		s = s + rune(r.Byte())
// 	}
// 	return s
// }

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
	fmt.Println(uvarintToBytes(n), "****")
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

func (w *Writer) pathSize() {
	copy(w.buf[0:4], Int32ToBytes(int32(len(w.buf)-4)))
}

func (w *Writer) Bytes() []byte {
	w.pathSize()
	return w.buf
}

func (w *Writer) CompactString(s string) {
	w.buf = append(w.buf, byte(int8(len(s)+1)))
	w.buf = append(w.buf, s...)
}

func (w *Writer) PartitionsArray(a []Partition) {
	w.buf = append(w.buf, byte(1))
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

func bytesToUvarint(bytes []byte) (int, uint32) {

	var i uint32
	var counter int
	cont := byte(1)
	for n, b := range bytes {
		if cont == 0 {
			break
		}
		cont = b >> 7
		b &= 127 // 0111...
		bi := uint32(b)
		for range n {
			bi *= 128
		}
		i = i + bi
		counter++
	}
	return counter, i
}
