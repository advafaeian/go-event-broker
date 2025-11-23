package protocol

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
