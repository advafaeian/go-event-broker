package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytesToInt32(t *testing.T) {
	tests := []struct {
		name      string
		testCase  []byte
		expResult int32
	}{
		{
			name:      "266",
			testCase:  []byte{0, 0, 1, 10},
			expResult: 266,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			newint := BytesToInt32(test.testCase)
			require.Equal(t, test.expResult, newint)
		})
	}
}

func TestBytesToInt16(t *testing.T) {
	tests := []struct {
		name      string
		testCase  []byte
		expResult int16
	}{
		{
			name:      "1029",
			testCase:  []byte{4, 5},
			expResult: 1029,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			newint := BytesToInt16(test.testCase)
			require.Equal(t, test.expResult, newint)
		})
	}
}

func TestUvarintToBytes(t *testing.T) {
	tests := []struct {
		name      string
		testCase  uint32
		expResult []byte
	}{
		{
			name:      "4",
			testCase:  4,
			expResult: []byte{4},
		},
		{
			name:      "160",
			testCase:  160,
			expResult: []byte{32 | 128, 1},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			newint := uvarintToBytes(test.testCase)
			require.Equal(t, test.expResult, newint)
		})
	}
}

func TestBytesToUvarint(t *testing.T) {
	tests := []struct {
		name      string
		testCase  []byte
		expResult uint32
	}{
		{
			name:      "4",
			testCase:  []byte{4},
			expResult: 4,
		},
		{
			name:      "160",
			testCase:  []byte{32 | 128, 1},
			expResult: 160,
		},
		{
			name:      "2097151",
			testCase:  []byte{0xFF, 0xFF, 0x7F},
			expResult: 2097151,
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(bytes.NewReader(tt.testCase))
			newint, err := bytesToUvarint(r)
			require.NoError(t, err)
			require.Equal(t, tt.expResult, newint)
		})
	}
}

func TestCompactString(t *testing.T) {
	tests := []struct {
		name      string
		testCase  string
		expResult []byte
	}{
		{
			name:      "salam",
			testCase:  "salam",
			expResult: []byte{0, 0, 0, 6, 6, 's', 'a', 'l', 'a', 'm'},
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter()
			w.CompactString(tt.testCase)
			require.Equal(t, tt.expResult, w.Bytes())
		})
	}
}
