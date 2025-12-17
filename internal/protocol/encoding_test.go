package protocol

import (
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

func TestCompactString(t *testing.T) {
	tests := []struct {
		name      string
		testCase  string
		expResult []byte
	}{
		{
			name:      "salam",
			testCase:  "salam",
			expResult: []byte{0, 0, 0, 6, 5, 's', 'a', 'l', 'a', 'm'},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			w := NewWriter()
			w.CompactString(test.testCase)
			require.Equal(t, test.expResult, w.Bytes())
		})
	}
}
